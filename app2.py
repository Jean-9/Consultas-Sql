# --------------------------------------
# App Streamlit para Gerenciar Consultas SQL
# --------------------------------------
# - Permite salvar, listar, deletar e executar SELECTs personalizados
# - Os SELECTs são salvos em um banco PostgreSQL
# - As consultas são executadas contra um banco SQL Server (Protheus)
# - Os filtros são definidos dinamicamente com base nas colunas detectadas
# --------------------------------------

from io import BytesIO
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from sqlalchemy.engine.url import URL
import os

# Carrega variáveis de ambiente do .env
load_dotenv()

# -------------------------
# Conexões com os bancos
# -------------------------

# Conexão com banco PostgreSQL (armazenamento de consultas)
engine_postgres = create_engine(
    URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("username_postgres"),
        password=os.getenv("password_postgres"),
        host=os.getenv("host_postgres"),
        port=os.getenv("port_postgres"),
        database=os.getenv("database_postgres"),
        query={"sslmode": "disable"}
    )
)

# Conexão com banco Protheus (execução de consultas)
connection_string = (
    f"DRIVER=ODBC Driver 17 for SQL Server;"
    f"SERVER={os.getenv('host')},{os.getenv('port')};"
    f"DATABASE={os.getenv('database')};"
    f"UID={os.getenv('username_protehus')};"
    f"PWD={os.getenv('password')}"
)
engine_protheus = create_engine(URL.create("mssql+pyodbc", query={"odbc_connect": connection_string}))

# -------------------------
# Utilitários
# -------------------------

# Verifica se a consulta contém comandos proibidos
def validar_sql_base(sql):
    proibidos = ["delete", "drop", "update", "insert"]
    if any(comando in sql.lower() for comando in proibidos):
        st.error("Comando SQL não permitido.")
        return False
    return True

# -------------------------
# Funções CRUD
# -------------------------

# Lista de Consultas
def listar_consultas():
    with engine_postgres.connect() as conn:
        result = conn.execute(text("SELECT id, nome, descricao, criado_em FROM consultas_salvas ORDER BY criado_em DESC"))
        return pd.DataFrame(result.fetchall(), columns=result.keys())

# Salvamento de Consultas
def salvar_consulta(nome, descricao, consulta_sql):
    with engine_postgres.begin() as conn:
        conn.execute(text("""
            INSERT INTO consultas_salvas (nome, descricao, consulta) VALUES (:nome, :descricao, :consulta)
        """), {"nome": nome, "descricao": descricao, "consulta": consulta_sql})

# Carrega a consulta da lista para o campo 'Consulta SQL'
def carregar_consulta(id):
    with engine_postgres.begin() as conn:
        result = conn.execute(text("SELECT consulta FROM consultas_salvas WHERE id = :id"), {"id": id}).fetchone()
    return result[0] if result else ""

# Deleta uma conaulta da Lista
def deletar_consulta(id):
    with engine_postgres.begin() as conn:
        conn.execute(text("DELETE FROM consultas_salvas WHERE id = :id"), {"id": id})

# -------------------------
# Interface Streamlit
# -------------------------
st.title("Gerenciador de Consultas SQL")

# Lista e seleção de consultas salvas
consultas = listar_consultas()
id_selecionado = st.selectbox("Consultas salvas:", options=consultas["id"] if not consultas.empty else [], format_func=lambda x: consultas[consultas["id"] == x]["nome"].values[0])


if "consulta" not in st.session_state:
    st.session_state["consulta"] = ""

# Botões carregar/deletar
col1, col2 = st.columns(2)
with col1:
    if st.button("Carregar") and id_selecionado:
        st.session_state["consulta"] = carregar_consulta(id_selecionado)
        st.rerun()
with col2:
    if st.button("Deletar") and id_selecionado:
        deletar_consulta(id_selecionado)
        st.success("Consulta deletada.")
        st.rerun()

# Editor da consulta
col_sql, col_filtros = st.columns([3, 1])
with col_sql:
    consulta_sql = st.text_area("Consulta SQL", height=250, value=st.session_state.get("consulta", ""), key="consulta_sql")
    nome = st.text_input("Nome", key="nome")
    descricao = st.text_area("Descrição", key="descricao")

# Filtros baseados nas colunas da consulta
filtros_valores = {}
colunas_disponiveis = []

with col_filtros:
    if consulta_sql.strip():
        try:
            # Executa a consulta para descobrir as colunas disponíveis
            with engine_protheus.connect() as conn:
                df_top1 = pd.read_sql(f"SELECT TOP 1 * FROM ({consulta_sql}) AS base", conn)
            colunas_disponiveis = df_top1.columns.tolist()

            # Seleção de colunas para aplicar filtros
            colunas_para_filtrar = st.multiselect("Selecionar colunas para filtro:", colunas_disponiveis)

            for col in colunas_para_filtrar:
                if "data" in col.lower() or "emissao" in col.lower():
                    filtros_valores[f"{col}_de"] = st.date_input(f"{col} - De:")
                    filtros_valores[f"{col}_ate"] = st.date_input(f"{col} - Até:")
                else:
                    filtros_valores[col] = st.text_input(f"Filtro para {col}:")
        except Exception as e:
            st.error(f"Erro ao carregar colunas: {e}")

# Botões salvar e executar
col_s, col_e = st.columns(2)
with col_s:
    if st.button("Salvar", key="btn_salvar"):
        if not nome.strip() or not consulta_sql.strip():
            st.error("Preencha todos os campos.")
        else:
            salvar_consulta(nome, descricao, consulta_sql)
            st.success("Consulta salva.")
            st.rerun()

with col_e:
    if st.button("Executar", key="btn_exec"):
        if not validar_sql_base(consulta_sql):
            st.stop()

        # Construção do WHERE com base nos filtros preenchidos
        clausulas = []
        params = {}
        for col in colunas_disponiveis:
            if f"{col}_de" in filtros_valores:
                clausulas.append(f"{col} >= :{col}_de")
                params[f"{col}_de"] = filtros_valores[f"{col}_de"]
            if f"{col}_ate" in filtros_valores:
                clausulas.append(f"{col} <= :{col}_ate")
                params[f"{col}_ate"] = filtros_valores[f"{col}_ate"]
            elif col in filtros_valores and filtros_valores[col]:
                clausulas.append(f"{col} LIKE :{col}")
                params[col] = f"%{filtros_valores[col]}%"

        # Combinação da cláusula WHERE
        where = " AND ".join(clausulas)
        sql_final = f"SELECT * FROM ({consulta_sql}) AS base WHERE 1=1"
        if where:
            sql_final += f" AND {where}"

        try:
            with engine_protheus.connect() as conn:
                df_result = pd.read_sql(text(sql_final), conn, params=params)

            st.session_state["df_result"] = df_result

        except Exception as e:
            st.error(f"Erro: {e}")

# Exibição do resultado e opções de download
if "df_result" in st.session_state:
    with st.container():
        st.subheader("Resultado da Consulta")
        st.dataframe(st.session_state["df_result"], use_container_width=True)

        excel_stream = BytesIO()
        st.session_state["df_result"].to_excel(excel_stream, index=False, engine="openpyxl")
        excel_stream.seek(0)
        csv_data = st.session_state["df_result"].to_csv(index=False).encode("utf-8")

        st.download_button("📥 Excel", data=excel_stream, file_name="resultado.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        st.download_button("📥 CSV", data=csv_data, file_name="resultado.csv", mime="text/csv")

