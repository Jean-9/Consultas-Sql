"""
App Streamlit para gerenciar, salvar e executar consultas SQL personalizadas.
- As consultas são armazenadas no PostgreSQL.
- A execução das consultas ocorre no banco Protheus (SQL Server).
"""

from io import BytesIO
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from sqlalchemy.engine.url import URL
import os

# Carrega variáveis de ambiente do arquivo .env
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

# Mapeamento lógico de filtros para possíveis colunas físicas na consulta
MAPEAMENTO_FILTROS = {
    "produto": ["B1_COD", "D2_COD", "D1_COD", "C7_PRODUTO", "C6_PRODUTO"],
    "fornecedor": ["A2_COD", "D1_FORNECE", "F1_FORNECE"],
    "cooperado": ["A1_COD", "F2_CLIENTE", "C5_CLIENTE", "C6_CLI"],
    "filial": ["D1_FILIAL", "C7_FILIAL", "M0_CODFIL", "C5_FILIAL", "C6_FILIAL"],
    "data": ["D1_EMISSAO", "D2_EMISSAO", "C7_EMISSAO", "F2_EMISSAO", "F1_EMISSAO", "C5_EMISSAO", "C6_ENTREG"],
}


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

# Detecta quais filtros podem ser aplicados à consulta com base nas colunas detectadas
def detectar_filtros(sql_base):
    if not sql_base.strip():
        return []
    try:
        query = f"SELECT TOP 1 * FROM ({sql_base}) AS sub"
        with engine_protheus.connect() as conn:
            df = pd.read_sql(query, conn)
        colunas = [c.upper() for c in df.columns]
        filtros = [k for k, v in MAPEAMENTO_FILTROS.items() if any(c in colunas for c in v)]
        return filtros
    except Exception as e:
        st.error(f"Erro ao detectar filtros: {e}")
        return []

# Monta a cláusula WHERE dinamicamente com base nos filtros detectados
def montar_where(sql_base, filtros_logicos, valores):
    with engine_protheus.connect() as conn:
        df = pd.read_sql(f"SELECT TOP 1 * FROM ({sql_base}) AS sub", conn)
    colunas_disponiveis = [c.upper() for c in df.columns]

    clausulas, params = [], {}
    for filtro in filtros_logicos:
        # Verifica se existe uma coluna física correspondente no resultado da consulta
        coluna_fisica = next((col for col in MAPEAMENTO_FILTROS[filtro] if col.upper() in colunas_disponiveis), None)
        if not coluna_fisica:
            continue

        # Adiciona os filtros ao WHERE com os parâmetros apropriados
        if filtro == "data":
            if valores.get("data_de"):
                clausulas.append(f"{coluna_fisica} >= :data_de")
                params["data_de"] = valores["data_de"]
            if valores.get("data_ate"):
                clausulas.append(f"{coluna_fisica} <= :data_ate")
                params["data_ate"] = valores["data_ate"]
        else:
            valor = valores.get(filtro)
            if valor:
                clausulas.append(f"{coluna_fisica} LIKE :{filtro}")
                params[filtro] = f"%{valor}%"
    return " AND ".join(clausulas), params


# -------------------------
# Funções CRUD (PostgreSQL)
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

# Inicializa a session_state se necessário
if "consulta" not in st.session_state:
    st.session_state["consulta"] = ""

# Botões para carregar ou deletar consulta
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

# Área principal da consulta
col_sql, col_filtros = st.columns([3, 1])
with col_sql:
    consulta_sql = st.text_area("Consulta SQL", height=250, value=st.session_state.get("consulta", ""), key="consulta_sql")
    nome = st.text_input("Nome", key="nome")
    descricao = st.text_area("Descrição", key="descricao")

with col_filtros:
    filtros_detectados = detectar_filtros(consulta_sql)
    filtros_valores = {}
    if filtros_detectados:
        if "data" in filtros_detectados:
            filtros_valores["data_de"] = st.date_input("Data de", key="data_de")
            filtros_valores["data_ate"] = st.date_input("Data até", key="data_ate")
        for campo in ["fornecedor", "filial", "cooperado", "produto"]:
            if campo in filtros_detectados:
                filtros_valores[campo] = st.text_input(campo.capitalize(), key=f"filtro_{campo}")

# Botões Salvar e Executar
col_s, col_e = st.columns(2)

# Botão para salvar consulta no PostgreSQL
with col_s:
    if st.button("Salvar", key="btn_salvar"):
        if not nome.strip() or not consulta_sql.strip():
            st.error("Preencha todos os campos.")
        else:
            salvar_consulta(nome, descricao, consulta_sql)
            st.success("Consulta salva.")
            st.rerun()

# Botão para executar consulta no Protheus
with col_e:
    if st.button("Executar", key="btn_exec"):
        if not validar_sql_base(consulta_sql):
            st.stop()
        where, params = montar_where(consulta_sql, filtros_detectados, filtros_valores)
        sql_final = f"SELECT * FROM ({consulta_sql}) AS base WHERE 1=1"
        if where:
            sql_final += f" AND {where}"

        try:
            with engine_protheus.connect() as conn:
                df_result = pd.read_sql(text(sql_final), conn, params=params)

            # Armazena o resultado para exibir fora da coluna
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
