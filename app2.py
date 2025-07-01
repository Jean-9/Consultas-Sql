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

# -------------------------
# Carregamento de variáveis de ambiente
# -------------------------

load_dotenv()

# -------------------------
# Conexões com os bancos de dados
# -------------------------

# Conexão com o banco PostgreSQL (onde as consultas são salvas)
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

# Conexão com o banco Protheus (onde as consultas são executadas)
connection_string = (
    f"DRIVER=ODBC Driver 17 for SQL Server;"
    f"SERVER={os.getenv('host')},{os.getenv('port')};"
    f"DATABASE={os.getenv('database')};"
    f"UID={os.getenv('username')};"
    f"PWD={os.getenv('password')}"
)
engine_protheus = create_engine(URL.create("mssql+pyodbc", query={"odbc_connect": connection_string}))

# -------------------------
# Utilitário para forçar recarregamento no Streamlit
# -------------------------

def rerun():
    import streamlit.runtime.scriptrunner.script_runner as script_runner
    from streamlit.runtime.scriptrunner import RerunException
    raise RerunException(script_runner.RerunData())

# -------------------------
# Funções CRUD para consultas salvas
# -------------------------

def listar_consultas():
    """Retorna todas as consultas salvas do banco PostgreSQL."""
    with engine_postgres.connect() as conn:
        result = conn.execute(text("SELECT id, nome, descricao, criado_em FROM consultas_salvas ORDER BY criado_em DESC"))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df

def salvar_consulta(nome, descricao, consulta_sql):
    """Salva uma nova consulta no banco PostgreSQL."""
    with engine_postgres.begin() as conn:
        conn.execute(text("""
            INSERT INTO consultas_salvas (nome, descricao, consulta) VALUES (:nome, :descricao, :consulta)
        """), {"nome": nome, "descricao": descricao, "consulta": consulta_sql})

def carregar_consulta(id):
    """Carrega o conteúdo SQL de uma consulta salva a partir do ID."""
    with engine_postgres.begin() as conn:
        result = conn.execute(text("SELECT consulta FROM consultas_salvas WHERE id = :id"), {"id": id}).fetchone()
    return result[0] if result else ""

def deletar_consulta(id):
    """Deleta uma consulta salva com base no ID."""
    with engine_postgres.begin() as conn:
        conn.execute(text("DELETE FROM consultas_salvas WHERE id = :id"), {"id": id})

# -------------------------
# Interface do Streamlit
# -------------------------

st.title("Gerenciador de Consultas SQL")

# Exibir consultas salvas
df_consultas = listar_consultas()
if not df_consultas.empty:
    id_selecionado = st.selectbox("Consultas salvas:", options=df_consultas["id"],
                                  format_func=lambda x: df_consultas[df_consultas["id"] == x]["nome"].values[0])
else:
    id_selecionado = None

# Botões de ação: carregar ou deletar consulta
col1, col2 = st.columns(2)
with col1:
    if st.button("Carregar Consulta") and id_selecionado:
        consulta_texto = carregar_consulta(id_selecionado)
        st.session_state["consulta"] = consulta_texto
with col2:
    if st.button("Deletar Consulta") and id_selecionado:
        deletar_consulta(id_selecionado)
        rerun()

# Formulário para salvar nova consulta
consulta_sql = st.text_area("Consulta SQL", value=st.session_state.get("consulta", ""), height=200)
nome = st.text_input("Nome da consulta")
descricao = st.text_area("Descrição")

if st.button("Salvar Consulta"):
    if nome.strip() == "":
        st.error("Por favor, insira um nome para a consulta.")
    elif consulta_sql.strip() == "":
        st.error("Consulta SQL não pode ser vazia.")
    else:
        salvar_consulta(nome, descricao, consulta_sql)
        st.success("Consulta salva com sucesso!")
        rerun()

# -------------------------
# Execução da consulta no banco Protheus
# -------------------------

if st.button("Executar Consulta"):
    try:
        # Verifica se a consulta começa com SELECT (case-insensitive, remove espaços em branco à esquerda)
        if not consulta_sql.strip().lower().startswith("select"):
            st.error("Somente consultas SELECT são permitidas.")
        else:
            with engine_protheus.connect() as conn2:
                df_result = pd.read_sql(text(consulta_sql), conn2)
            st.dataframe(df_result, use_container_width=True)
    except Exception as e:
        st.error(f"Erro na execução: {e}")
        with engine_protheus.connect() as conn2:
            df_result = pd.read_sql(text(consulta_sql), conn2)
        st.dataframe(df_result, use_container_width=True)
    except Exception as e:
        st.error(f"Erro na execução: {e}")
