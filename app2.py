from io import BytesIO
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from credentials import host, port, username, password, database, host_postgres, port_postgres, username_postgres, password_postgres, database_postgres
from sqlalchemy.engine.url import URL

# Conexão
# Postgres
engine_postgres = create_engine(
    URL.create(
        drivername="postgresql+psycopg2",
        username=username_postgres,
        password=password_postgres,
        host=host_postgres,
        port=port_postgres,
        database=database_postgres,
        query={"sslmode": "disable"}
    )
)

# Protheus
connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={host},{port};DATABASE={database};UID={username};PWD={password}"
engine_protheus = create_engine(URL.create("mssql+pyodbc", query={"odbc_connect": connection_string}))

def rerun():
    import streamlit.runtime.scriptrunner.script_runner as script_runner
    from streamlit.runtime.scriptrunner import RerunException
    raise RerunException(script_runner.RerunData())


# Funções CRUD básicas
def listar_consultas():
    with engine_postgres.connect() as conn:
        result = conn.execute(text("SELECT id, nome, descricao, criado_em FROM consultas_salvas ORDER BY criado_em DESC"))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df

def salvar_consulta(nome, descricao, consulta_sql):
    with engine_postgres.begin() as conn:
        conn.execute(text("""
            INSERT INTO consultas_salvas (nome, descricao, consulta) VALUES (:nome, :descricao, :consulta)
        """), {"nome": nome, "descricao": descricao, "consulta": consulta_sql})

def carregar_consulta(id):
    with engine_postgres.connect() as conn:
        result = conn.execute(text("SELECT consulta FROM consultas_salvas WHERE id = :id"), {"id": id}).fetchone()
    return result[0] if result else ""

def deletar_consulta(id):
    with engine_postgres.connect() as conn:
        conn.execute(text("DELETE FROM consultas_salvas WHERE id = :id"), {"id": id})

# UI Streamlit
st.title("Gerenciador de Consultas SQL")

# Listar consultas salvas
df_consultas = listar_consultas()

if not df_consultas.empty:
    id_selecionado = st.selectbox("Consultas salvas:", options=df_consultas["id"], format_func=lambda x: df_consultas[df_consultas["id"] == x]["nome"].values[0])
else:
    id_selecionado = None

# Botões para carregar ou deletar
col1, col2 = st.columns(2)
with col1:
    if st.button("Carregar Consulta") and id_selecionado:
        consulta_texto = carregar_consulta(id_selecionado)
        st.session_state["consulta"] = consulta_texto
with col2:
    if st.button("Deletar Consulta") and id_selecionado:
        deletar_consulta(id_selecionado)
        rerun()

# Caixa para editar/inserir a consulta
consulta_atual = st.text_area("Consulta SQL", value=st.session_state.get("consulta", ""), height=200)
nome = st.text_input("Nome da consulta")
descricao = st.text_area("Descrição")

if st.button("Salvar Consulta"):
    if nome.strip() == "":
        st.error("Por favor, insira um nome para a consulta.")
    elif consulta_atual.strip() == "":
        st.error("Consulta SQL não pode ser vazia.")
    else:
        salvar_consulta(nome, descricao, consulta_atual)
        st.success("Consulta salva com sucesso!")
        rerun()

# Botão para executar a consulta diretamente
if st.button("Executar Consulta"):
    try:
        with engine_protheus.connect() as conn2:
            df_result = pd.read_sql(text(consulta_atual), conn2)
        st.dataframe(df_result, use_container_width=True)
    except Exception as e:
        st.error(f"Erro na execução: {e}")
