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
from components.streamlit_ace import st_ace
import json

# Carrega variáveis de ambiente do .env
load_dotenv()

# Configuração da página para usar toda a largura
st.set_page_config(
    page_title="Gerenciador de Consultas SQL",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Estilo customizado profissional com destaque para filtros
st.markdown("""
    <style>
    .main .block-container {
        padding-top: 1rem;
        padding-left: 1rem;
        padding-right: 1rem;
        padding-bottom: 1rem;
        max-width: 100%;
    }

    /* Estilo profissional para o fundo */
    .stApp {
        background: linear-gradient(135deg, #1e1e1e 0%, #2d2d2d 100%);
    }

    /* Melhor espaçamento para os elementos */
    .stSelectbox > div > div {
        background-color: #262730;
        border: 1px solid #444;
        border-radius: 6px;
    }

    /* DESTAQUE ESPECIAL PARA FILTROS */
    .filtros-container {
        background: linear-gradient(135deg, #1a4b8c 0%, #2563eb 100%);
        border: 2px solid #3b82f6;
        border-radius: 12px;
        padding: 1.5rem;
        margin: 1rem 0;
        box-shadow: 0 8px 25px rgba(59, 130, 246, 0.3);
        position: relative;
        overflow: hidden;
    }

    .filtros-container::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 4px;
        background: linear-gradient(90deg, #60a5fa, #3b82f6, #1d4ed8);
        animation: shimmer 2s infinite;
    }

    @keyframes shimmer {
        0% { transform: translateX(-100%); }
        100% { transform: translateX(100%); }
    }

    .filtros-title {
        color: #ffffff !important;
        font-weight: bold;
        font-size: 1.2rem;
        margin-bottom: 1rem;
        text-shadow: 0 2px 4px rgba(0,0,0,0.3);
    }

    /* Destaque para a área de resultado */
    .resultado-container {
        background: linear-gradient(135deg, #1e1e1e 0%, #2a2a2a 100%);
        border: 1px solid #444;
        border-radius: 12px;
        padding: 1.5rem;
        margin-top: 0.5rem;
        box-shadow: 0 4px 20px rgba(0,0,0,0.3);
    }

    /* Melhor visibilidade dos botões */
    .stButton > button {
        width: 100%;
        margin-bottom: 0.5rem;
        border-radius: 8px;
        font-weight: 600;
        transition: all 0.3s ease;
        border: 1px solid #444;
    }

    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    }

    /* Botão primário especial */
    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #3b82f6 0%, #1d4ed8 100%);
        border: none;
        color: white;
    }

    /* Espaçamento dos campos de entrada */
    .stTextInput > div > div > input,
    .stTextArea > div > div > textarea {
        background-color: #262730;
        border: 1px solid #444;
        border-radius: 6px;
        transition: border-color 0.3s ease;
    }

    .stTextInput > div > div > input:focus,
    .stTextArea > div > div > textarea:focus {
        border-color: #3b82f6;
        box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.2);
    }

    /* Título principal */
    h1 {
        text-align: center;
        margin-bottom: 2rem;
        color: #ffffff;
        text-shadow: 0 2px 4px rgba(0,0,0,0.3);
        font-size: 2.5rem;
        background: linear-gradient(135deg, #60a5fa, #3b82f6);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }

    /* Subtítulos das seções */
    h3 {
        border-bottom: 2px solid #3b82f6;
        padding-bottom: 0.5rem;
        margin-bottom: 1rem;
        color: #ffffff;
        font-weight: 600;
    }

    /* Cards profissionais */
    .section-card {
        background: linear-gradient(135deg, #1e1e1e 0%, #2a2a2a 100%);
        border: 1px solid #444;
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1rem;
        box-shadow: 0 4px 20px rgba(0,0,0,0.2);
    }

    /* Métricas destacadas */
    .metric-card {
        background: linear-gradient(135deg, #374151 0%, #4b5563 100%);
        border-radius: 8px;
        padding: 1rem;
        text-align: center;
        border: 1px solid #6b7280;
    }

    /* Melhor aparência para multiselect */
    .stMultiSelect > div > div {
        background-color: #262730;
        border: 2px solid #3b82f6;
        border-radius: 8px;
    }

    /* Destaque para campos de data nos filtros */
    .filtros-container .stDateInput > div > div > input {
        background-color: rgba(255,255,255,0.1);
        border: 1px solid rgba(255,255,255,0.3);
        color: white;
    }

    .filtros-container .stTextInput > div > div > input {
        background-color: rgba(255,255,255,0.1);
        border: 1px solid rgba(255,255,255,0.3);
        color: white;
    }

    .filtros-container .stMultiSelect > div > div {
        background-color: rgba(255,255,255,0.1);
        border: 1px solid rgba(255,255,255,0.3);
    }

    /* Animação suave para elementos interativos */
    .stSelectbox, .stTextInput, .stTextArea, .stMultiSelect {
        transition: all 0.3s ease;
    }

    /* Divider personalizado */
    hr {
        border: none;
        height: 2px;
        background: linear-gradient(90deg, transparent, #3b82f6, transparent);
        margin: 2rem 0;
    }

    /* Customização do editor ACE - mudando a faixa roxa para azul */
    .ace_gutter {
        background: linear-gradient(135deg, #1e40af 0%, #3b82f6 100%) !important;
        border-right: 2px solid #60a5fa !important;
    }

    .ace_gutter-active-line {
        background-color: rgba(96, 165, 250, 0.3) !important;
    }

    .ace_gutter-cell {
        color: #e5e7eb !important;
        background: transparent !important;
    }

    .ace_gutter-cell.ace_info {
        background: rgba(59, 130, 246, 0.2) !important;
    }

    /* Linha ativa no editor */
    .ace_active-line {
        background: rgba(59, 130, 246, 0.1) !important;
    }

    /* Cursor do editor */
    .ace_cursor {
        color: #60a5fa !important;
    }

    /* Seleção no editor */
    .ace_selection {
        background: rgba(59, 130, 246, 0.3) !important;
    }

    /* Força a customização do gutter mesmo com tema chrome */
    div[data-testid="stAce"] .ace_gutter {
        background: linear-gradient(135deg, #1e40af 0%, #3b82f6 100%) !important;
        border-right: 2px solid #60a5fa !important;
    }

    /* Customização adicional para garantir que o azul apareça */
    .ace_editor .ace_gutter {
        background: linear-gradient(135deg, #1e40af 0%, #3b82f6 100%) !important;
        border-right: 2px solid #60a5fa !important;
    }

    /* Sobrescreve qualquer cor de fundo do tema */
    .ace_gutter-layer {
        background: linear-gradient(135deg, #1e40af 0%, #3b82f6 100%) !important;
    }
    </style>
""", unsafe_allow_html=True)

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
engine_protheus = create_engine(
    URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("username_protheus"),
        password=os.getenv("password"),
        host=os.getenv("host"),
        port=os.getenv("port"),
        database=os.getenv("database"),
        query={"sslmode": "disable"}
    )
)


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
        result = conn.execute(
            text("SELECT id, nome, descricao, criado_em FROM consultas_salvas ORDER BY criado_em DESC"))
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


# Deleta uma consulta da Lista
def deletar_consulta(id):
    with engine_postgres.begin() as conn:
        conn.execute(text("DELETE FROM consultas_salvas WHERE id = :id"), {"id": id})


# -------------------------
# Interface Streamlit
# -------------------------

st.title("Gerenciador de Consultas SQL")

# Seção de gerenciamento de consultas salvas (topo da página)
st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.markdown("### 📋 Consultas Salvas")
consultas = listar_consultas()

# Layout em colunas para a seção de consultas salvas
col_select, col_actions = st.columns([3, 1])

with col_select:
    id_selecionado = st.selectbox(
        "Selecione uma consulta:",
        options=consultas["id"] if not consultas.empty else [],
        format_func=lambda x: consultas[consultas["id"] == x]["nome"].values[
            0] if not consultas.empty else "Nenhuma consulta disponível"
    )

with col_actions:
    col_carregar, col_deletar = st.columns(2)

    if "consulta" not in st.session_state:
        st.session_state["consulta"] = ""

    with col_carregar:
        if st.button("🔄 Carregar", use_container_width=True) and id_selecionado:
            st.session_state["consulta"] = carregar_consulta(id_selecionado)
            st.rerun()

    with col_deletar:
        if st.button("🗑️ Deletar", use_container_width=True) and id_selecionado:
            deletar_consulta(id_selecionado)
            st.success("Consulta deletada.")
            st.rerun()

st.markdown('</div>', unsafe_allow_html=True)
st.divider()

# Carrega as palavras do autocomplete
try:
    with open("autocomplete/autocomplete_cache.json", "r", encoding="utf-8") as f:
        autocomplete_formatado = json.load(f)
except FileNotFoundError:
    autocomplete_formatado = []
    st.warning("Arquivo 'autocomplete_cache.json' não encontrado. Autocomplete desabilitado.")

# Layout principal em duas colunas: Editor/Controles | Resultado
col_esquerda, col_direita = st.columns([1, 2])

# ---------------------
# COLUNA ESQUERDA - Editor e Controles
# ---------------------
with col_esquerda:
    # Editor SQL com altura aumentada em 15%
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown("### 💻 Editor SQL")

    consulta_sql = st_ace(
        value=st.session_state.get("consulta", ""),
        language="sql",
        theme="tomorrow_night",  # Mudado de 'terminal' para 'chrome' para permitir customização
        height=400,  # Aumentado de 350 para 400 (15% a mais)
        key="consulta_sql_editor",
        auto_update=True,
        show_gutter=True,
        show_print_margin=True,
        wrap=True,
        completer=autocomplete_formatado
    )
    st.session_state["consulta"] = consulta_sql
    st.markdown('</div>', unsafe_allow_html=True)

    # Campos para salvar consulta
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown("### 💾 Salvar Consulta")
    nome = st.text_input("Nome da consulta:", key="nome")
    descricao = st.text_area("Descrição:", key="descricao", height=80)

    if st.button("💾 Salvar Consulta", use_container_width=True, type="primary"):
        if not nome.strip() or not consulta_sql.strip():
            st.error("Preencha o nome e a consulta SQL.")
        else:
            salvar_consulta(nome, descricao, consulta_sql)
            st.success("Consulta salva com sucesso!")
            st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

    # Seção de filtros COM DESTAQUE ESPECIAL
    st.markdown('<div class="filtros-container">', unsafe_allow_html=True)
    st.markdown('<h3 class="filtros-title">🔍 FILTROS DINÂMICOS</h3>', unsafe_allow_html=True)

    filtros_valores = {}
    colunas_disponiveis = []

    if consulta_sql.strip():
        try:
            with engine_protheus.connect() as conn:
                df_top1 = pd.read_sql(f"SELECT * FROM ({consulta_sql}) AS base LIMIT 1", conn)
            colunas_disponiveis = df_top1.columns.tolist()

            if colunas_disponiveis:
                st.markdown("**🎯 Selecione as colunas para filtrar:**")
                colunas_para_filtrar = st.multiselect(
                    "",
                    colunas_disponiveis,
                    help="Escolha as colunas que deseja filtrar",
                    key="multiselect_filtros"
                )

                # Container para os filtros
                if colunas_para_filtrar:
                    st.markdown("**⚙️ Configure os valores dos filtros:**")
                    for col in colunas_para_filtrar:
                        if "data" in col.lower() or "emissao" in col.lower():
                            col_data1, col_data2 = st.columns(2)
                            with col_data1:
                                filtros_valores[f"{col}_de"] = st.date_input(f"📅 {col} - De:", key=f"data_de_{col}")
                            with col_data2:
                                filtros_valores[f"{col}_ate"] = st.date_input(f"📅 {col} - Até:", key=f"data_ate_{col}")
                        else:
                            filtros_valores[col] = st.text_input(f"🔎 Filtro para {col}:", key=f"filtro_{col}")
                else:
                    st.markdown("**ℹ️ Selecione colunas acima para configurar filtros**")
            else:
                st.markdown("**⚠️ Execute uma consulta válida para ver as colunas disponíveis.**")

        except Exception as e:
            st.error(f"Erro ao carregar colunas: {e}")
    else:
        st.markdown("**📝 Digite uma consulta SQL para habilitar os filtros.**")

    st.markdown('</div>', unsafe_allow_html=True)

    # Botão de execução
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown("### ▶️ Executar")
    if st.button("🚀 Executar Consulta", use_container_width=True, type="primary"):
        if not consulta_sql.strip():
            st.error("Digite uma consulta SQL.")
        elif not validar_sql_base(consulta_sql):
            st.stop()
        else:
            # Construir filtros
            clausulas = []
            params = {}
            for col in colunas_disponiveis:
                if f"{col}_de" in filtros_valores and filtros_valores[f"{col}_de"]:
                    clausulas.append(f"{col} >= :{col}_de")
                    params[f"{col}_de"] = filtros_valores[f"{col}_de"]
                if f"{col}_ate" in filtros_valores and filtros_valores[f"{col}_ate"]:
                    clausulas.append(f"{col} <= :{col}_ate")
                    params[f"{col}_ate"] = filtros_valores[f"{col}_ate"]
                elif col in filtros_valores and filtros_valores[col]:
                    clausulas.append(f"{col} LIKE :{col}")
                    params[col] = f"%{filtros_valores[col]}%"

            where = " AND ".join(clausulas)
            sql_final = f"SELECT * FROM ({consulta_sql}) AS base WHERE 1=1"
            if where:
                sql_final += f" AND {where}"

            try:
                with st.spinner("Executando consulta..."):
                    with engine_protheus.connect() as conn:
                        df_result = pd.read_sql(text(sql_final), conn, params=params)
                    st.session_state["df_result"] = df_result
                    st.success(f"Consulta executada! {len(df_result)} registros encontrados.")
            except Exception as e:
                st.error(f"Erro na execução: {e}")
    st.markdown('</div>', unsafe_allow_html=True)

# ---------------------
# COLUNA DIREITA - Resultado
# ---------------------
with col_direita:
    st.markdown('<div class="resultado-container">', unsafe_allow_html=True)
    st.markdown("### 📊 Resultado da Consulta")

    if "df_result" in st.session_state and not st.session_state["df_result"].empty:
        df_result = st.session_state["df_result"]

        # Informações sobre o resultado em cards
        col_info1, col_info2, col_info3 = st.columns(3)
        with col_info1:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("📊 Registros", len(df_result))
            st.markdown('</div>', unsafe_allow_html=True)
        with col_info2:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("📋 Colunas", len(df_result.columns))
            st.markdown('</div>', unsafe_allow_html=True)
        with col_info3:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("💾 Tamanho", f"{df_result.memory_usage(deep=True).sum() / 1024:.1f} KB")
            st.markdown('</div>', unsafe_allow_html=True)

        # Tabela de resultados com altura fixa para melhor visualização
        st.dataframe(
            df_result,
            use_container_width=True,
            height=500
        )

        # Botões de download
        st.markdown("### 📥 Downloads")
        col_excel, col_csv = st.columns(2)

        with col_excel:
            excel_stream = BytesIO()
            df_result.to_excel(excel_stream, index=False, engine="openpyxl")
            excel_stream.seek(0)
            st.download_button(
                "📊 Baixar Excel",
                data=excel_stream,
                file_name="resultado.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

        with col_csv:
            csv_data = df_result.to_csv(index=False).encode("utf-8")
            st.download_button(
                "📄 Baixar CSV",
                data=csv_data,
                file_name="resultado.csv",
                mime="text/csv",
                use_container_width=True
            )
    else:
        # Placeholder quando não há resultado
        st.info("Execute uma consulta para ver os resultados aqui.")
        st.markdown("""
        <div style="text-align: center; padding: 3rem; color: #666;">
            <h4>🔍 Aguardando execução da consulta</h4>
            <p>Os resultados aparecerão nesta área após a execução.</p>
        </div>
        """, unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)