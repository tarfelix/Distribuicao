# -*- coding: utf-8 -*-
"""
Ferramenta de Apoio à Distribuição de Atividades 'Verificar'
===================================================================

Este aplicativo foi redesenhado para focar na distribuição inteligente
de atividades do tipo 'Verificar'. O objetivo principal é fornecer contexto
histórico para cada atividade que está atualmente em aberto.

Funcionalidades Principais:
- Login de Usuário: Acesso seguro utilizando credenciais armazenadas no
  Streamlit secrets.
- Visão Focada: Lista todas as atividades com status 'Aberta'.
- Ordenação Inteligente: Ordena as atividades por responsável e depois por pasta.
- Destaque Visual: Usa cores para alertar sobre múltiplas atividades abertas
  na mesma pasta.
- Contexto Histórico: Para cada atividade aberta, exibe todas as outras
  atividades da mesma pasta dentro do período de tempo selecionado.
- Filtros Inteligentes: Os filtros de responsável, pasta e texto se aplicam
  apenas às atividades abertas.
"""

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text, exc
from sqlalchemy.engine import Engine
from datetime import datetime, timedelta
from typing import Optional

# --- Chave de Sessão para Login ---
USERNAME_KEY = "username_distro_app"

# --- Configuração Geral da Página ---
st.set_page_config(
    layout="wide",
    page_title="Apoio à Distribuição de 'Verificar'"
)

st.title("Apoio à Distribuição de Atividades 'Verificar'")

# --- Conexão com o Banco de Dados ---
@st.cache_resource
def db_engine_mysql() -> Optional[Engine]:
    """
    Cria e gerencia a conexão com o banco de dados MySQL usando SQLAlchemy.
    As credenciais são lidas dos segredos do Streamlit.
    """
    try:
        cfg = st.secrets.get("database", {})
        db_user, db_password, db_host, db_name = cfg.get("user"), cfg.get("password"), cfg.get("host"), cfg.get("name")

        if not all([db_user, db_password, db_host, db_name]):
            st.error("As credenciais do banco de dados (MySQL) não foram configuradas nos segredos.")
            return None

        connection_url = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}"
        engine = create_engine(connection_url, pool_pre_ping=True, pool_recycle=3600)
        with engine.connect():
            pass
        return engine
    except exc.SQLAlchemyError as e:
        st.error(f"Ocorreu um erro ao conectar ao banco de dados (MySQL): {e}")
        return None

# --- Carregamento de Dados ---
@st.cache_data(ttl=300) # Cache de 5 minutos
def carregar_dados_contextuais(_eng: Engine, data_inicio: datetime.date, data_fim: datetime.date) -> pd.DataFrame:
    """
    Carrega dados de forma contextual e corrigida.
    1. Encontra todas as pastas que têm pelo menos uma atividade 'Verificar' aberta.
    2. Busca TODAS as atividades 'Abertas' dessas pastas (independente da data).
    3. Busca as DEMAIS atividades (histórico) dessas pastas que ocorreram no período de tempo especificado.
    """
    if _eng is None:
        return pd.DataFrame()

    query = text("""
        WITH PastasComAbertas AS (
            SELECT DISTINCT activity_folder
            FROM ViewGrdAtividadesTarcisio
            WHERE activity_type = 'Verificar' AND activity_status = 'Aberta'
        )
        SELECT 
            v.activity_id, 
            v.activity_folder, 
            v.user_profile_name, 
            v.activity_date, 
            v.activity_status, 
            v.Texto
        FROM 
            ViewGrdAtividadesTarcisio v
        JOIN 
            PastasComAbertas p ON v.activity_folder = p.activity_folder
        WHERE 
            v.activity_type = 'Verificar' 
            AND (
                v.activity_status = 'Aberta' OR
                DATE(v.activity_date) BETWEEN :data_inicio AND :data_fim
            )
    """)
    try:
        with _eng.connect() as conn:
            df = pd.read_sql(query, conn, params={"data_inicio": data_inicio, "data_fim": data_fim})
        
        if not df.empty:
            df["activity_id"] = df["activity_id"].astype(str)
            df["activity_date"] = pd.to_datetime(df["activity_date"], errors='coerce')
            df["Texto"] = df["Texto"].fillna("").astype(str)
        
        return df.sort_values("activity_date", ascending=False)
    except exc.SQLAlchemyError as e:
        st.error(f"Erro ao executar a consulta no banco de dados: {e}")
        return pd.DataFrame()

# --- Interface Principal ---
def main():
    if USERNAME_KEY not in st.session_state:
        st.session_state[USERNAME_KEY] = None

    if not st.session_state.get(USERNAME_KEY):
        st.sidebar.header("🔐 Login")
        with st.sidebar.form("login_form"):
            username = st.text_input("Nome de Usuário")
            password = st.text_input("Senha", type="password")
            submitted = st.form_submit_button("Entrar")
            if submitted:
                creds = st.secrets.get("credentials", {})
                user_creds = creds.get("usernames", {})
                if username in user_creds and user_creds[username] == password:
                    st.session_state[USERNAME_KEY] = username
                    st.rerun()
                else:
                    st.sidebar.error("Usuário ou senha inválidos.")
        
        st.info("👋 Bem-vindo! Por favor, faça o login na barra lateral para continuar.")
        st.stop()

    st.sidebar.success(f"Logado como: **{st.session_state[USERNAME_KEY]}**")
    st.sidebar.header("🔍 Filtros da Consulta")

    data_fim_padrao = datetime.now().date()
    data_inicio_padrao = data_fim_padrao - timedelta(days=10)
    
    st.sidebar.info("O filtro de data define o período para buscar o **histórico de contexto** das atividades abertas.")
    data_inicio = st.sidebar.date_input("📅 Início do Histórico", value=data_inicio_padrao)
    data_fim = st.sidebar.date_input("📅 Fim do Histórico", value=data_fim_padrao)

    if data_inicio > data_fim:
        st.sidebar.error("A data de início não pode ser posterior à data de fim.")
        st.stop()

    if st.sidebar.button("🔄 Recarregar Dados", use_container_width=True):
        st.cache_data.clear()
        st.success("Cache limpo! Os dados serão recarregados.")
        st.rerun()

    engine = db_engine_mysql()
    if engine is None:
        st.warning("Aplicação não conectada ao banco de dados.")
        st.stop()
        
    df_contexto_total = carregar_dados_contextuais(engine, data_inicio, data_fim)

    if df_contexto_total.empty:
        st.info("Nenhuma atividade 'Verificar' em aberto foi encontrada ou não há histórico para elas no período selecionado.")
        st.stop()

    df_abertas = df_contexto_total[df_contexto_total['activity_status'] == 'Aberta'].copy()
    
    st.sidebar.markdown("---")
    st.sidebar.header("🔎 Filtrar Atividades Abertas")

    lista_pastas = sorted(df_abertas['activity_folder'].dropna().unique().tolist())
    pastas_selecionadas = st.sidebar.multiselect("📁 Pastas", options=lista_pastas)

    lista_responsaveis = sorted(df_abertas['user_profile_name'].dropna().unique().tolist())
    usuarios_selecionados = st.sidebar.multiselect("👤 Responsáveis", options=lista_responsaveis)
    
    texto_busca = st.sidebar.text_input("📝 Buscar no Texto")

    df_abertas_filtrado = df_abertas
    if pastas_selecionadas:
        df_abertas_filtrado = df_abertas_filtrado[df_abertas_filtrado['activity_folder'].isin(pastas_selecionadas)]
    if usuarios_selecionados:
        df_abertas_filtrado = df_abertas_filtrado[df_abertas_filtrado['user_profile_name'].isin(usuarios_selecionados)]
    if texto_busca:
        df_abertas_filtrado = df_abertas_filtrado[df_abertas_filtrado['Texto'].str.contains(texto_busca, case=False, na=False)]

    # --- Lógica de Destaque e Ordenação ---
    # Contagem de atividades abertas por pasta
    contagem_pastas = df_abertas_filtrado['activity_folder'].value_counts()
    df_abertas_filtrado['alerta_pasta'] = df_abertas_filtrado['activity_folder'].map(contagem_pastas) > 1

    # Ordena por Responsável e depois por Pasta
    df_abertas_filtrado = df_abertas_filtrado.sort_values(
        by=['user_profile_name', 'activity_folder', 'activity_date'], 
        ascending=[True, True, False]
    )

    # --- Exibição dos Resultados ---
    st.metric("Total de Atividades 'Verificar' Abertas (após filtros)", len(df_abertas_filtrado))
    
    # Adiciona a legenda de cores
    st.markdown(
        """
        <style>
            .legenda { display: flex; align-items: center; margin-bottom: 1rem; }
            .cor-box { width: 20px; height: 20px; margin-right: 10px; border: 1px solid #ccc; }
            .vermelho { background-color: #ffcdd2; }
            .preto { background-color: #f5f5f5; }
        </style>
        <div class="legenda">
            <div class="cor-box vermelho"></div><span><b>Alerta:</b> Mais de uma atividade 'Aberta' na mesma pasta.</span>
        </div>
        <div class="legenda">
            <div class="cor-box preto"></div><span><b>Normal:</b> Apenas uma atividade 'Aberta' na pasta.</span>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.caption(f"Exibindo atividades abertas e seu histórico de contexto entre {data_inicio.strftime('%d/%m/%Y')} e {data_fim.strftime('%d/%m/%Y')}.")
    st.markdown("---")

    for _, atividade_aberta in df_abertas_filtrado.iterrows():
        pasta = atividade_aberta['activity_folder']
        
        cor_header = "color: red;" if atividade_aberta['alerta_pasta'] else "color: black;"
        
        expander_title = (
            f"<span style='{cor_header}'>"
            f"ID: {atividade_aberta['activity_id']} | Pasta: {pasta} | "
            f"Aberta em: {atividade_aberta['activity_date'].strftime('%d/%m/%Y %H:%M')} | "
            f"Responsável Atual: {atividade_aberta['user_profile_name']}"
            f"</span>"
        )

        with st.expander(expander_title, expanded=False):
            st.subheader("Detalhes da Atividade em Aberto")
            st.text_area(
                "Conteúdo", 
                atividade_aberta['Texto'], 
                key=f"texto_{atividade_aberta['activity_id']}",
                height=150,
                disabled=True
            )

            st.subheader(f"Histórico da Pasta '{pasta}' no Período")
            
            df_historico_pasta = df_contexto_total[df_contexto_total['activity_folder'] == pasta]
            
            if df_historico_pasta.empty:
                st.info("Nenhum outro histórico encontrado para esta pasta no período.")
            else:
                st.dataframe(
                    df_historico_pasta,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "activity_id": "ID",
                        "activity_folder": None,
                        "user_profile_name": "Responsável",
                        "activity_date": st.column_config.DatetimeColumn("Data", format="DD/MM/YYYY HH:mm"),
                        "activity_status": "Status",
                        "Texto": None
                    }
                )

if __name__ == "__main__":
    main()
