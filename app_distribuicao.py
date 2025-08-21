# -*- coding: utf-8 -*-
"""
Aplicativo para Distribuição e Consulta de Atividades 'Verificar'
===================================================================

Este aplicativo foi desenvolvido para permitir a visualização e filtragem
de atividades do tipo 'Verificar', facilitando o acompanhamento e a
distribuição de tarefas para a equipe.

Funcionalidades Principais:
- Filtro por Período: Permite selecionar um intervalo de datas para a consulta.
- Filtro por Responsável: Seleção múltipla de usuários para visualizar suas atividades.
- Filtro por Status: Permite filtrar as atividades por um ou mais status.
- Busca por Texto: Campo para busca de texto livre dentro do conteúdo das atividades.
- Visualização em Tabela: Exibe os resultados de forma clara e organizada.
"""

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text, exc
from sqlalchemy.engine import Engine
from datetime import datetime, timedelta
from typing import Optional, List

# --- Configuração Geral da Página ---
st.set_page_config(
    layout="wide",
    page_title="Distribuição de Atividades 'Verificar'"
)

st.title(" dashboards de Distribuição e Consulta de Atividades 🚀")

# --- Conexão com o Banco de Dados (reutilizado do app anterior) ---
@st.cache_resource
def db_engine_mysql() -> Optional[Engine]:
    """
    Cria e gerencia a conexão com o banco de dados MySQL usando SQLAlchemy.
    As credenciais são lidas dos segredos do Streamlit.
    """
    try:
        # Busca as credenciais do arquivo secrets.toml
        cfg = st.secrets.get("database", {})
        db_user = cfg.get("user")
        db_password = cfg.get("password")
        db_host = cfg.get("host")
        db_name = cfg.get("name")

        if not all([db_user, db_password, db_host, db_name]):
            st.error("As credenciais do banco de dados (MySQL) não foram configuradas nos segredos.")
            return None

        # Cria a string de conexão e o engine
        connection_url = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}"
        engine = create_engine(connection_url, pool_pre_ping=True, pool_recycle=3600)

        # Testa a conexão
        with engine.connect():
            pass
        
        return engine
    except exc.SQLAlchemyError as e:
        st.error(f"Ocorreu um erro ao conectar ao banco de dados (MySQL): {e}")
        return None
    except Exception as e:
        st.error(f"Um erro inesperado ocorreu na configuração do banco de dados: {e}")
        return None

# --- Carregamento de Dados ---
@st.cache_data(ttl=600) # Cache de 10 minutos
def carregar_atividades(_eng: Engine, data_inicio: datetime.date, data_fim: datetime.date) -> pd.DataFrame:
    """
    Carrega as atividades do tipo 'Verificar' do banco de dados para um determinado período.
    """
    if _eng is None:
        return pd.DataFrame()

    query = text("""
        SELECT 
            activity_id, 
            activity_folder, 
            user_profile_name, 
            activity_date, 
            activity_status, 
            Texto
        FROM 
            ViewGrdAtividadesTarcisio
        WHERE 
            activity_type = 'Verificar' 
            AND DATE(activity_date) BETWEEN :data_inicio AND :data_fim
    """)
    try:
        with _eng.connect() as conn:
            df = pd.read_sql(query, conn, params={"data_inicio": data_inicio, "data_fim": data_fim})
        
        # Tratamento de tipos de dados para evitar erros
        if not df.empty:
            df["activity_id"] = df["activity_id"].astype(str)
            df["activity_date"] = pd.to_datetime(df["activity_date"], errors='coerce')
            df["Texto"] = df["Texto"].fillna("").astype(str)
        
        return df.sort_values("activity_date", ascending=False)
    except exc.SQLAlchemyError as e:
        st.error(f"Erro ao executar a consulta no banco de dados: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Um erro inesperado ocorreu ao carregar as atividades: {e}")
        return pd.DataFrame()

# --- Interface Principal ---
def main():
    # --- Barra Lateral de Filtros ---
    st.sidebar.header("🔍 Filtros da Consulta")

    # Filtro de Data
    data_fim_padrao = datetime.now().date()
    data_inicio_padrao = data_fim_padrao - timedelta(days=10)
    
    data_inicio = st.sidebar.date_input(
        "📅 Data de Início",
        value=data_inicio_padrao,
        help="Selecione a data inicial do período de busca."
    )
    data_fim = st.sidebar.date_input(
        "📅 Data de Fim",
        value=data_fim_padrao,
        help="Selecione a data final do período de busca."
    )

    # Validação de datas
    if data_inicio > data_fim:
        st.sidebar.error("A data de início não pode ser posterior à data de fim.")
        st.stop()

    # Botão para forçar a atualização dos dados
    if st.sidebar.button("🔄 Recarregar Dados", use_container_width=True):
        st.cache_data.clear()
        st.success("Cache de dados limpo! Os dados serão recarregados.")
        st.rerun()

    # Conecta ao banco e carrega os dados brutos do período selecionado
    engine = db_engine_mysql()
    if engine is None:
        st.warning("A aplicação não pôde se conectar ao banco de dados. Verifique as configurações.")
        st.stop()
        
    df_atividades = carregar_atividades(engine, data_inicio, data_fim)

    if df_atividades.empty:
        st.info("Nenhuma atividade do tipo 'Verificar' foi encontrada para o período selecionado.")
    else:
        # --- Filtros Dinâmicos baseados nos dados carregados ---
        
        # Filtro de Responsáveis (multiselect) - AGORA DIRETAMENTE DO BANCO
        lista_responsaveis = sorted(df_atividades['user_profile_name'].dropna().unique().tolist())
        
        usuarios_selecionados = st.sidebar.multiselect(
            "👤 Responsáveis",
            options=lista_responsaveis,
            help="Selecione um ou mais usuários para filtrar as atividades."
        )

        # Filtro de Status (multiselect)
        status_disponiveis = sorted(df_atividades['activity_status'].dropna().unique().tolist())
        status_selecionados = st.sidebar.multiselect(
            "📊 Status",
            options=status_disponiveis,
            help="Selecione um ou mais status para filtrar."
        )

        # Filtro de Texto
        texto_busca = st.sidebar.text_input(
            "📝 Buscar no Texto",
            help="Digite qualquer texto para buscar no conteúdo das atividades."
        )

        # --- Aplicação dos Filtros ---
        df_filtrado = df_atividades.copy()

        if usuarios_selecionados:
            df_filtrado = df_filtrado[df_filtrado['user_profile_name'].isin(usuarios_selecionados)]
        
        if status_selecionados:
            df_filtrado = df_filtrado[df_filtrado['activity_status'].isin(status_selecionados)]

        if texto_busca:
            # A busca é case-insensitive (não diferencia maiúsculas de minúsculas)
            df_filtrado = df_filtrado[df_filtrado['Texto'].str.contains(texto_busca, case=False, na=False)]

        # --- Exibição dos Resultados ---
        st.markdown("---")
        
        # Métricas
        col1, col2 = st.columns(2)
        col1.metric("Total de Atividades no Período", len(df_atividades))
        col2.metric("Atividades Encontradas (após filtros)", len(df_filtrado))

        st.markdown("### 📋 Tabela de Atividades")
        
        # Configuração de exibição do DataFrame
        st.dataframe(
            df_filtrado,
            use_container_width=True,
            hide_index=True,
            column_config={
                "activity_id": st.column_config.TextColumn("ID Atividade"),
                "activity_folder": st.column_config.TextColumn("Pasta"),
                "user_profile_name": st.column_config.TextColumn("Responsável"),
                "activity_date": st.column_config.DatetimeColumn(
                    "Data",
                    format="DD/MM/YYYY HH:mm"
                ),
                "activity_status": st.column_config.TextColumn("Status"),
                "Texto": st.column_config.TextColumn("Conteúdo")
            }
        )

if __name__ == "__main__":
    main()
