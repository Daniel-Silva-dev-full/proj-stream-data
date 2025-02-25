import streamlit as st
import psycopg2
import mysql.connector
from mysql.connector import errorcode
import time
import random
import os
import logging
from threading import Thread, Event
from dotenv import load_dotenv
import pandas as pd
from psycopg2.pool import SimpleConnectionPool
from mysql.connector.pooling import MySQLConnectionPool
from functools import wraps
from contextlib import contextmanager
import pytz
from datetime import datetime

#configuracao do log
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()
fuso_brasil = pytz.timezone('America/Sao_Paulo')

#configurar conexao postgres simulando a aplicacao
PG_CONFIG = {
    'host': os.getenv("PG_HOST"),
    'user': os.getenv("PG_USER"),
    'password': os.getenv("PG_PASSWORD"),
    'database': os.getenv("PG_DATABASE"),
    'port': os.getenv("PG_PORT")
}
#configurar conexao mysql simulando o banco analitico
MYSQL_CONFIG = {
    'host': os.getenv("MYSQL_HOST"),
    'user': os.getenv("MYSQL_USER"),
    'password': os.getenv("MYSQL_PASSWORD"),
    'database': os.getenv("MYSQL_DATABASE"),
    'port': os.getenv("MYSQL_PORT")
}

#pools de conexao
pools = 5

#inicia os pools
pg_pool = SimpleConnectionPool(
    minconn=1,
    maxconn=pools,
    **PG_CONFIG
)

mysql_pool = MySQLConnectionPool(
    pool_name="mysql_pool",
    pool_size=pools,
    **MYSQL_CONFIG
)

#decorador para reconetar automaticamente
def reconecta_db(max_retries=3, delay=1, backoff=2):
    def decorator(func):
        @wraps(func)
        def reconectar(*args, **kwargs):
            retries, current_delay = 0, delay
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except (psycopg2.OperationalError, mysql.connector.Error) as e:
                    logger.error(f"Rrro na conexao, tentando reconectar.")
                    time.sleep(current_delay)
                    retries += 1
                    current_delay *= backoff
            return func(*args, **kwargs)
        return reconectar
    return decorator

#gerenciador da conexao
@contextmanager
def post_connection():
    conn = pg_pool.getconn()
    try:
        yield conn
    finally:
        pg_pool.putconn(conn)

@contextmanager
def mysql_connection():
    conn = mysql_pool.get_connection()
    try:
        yield conn
    finally:
        conn.close()

#criar as tabelas de validacao
def inicia_db():
    try:
        # Criar tabela de validacao
        with mysql_connection() as conn, conn.cursor() as cursor:
            cursor.execute(''' 
                CREATE TABLE IF NOT EXISTS validacao (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    ultimo_id BIGINT NOT NULL,
                    alteracao_data TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.commit()
            
            # Inicializa a validacao
            cursor.execute("SELECT ultimo_id FROM validacao ORDER BY id DESC LIMIT 1")
            if not cursor.fetchone():
                cursor.execute("INSERT INTO validacao (ultimo_id) VALUES (0)")
                conn.commit()
    except Exception as e:
        logger.error(f"Erro: {e}")

#gera dados simulando as transacoes
def gera_transacao_simulada(stop_event):
    while not stop_event.is_set():
        try:
            with post_connection() as conn, conn.cursor() as cursor:
                transaction = (
                    round(random.uniform(10, 10000), 2),
                    random.choice(["BH", "São Paulo", "Contagem", "Fortaleza"]),
                    random.choice(["Construo Software", "Mindbi IA e Dados", "Mercado Livre", "Nubank"]),
                    datetime.now(fuso_brasil) 
                )
                cursor.execute(
                    "INSERT INTO transacoes (valor, origem, destino, timestamp) VALUES (%s, %s, %s, %s)",
                    transaction
                )
                conn.commit()
                time.sleep(random.uniform(1, 5))
        except Exception as e:
            logger.error(f"Erro: {e}")
            time.sleep(5)

#faz a replicacao dos dados
def replica_dados(para_pool):
    inicia_db()
    batch = 100
    tempo_retry = 5

    while not para_pool.is_set():
        try:
            with post_connection() as pg_conn, \
                 mysql_connection() as mysql_conn, \
                 mysql_conn.cursor() as mysql_cursor:

                #obtem ultimo id
                mysql_cursor.execute("SELECT ultimo_id FROM validacao ORDER BY id DESC LIMIT 1")
                ultimo_id = mysql_cursor.fetchone()[0]

                #busca novos registros
                with pg_conn.cursor() as pg_cursor:
                    pg_cursor.execute(
                        "SELECT id, valor, origem, destino, timestamp "
                        "FROM transacoes WHERE id > %s ORDER BY id LIMIT %s",
                        (ultimo_id, batch)
                    )
                    novos_registros = pg_cursor.fetchall()

                if novos_registros:
                    # inserir em lote removendo as duplicadas
                    try:
                        mysql_cursor.executemany(
                            "INSERT IGNORE INTO transacoes_mysql (id, valor, origem, destino, timestamp) "
                            "VALUES (%s, %s, %s, %s, %s)",
                            novos_registros
                        )
                        
                        # AAtualiza a validacao
                        novo_ult_id = novos_registros[-1][0]
                        mysql_cursor.execute(
                            "INSERT INTO validacao (ultimo_id) VALUES (%s)",
                            (novo_ult_id,)
                        )
                        mysql_conn.commit()
                        logger.info(f"Replicado {len(novos_registros)} registros")
                        
                    except mysql.connector.Error as e:
                        mysql_conn.rollback()
                        logger.error(f"Erro no Mysql: {e}")
                        time.sleep(tempo_retry)

        except Exception as e:
            logger.error(f"Erro: {e}")
            time.sleep(tempo_retry)

        time.sleep(5)

# app para fazer a exibicao
def app_principal():
    st.set_page_config(layout="wide")
    st.title("Monitoramento de Transações entre Postgres e Mysql")
    
    intervalo_refresh = st.sidebar.slider("Intervalo de atualizacoes", 1, 20, 5)
    
    @st.cache_data(ttl=1)
    def ler_dados_dash():
        try:
            with mysql_connection() as conn, conn.cursor(dictionary=True) as cursor:
                cursor.execute("""
                    SELECT id, valor, origem, destino, timestamp 
                    FROM transacoes_mysql 
                    ORDER BY timestamp DESC 
                    LIMIT 100
                """)
                data = cursor.fetchall()

                for row in data:
                    row['timestamp'] = row['timestamp'].astimezone(fuso_brasil)

                return pd.DataFrame(data)
        except Exception as e:
            logger.error(f"Erro: {e}")
            return pd.DataFrame()

    #atualizacao automatica
    placeholder = st.empty()
    while True:
        df = ler_dados_dash()
        
        if not df.empty:
            with placeholder.container():
                st.subheader("Últimas Transações")
                st.dataframe(
                    df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "valor": st.column_config.NumberColumn(format="$ %.2f"),
                        "timestamp": "Data/Hora"
                    }
                )
                
                #Indicadores
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total de Transações", len(df))
                with col2:
                    st.metric("Valor Médio", f"${df['valor'].mean():.2f}")
                with col3:
                    st.metric("Última Atualização", pd.Timestamp.now().strftime("%H:%M:%S"))
                
                #grafico barras
                st.subheader("Transações por minuto")
                df['data'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M')
                transacoes_por_data_hora_segundo = df.groupby('data').size()
                st.line_chart(transacoes_por_data_hora_segundo)
        else:
            st.warning("processando novos dados...")
        
        time.sleep(intervalo_refresh)

if __name__ == "__main__":
    stop_event = Event()
    
    try:
        #iniciando as thread
        Thread(target=gera_transacao_simulada, args=(stop_event,), daemon=True).start()
        Thread(target=replica_dados, args=(stop_event,), daemon=True).start()
        
        #inicia o streamlit
        app_principal()
    except KeyboardInterrupt:
        logger.info("Parando..")
        stop_event.set()
        pg_pool.closeall()
