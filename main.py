
import os
import sys
import pandas as pd
import psycopg
import vault_cli
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime, timedelta
import shutil
from log import Log

# --- Configuração de logging ---
log = Log()

# --- Função para mensagens de erro ---
def mensagem_error(mensagem, error, encerrar=False):
    log.error.error(f"{mensagem} | Detalhes: {error}")
    if encerrar:
        sys.exit(1)

def mover_arquivo_para_nao_processado(caminho_arquivo):
    pasta_backup = Path("Nao_Processados")
    pasta_backup.mkdir(exist_ok=True)
    destino = pasta_backup / caminho_arquivo.name
    shutil.move(str(caminho_arquivo), str(destino))
    log.info.info(f"Arquivo movido para não processado: {destino}")

# --- Carrega variáveis do .env ---
load_dotenv()

# --- Utilitários ---
def converte_formato_hora(hora):
    segundos = int(hora * 3600)
    return (datetime.min + timedelta(seconds=segundos)).strftime('%H:%M:%S')

# --- Conecta ao Vault ---
def conectar_vault():
    try:
        vault = vault_cli.get_client(url=os.getenv('VAULT_URL'), token=os.getenv('VAULT_TOKEN'))
        return {
            "host": vault.get_secret('secret/saude-service', 'saudeDBHost'),
            "dbname": vault.get_secret('secret/saude-service', 'saudeDBBase'),
            "user": vault.get_secret('secret/saude-service', 'saudeDBUser'),
            "password": vault.get_secret('secret/saude-service', 'saudeDBPass')
        }
    except Exception as e:
        mensagem_error("Erro ao conectar ao Vault", e, True)

# --- Conecta ao PostgreSQL ---
def conectar_postgres(config):
    try:
        return psycopg.connect(**config)
    except Exception as e:
        mensagem_error("Erro ao conectar ao PostgreSQL", e, True)

# --- Processa métricas ---
def processar_metricas(cur, df):
    for indice, linha in df.iterrows():
        data = str(df.iloc[indice, 0]).split(' ')[0]
        log.info.info(f"Processando métrica do dia: {data}")
        try:
            cur.execute("SELECT id FROM metricas WHERE data = %s", (data,))
            retorno = cur.fetchall()
            if retorno:
                cur.execute("DELETE FROM metricas WHERE id = %s", (retorno[0][0],))
            sql = """
                INSERT INTO metricas (
                    data, sono, passos, distancia, energia_ativa,
                    frequencia_cardiaca_maxima, frequencia_cardiaca_minima,
                    frequencia_cardiaca_media, frequencia_cardiaca_em_repouso,
                    hora_de_ficar_em_pe, frequencia_cardiaca_ao_caminhar_media,
                    peso, saturacao_de_oxigenio, tempo_em_pe, exposicao_ao_sol, criado
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
                )
            """
            valores = (
                data, converte_formato_hora(df.iloc[indice, 1]), df.iloc[indice, 8], df.iloc[indice, 9], 
                df.iloc[indice, 10], df.iloc[indice, 11], df.iloc[indice, 12], df.iloc[indice, 13], df.iloc[indice, 14], 
                df.iloc[indice, 15], df.iloc[indice, 16], df.iloc[indice, 17], df.iloc[indice, 18], df.iloc[indice, 19], 
                df.iloc[indice, 20]
            )
            cur.execute(sql, valores)
        except Exception as e:
            mensagem_error(f"Erro ao processar métrica do dia {data}", e)
            raise

# --- Processa treinos ---
def processar_treinos(cur, df):
    for indice, linha in df.iterrows():
        descricao, inicio = df.iloc[indice, 0], df.iloc[indice, 1]
        log.info.info(f"Processando treino: {descricao} do dia: {inicio}")
        try:
            cur.execute("SELECT id FROM atividades WHERE descricao = %s", (descricao,))
            retorno = cur.fetchall()
            if retorno:
                atividade_id = retorno[0][0]
            else:
                cur.execute("INSERT INTO atividades (descricao, criado) VALUES (%s, CURRENT_TIMESTAMP) RETURNING id", (descricao,))
                atividade_id = cur.fetchone()[0]

            cur.execute("SELECT id FROM treinos WHERE inicio = %s", (inicio + ":00",))
            retorno = cur.fetchall()
            if retorno:
                cur.execute("DELETE FROM treinos WHERE id = %s", (retorno[0][0],))

            sql = """
                INSERT INTO treinos (
                    atividade_id, inicio, fim, duracao, energia_ativa,
                    frequencia_cardiaca_maxima, frequencia_cardiaca_media,
                    distancia, velocidade_media, criado
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """
            valores = (
                atividade_id, df.iloc[indice, 1], df.iloc[indice, 2], df.iloc[indice, 3], df.iloc[indice, 5], 
                df.iloc[indice, 6], df.iloc[indice, 7], df.iloc[indice, 8], df.iloc[indice, 9]
            )
            cur.execute(sql, valores)
        except Exception as e:
            mensagem_error(f"Erro ao processar treino {descricao}", e)
            raise

# --- Função principal ---
def main():
    config_db = conectar_vault()
    pastas = ["Metricas", "Treinos"]
    pasta_atual = Path.cwd()

    with conectar_postgres(config_db) as conn:
        with conn.cursor() as cur:
            for pasta in pastas:
                caminho_total = pasta_atual / pasta
                for caminho_arquivo in Path(caminho_total).rglob("*.csv"):
                    log.info.info(f"Processando arquivo: {caminho_arquivo}")
                    try:
                        df = pd.read_csv(caminho_arquivo)
                        df.fillna(0, inplace=True)

                        if pasta == "Metricas":
                            processar_metricas(cur, df)
                        else:
                            processar_treinos(cur, df)

                        conn.commit()
                    except Exception as e:
                        conn.rollback()
                        mensagem_error(f"Erro ao processar arquivo {caminho_arquivo}", e)
                        mover_arquivo_para_nao_processado(caminho_arquivo)
                    else:
                        os.remove(caminho_arquivo)
                        log.info.info(f"Arquivo removido: {caminho_arquivo}")

if __name__ == "__main__":
    main()