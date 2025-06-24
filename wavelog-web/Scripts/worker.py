import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging
import sys
import argparse
import os
import zipfile
from kaggle.api.kaggle_api_extended import KaggleApi

# --- Configuração dos Logs ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    stream=sys.stdout)

# --- Funções de Processamento ---
def processar_e_inserir_portos(conexao, dataframe):
    logging.info("Processando tabela 'Porto'...")
    portos_df = dataframe[['portname', 'country', 'ISO3']].drop_duplicates().dropna()
    cursor = conexao.cursor()
    query_select = "SELECT id_porto FROM Porto WHERE nome_porto = %s AND pais = %s"
    query_insert = "INSERT INTO Porto (nome_porto, pais, codigo_iso_pais) VALUES (%s, %s, %s)"

    for _, linha in portos_df.iterrows():
        cursor.execute(query_select, (linha['portname'], linha['country']))
        if cursor.fetchone() is None:
            cursor.execute(query_insert, (linha['portname'], linha['country'], linha['ISO3']))

    conexao.commit()
    cursor.close()
    logging.info("Tabela 'Porto' atualizada.")


def processar_e_inserir_datas(conexao, dataframe):
    logging.info("Processando tabela 'Data'...")
    dataframe['date'] = pd.to_datetime(dataframe['date'], errors='coerce')
    datas_unicas = dataframe['date'].drop_duplicates().dropna()
    cursor = conexao.cursor()
    query_select = "SELECT data FROM Data WHERE data = %s"
    query_insert = "INSERT INTO Data (data, ano, mes, dia) VALUES (%s, %s, %s, %s)"

    for data in datas_unicas:
        cursor.execute(query_select, (data.date(),))
        if cursor.fetchone() is None:
            cursor.execute(query_insert, (data.date(), data.year, data.month, data.day))

    conexao.commit()
    cursor.close()
    logging.info("Tabela 'Data' atualizada.")


def processar_e_inserir_movimentacao(conexao, dataframe):
    logging.info("Processando tabela 'MovimentacaoPortuaria'...")
    MAPA_COLUNAS = {
        'portname': 'nome_porto', 'country': 'pais', 'ISO3': 'codigo_iso_pais', 'date': 'data',
        'portcalls_container': 'chamadas_navios_container', 'portcalls_dry_bulk': 'chamadas_navios_granel_solido',
        'portcalls_general_cargo': 'chamadas_navios_carga_geral', 'portcalls_roro': 'chamadas_navios_roro',
        'portcalls_tanker': 'chamadas_navios_tanque', 'portcalls_cargo': 'chamadas_navios_carga',
        'portcalls': 'chamadas_navios_total', 'import_container': 'importacao_container',
        'import_dry_bulk': 'importacao_granel_solido', 'import_general_cargo': 'importacao_carga_geral',
        'import_roro': 'importacao_roro', 'import_tanker': 'importacao_tanque',
        'import_cargo': 'importacao_carga', 'import': 'importacao_total',
        'export_container': 'exportacao_container', 'export_dry_bulk': 'exportacao_granel_solido',
        'export_general_cargo': 'exportacao_carga_geral', 'export_roro': 'exportacao_roro',
        'export_tanker': 'exportacao_tanque', 'export_cargo': 'exportacao_carga',
        'export': 'exportacao_total'
    }
    df_renomeado = dataframe.rename(columns=MAPA_COLUNAS)
    cursor = conexao.cursor()

    query_get_porto_id = "SELECT id_porto FROM Porto WHERE nome_porto = %s AND pais = %s"
    query_check_mov = "SELECT id_movimentacao FROM MovimentacaoPortuaria WHERE id_porto = %s AND data = %s"

    colunas_db = list(MAPA_COLUNAS.values())[4:]
    colunas_sql = ", ".join(colunas_db)
    placeholders_sql = ", ".join(["%s"] * len(colunas_db))
    query_insert_mov = f"INSERT INTO MovimentacaoPortuaria (id_porto, data, {colunas_sql}) VALUES (%s, %s, {placeholders_sql})"

    for _, linha in df_renomeado.iterrows():
        cursor.execute(query_get_porto_id, (linha['nome_porto'], linha['pais']))
        id_porto_result = cursor.fetchone()
        if id_porto_result:
            id_porto = id_porto_result[0]
            data_mov = linha['data'].date()
            cursor.execute(query_check_mov, (id_porto, data_mov))
            if cursor.fetchone() is None:
                valores = [id_porto, data_mov] + [linha.get(col, 0) if pd.notna(linha.get(col, 0)) else 0 for col in colunas_db]
                cursor.execute(query_insert_mov, tuple(valores))

    conexao.commit()
    cursor.close()
    logging.info("Tabela 'MovimentacaoPortuaria' atualizada.")


def atualizar_totais_diarios(conexao, dataframe):
    logging.info("Calculando e atualizando totais diários...")
    totais_diarios = dataframe.groupby('date')[['import', 'export']].sum().reset_index()
    cursor = conexao.cursor()
    query_update = "UPDATE Data SET importacao_total_dia = %s, exportacao_total_dia = %s WHERE data = %s"

    for _, linha in totais_diarios.iterrows():
        data = linha['date'].date()
        total_import = linha['import']
        total_export = linha['export']
        cursor.execute(query_update, (total_import, total_export, data))

    conexao.commit()
    cursor.close()
    logging.info("Totais diários atualizados com sucesso.")

def funcao_principal():
    """Organiza e executa todas as etapas do processo."""
    parser = argparse.ArgumentParser(description="Job de atualização de dados portuários.")
    parser.add_argument("--host", required=True, help="Host da base de dados")
    parser.add_argument("--database", required=True, help="Nome da base de dados")
    parser.add_argument("--user", required=True, help="Utilizador da base de dados")
    parser.add_argument("--password", required=True, help="Palavra-passe da base de dados")
    argumentos = parser.parse_args()

    config_banco = {
        'host': argumentos.host, 'database': argumentos.database,
        'user': argumentos.user, 'password': argumentos.password
    }

    logging.info("INICIANDO JOB DE ATUALIZAÇÃO (WORKER PYTHON)")
    conexao_bd = None
    try:
        #Conectar à base de dados
        logging.info("A ligar à base de dados...")
        conexao_bd = mysql.connector.connect(**config_banco)
        if not conexao_bd.is_connected():
            raise Exception("Não foi possível conectar à base de dados.")
        logging.info("Ligação bem-sucedida.")

        # Baixar e ler os dados do Kaggle via API 
        logging.info("A carregar o dataset do Kaggle...")
        api = KaggleApi()
        api.authenticate()

        dataset_name = "arunvithyasegar/daily-port-activity-data-and-trade-estimates"

        script_dir = os.path.dirname(os.path.abspath(__file__))
        download_path = os.path.join(script_dir, 'kaggle_dataset')

        logging.info(f"Usando caminho absoluto para download: {download_path}")

        if not os.path.exists(download_path):
            os.makedirs(download_path)

        api.dataset_download_files(dataset_name, path=download_path, unzip=False)
        logging.info(f"Dataset descarregado como 'archive.zip' em '{download_path}'")

        # Encontrar dinamicamente o arquivo ZIP baixado
        zip_file_name = None
        for file in os.listdir(download_path):
            if file.endswith('.zip'):
                zip_file_name = file
                logging.info(f"Arquivo ZIP encontrado: {zip_file_name}")
                break

        if zip_file_name is None:
            raise FileNotFoundError(f"Nenhum arquivo .zip encontrado na pasta {download_path} após o download.")

        zip_file_path = os.path.join(download_path, zip_file_name)

        # Descompactar
        logging.info(f"Descompactando {zip_file_name}...")
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(download_path)

        # Remover o ZIP
        os.remove(zip_file_path)
        logging.info(f"Arquivo {zip_file_name} descompactado e removido com sucesso.")

        # Encontrar dinamicamente o nome do arquivo .csv
        csv_file_name = None
        for file in os.listdir(download_path):
            if file.endswith('.csv'):
                csv_file_name = file
                logging.info(f"Ficheiro CSV encontrado: {csv_file_name}")
                break

        if csv_file_name is None:
            logging.error(f"Nenhum ficheiro .csv encontrado em '{download_path}' após descompactar.")
            raise FileNotFoundError(f"Nenhum ficheiro .csv encontrado em {download_path}")

        # Usa o nome do ficheiro encontrado para carregar o DataFrame
        caminho_ficheiro = os.path.join(download_path, csv_file_name)
        logging.info(f"A ler o ficheiro: {caminho_ficheiro}")
        df_dados = pd.read_csv(caminho_ficheiro)
        logging.info("DataFrame carregado com sucesso!")

        # Preparar os dados
        logging.info("A preparar os dados para inserção...")
        df_dados['date'] = pd.to_datetime(df_dados['date'], errors='coerce')
        df_dados.dropna(subset=['date', 'portname', 'country', 'ISO3'], inplace=True)

        # Executar as funções de inserção na ordem correta
        processar_e_inserir_portos(conexao_bd, df_dados)
        processar_e_inserir_datas(conexao_bd, df_dados)
        processar_e_inserir_movimentacao(conexao_bd, df_dados)
        atualizar_totais_diarios(conexao_bd, df_dados)

        logging.info("JOB FINALIZADO COM SUCESSO PELO PYTHON")

    except Exception as erro:
        logging.error(f"Ocorreu um erro crítico durante a execução: {erro}", exc_info=True)
        sys.exit(1)
    finally:
        if conexao_bd and conexao_bd.is_connected():
            conexao_bd.close()
            logging.info("Conexão com a base de dados fechada.")


if __name__ == '__main__':
    funcao_principal()
