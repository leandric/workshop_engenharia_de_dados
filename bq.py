import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


def upload_dataframe_to_bigquery(path_file, project_id, target_table, credentials_path, write_disposition='WRITE_TRUNCATE'):
    """
    Faz upload de um DataFrame para uma tabela no BigQuery.

    Parâmetros:
    - df (pd.DataFrame): DataFrame a ser enviado
    - project_id (str): ID do projeto GCP
    - target_table (str): Nome completo da tabela de destino no formato 'dataset.tabela'
    - credentials_path (str): Caminho para o arquivo JSON de credenciais
    - write_disposition (str): Modo de gravação no BigQuery ('WRITE_TRUNCATE', 'WRITE_APPEND', etc.)

    Retorno:
    - None
    """
    df= pd.read_parquet(path_file)

    print("Informações do DataFrame:")
    print(target_table)
    print(df.info())

    # Autenticação
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(project=project_id, credentials=credentials)

    # Configuração do Job
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition=write_disposition
    )

    # Envio dos dados
    print(f"Iniciando upload para a tabela: {target_table}...")
    load_job = client.load_table_from_dataframe(
        df,
        target_table,
        job_config=job_config
    )
    load_job.result()  # Espera o job terminar

    # Confirmação
    table = client.get_table(target_table)
    print(f"Upload concluído! Total de linhas na tabela: {table.num_rows}\n")