# importar bibliotecas
from google.cloud import bigquery
from google.oauth2 import service_account
import os


def gcp_connection(file_key):
    ##########################################################################
    #                        Cria a conexão com o GCP                        #
    ##########################################################################

    print("##########################################################################")
    print("#                     Iniciando execução do programa                     #")
    print("##########################################################################")
    print("--------------------------------------------------------------------------")
    print("Criando conexão com o GCP...")
    try:
        current_directory = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(current_directory, file_key)
        credentials = service_account.Credentials.from_service_account_file(file_path)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        print(f"Conexão realizada com sucesso com o projeto {credentials.project_id}.")
        print("--------------------------------------------------------------------------")
    except Exception:
        print(f"Não foi possível efetivar a conexão com o GCP.")
        print("--------------------------------------------------------------------------")
    return client

def dataset_exist(client,dataset_name):
    ##########################################################################
    #                     Cria o dataset caso não exista                     #
    ##########################################################################
    print("--------------------------------------------------------------------------")
    print("Verificando a existência do dataset no GCP...")
    dataset_fonte = client.dataset(dataset_name)
    try:
        client.get_dataset(dataset_fonte)
        print(f"O conjunto de dados {dataset_fonte} já existe no GCP.")
        print("--------------------------------------------------------------------------")
    except Exception:
        print(f"Dataset {dataset_fonte} não foi encontrado no GCP, criando o dataset...")
        client.create_dataset(dataset_fonte)
        print(f"O conjunto de dados {dataset_fonte} foi criado no GCP com sucesso.")
        print("--------------------------------------------------------------------------")
    return dataset_fonte

def table_exist(client,dataset_fonte):
    ##########################################################################
    #                    Cria as tabelas caso não existam                    #
    ##########################################################################

    # Tabela e schema da tabela dim_produto
    table_produto = dataset_fonte.table("dim_produto")

    schema_produto = [
        bigquery.SchemaField("sk_produto", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("produto", "STRING"),
        bigquery.SchemaField("categoria", "STRING"),
        bigquery.SchemaField("subcategoria", "STRING"),
        bigquery.SchemaField("departamento", "STRING")
    ]

    # Tabela e schema da tabela dim_promocao
    table_promocao = dataset_fonte.table("dim_promocao")

    schema_promocao = [
        bigquery.SchemaField("sk_promocao", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("promocao", "STRING"),
        bigquery.SchemaField("tipop_reducao_preco", "STRING"),
        bigquery.SchemaField("veiculo_divulgacao", "STRING"),
        bigquery.SchemaField("tipo_display", "STRING")
    ]

    # Tabela e schema da tabela dim_localidade
    table_localidade = dataset_fonte.table("dim_localidade")

    schema_localidade = [
        bigquery.SchemaField("sk_localidade", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("loja", "STRING"),
        bigquery.SchemaField("cidade", "STRING"),
        bigquery.SchemaField("uf", "STRING"),
        bigquery.SchemaField("regiao", "STRING")
    ]

    # Tabela e schema da tabela dim_periodo
    table_periodo = dataset_fonte.table("dim_periodo")

    schema_periodo = [
        bigquery.SchemaField("sk_periodo", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("data", "DATE"),
        bigquery.SchemaField("dia", "INTEGER"),
        bigquery.SchemaField("mes", "INTEGER"),
        bigquery.SchemaField("ano", "INTEGER"),
        bigquery.SchemaField("dia_da_semana", "STRING"),
        bigquery.SchemaField("flag_feriado", "STRING")
    ]

    # Tabela e schema da tabela fato_vendas
    table_fato_vendas = dataset_fonte.table("fato_vendas")

    schema_fato_vendas = [
        bigquery.SchemaField("sk_produto", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("sk_promocao", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("sk_localidade", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("sk_periodo", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("vl_total_vendas", "FLOAT"),
        bigquery.SchemaField("qt_vendas", "INTEGER"),
        bigquery.SchemaField("vl_custo_total", "FLOAT"),
        bigquery.SchemaField("qt_cliente", "INTEGER")
    ]

    tabelas = {
        table_produto:schema_produto,
        table_promocao:schema_promocao,
        table_localidade:schema_localidade,
        table_periodo:schema_periodo,
        table_fato_vendas:schema_fato_vendas
    }

    print("--------------------------------------------------------------------------")
    print("Verificando a existência das tabelas no GCP...")
    for tabela, schema in tabelas.items():
        try:
            client.get_table(tabela, timeout=30)
            print(f"A tabela {tabela} já existe!")
            print("--------------------------------------------------------------------------")
        except:
            print(f"Tabela {tabela} não encontrada! Criando tabela {tabela}...")
            client.create_table(bigquery.Table(tabela, schema=schema))
            print(f"A tabela {tabela} foi criada.")
            print("--------------------------------------------------------------------------")

    return table_produto, table_promocao, table_localidade, table_periodo, table_fato_vendas

def load_data(tables_dfs,client,dataset_fonte):
    print("--------------------------------------------------------------------------")
    print("Carregando dados no GCP...")
    for tabela, df in tables_dfs.items():
        table_ref = client.dataset(dataset_fonte.dataset_id).table(tabela.table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"Dados carregados na tabela {tabela}.")
    
    print("--------------------------------------------------------------------------")
    print("##########################################################################")
    print("#                       Fim da execução do programa                      #")
    print("##########################################################################")

