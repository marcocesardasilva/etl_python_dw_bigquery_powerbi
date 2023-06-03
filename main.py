# importar de outro arquivo
from load import *
from transform import *

##########################################################################
#                            Definir variáveis                           #
##########################################################################
file_key = "keys\dw-caged-e843b4e16870.json"
dataset_name = "vendas"
data_folder = "dados"
file = "vendas.xls"

##########################################################################
#               Criar conexão com o GCP, dataset e tabelas               #
##########################################################################
# Conexão com GCP
client = gcp_connection(file_key)
# Verificar se o dataset já existe, se não existe, cria
dataset_fonte = dataset_exist(client,dataset_name)
# Verifica se as tabelas já existem, se não existe, cria
table_produto, table_promocao, table_localidade, table_periodo, table_fato_vendas = table_exist(client,dataset_fonte)

##########################################################################
#                         Executar transformações                        #
##########################################################################
# Ler todos os arquivos, tratar e agrupar em um único dataframe
df = group_files(data_folder,file)
# Criar dfs da fato e das dimensões
df_produto, df_promocao, df_localidade, df_periodo, fato_vendas = create_dfs(df)

##########################################################################
#                          Carregar dados no GCP                         #
##########################################################################
# Incluir tabelas e dfs em uma biblioteca
tables_dfs = {table_produto:df_produto,table_promocao:df_promocao,table_localidade:df_localidade,table_periodo:df_periodo,table_fato_vendas:fato_vendas} 
load_data(tables_dfs,client,dataset_fonte)

