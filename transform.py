# importar bibliotecas
import pandas as pd
import os


def group_files(data_folder,file):

    print("--------------------------------------------------------------------------")
    print("Carregando os dados do arquivo e transformando...")

    # lê o arquivo CSV com o pandas
    df = pd.read_excel(os.path.join(data_folder, file))

    # Converter o campo 'data' para o tipo datetime
    df['data'] = pd.to_datetime(df['data'])

    # Extrair o mês e o dia do campo 'data'
    df['mes'] = df['data'].dt.month
    df['dia'] = df['data'].dt.day

    # Atualizar o campo 'data' com o ano correto do campo 'ano'
    df['data'] = pd.to_datetime(df['ano'].astype(str) + '-' + df['mes'].astype(str) + '-' + df['dia'].astype(str))

    # Remover as colunas 'mês_ano' e 'units_old' se necessário
    df = df.drop(['mês_ano','units_old'], axis=1)

    # Renomear colunas
    df = df.rename(columns={
        'nome_loja': 'loja',
        'cidade_loja': 'cidade',
        'estado_loja': 'uf',
        'nome_produto': 'produto',
        'subcategoria_produto': 'subcategoria',
        'categoria_produto': 'categoria',
        'departamento_produto': 'departamento',
        'nome_promocao': 'promocao',
        'total_vendas': 'vl_total_vendas',
        'unidades_vendidas': 'qt_vendas',
        'total_custo': 'vl_total_custo',
        'quantidade_clientes': 'qt_clientes'
        })

    # Incluir sks
    df['sk_localidade'] = pd.factorize(df['loja'] + '_' + df['cidade'] + '_' + df['uf'] + '_' + df['regiao'])[0] + 1
    df['sk_produto'] = pd.factorize(df['produto'] + '_' + df['subcategoria'] + '_' + df['categoria'] + '_' + df['departamento'])[0] + 1
    df['sk_promocao'] = pd.factorize(df['promocao'] + '_' + df['tipo_reducao_preco'] + '_' + df['veiculo_divulgacao'] + '_' + df['tipo_display'])[0] + 1
    df['sk_periodo'] = pd.factorize(df['data'].dt.strftime('%Y-%m-%d') + '_' + df['flag_feriado'])[0] + 1

    print("Dataframe único criado com todos os dados.")
    print("--------------------------------------------------------------------------")

    return df

def create_dfs(df):
    print("--------------------------------------------------------------------------")
    print("Criando dataframes para geração de modelo dimensional star schema...")

    # Criar df_produto
    df_produto = df[['sk_produto','produto','subcategoria','categoria','departamento']].drop_duplicates().reset_index(drop=True)
    df_produto.name = 'df_produto'
    print(f"Dataframe {df_produto.name} criado.")

    # Criar df_promocao
    df_promocao = df[['sk_promocao','promocao','tipo_reducao_preco','veiculo_divulgacao','tipo_display']].drop_duplicates().reset_index(drop=True)
    df_promocao.name = 'df_promocao'
    print(f"Dataframe {df_promocao.name} criado.")

    # Criar df_localidade
    df_localidade = df[['sk_localidade','loja','cidade','uf','regiao']].drop_duplicates().reset_index(drop=True)
    df_localidade.name = 'df_localidade'
    print(f"Dataframe {df_localidade.name} criado.")

    # Criar df_periodo
    df_periodo = df[['sk_periodo','data','dia','mes','ano','dia_da_semana','flag_feriado']].drop_duplicates().reset_index(drop=True)
    df_periodo.name = 'df_periodo'
    print(f"Dataframe {df_periodo.name} criado.")

    # Criar fato_vendas
    fato_vendas = df[['sk_produto','sk_promocao','sk_localidade','sk_periodo','vl_total_vendas','qt_vendas','vl_total_custo','qt_clientes']]
    fato_vendas.name = 'fato_sisab'
    print(f"Dataframe {fato_vendas.name} criado.")
    print("--------------------------------------------------------------------------")

    return df_produto, df_promocao, df_localidade, df_periodo, fato_vendas

