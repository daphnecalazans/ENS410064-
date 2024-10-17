# -*- coding: utf-8 -*-
"""
Created on Tue Oct 15 10:07:22 2024

@author: User
"""

#------------------------------------------------------------------------------
# BIBLIOTECAS
#------------------------------------------------------------------------------
import pandas as pd
import calendar
from datetime import datetime
import pyodbc


#------------------------------------------------------------------------------
# FUNÇÕES
#------------------------------------------------------------------------------
#...parameters
lista_dados = ['Data','Cota01', 'Cota02', 'Cota03',
       'Cota04', 'Cota05', 'Cota06', 'Cota07', 'Cota08', 'Cota09', 'Cota10',
       'Cota11', 'Cota12', 'Cota13', 'Cota14', 'Cota15', 'Cota16', 'Cota17',
       'Cota18', 'Cota19', 'Cota20', 'Cota21', 'Cota22', 'Cota23', 'Cota24',
       'Cota25', 'Cota26', 'Cota27', 'Cota28', 'Cota29', 'Cota30', 'Cota31',
       'EstacaoCodigo','NivelConsistencia']

lista_dados_vazoes = ['Data','Vazao01', 'Vazao02',
       'Vazao03', 'Vazao04', 'Vazao05', 'Vazao06', 'Vazao07', 'Vazao08',
       'Vazao09', 'Vazao10', 'Vazao11', 'Vazao12', 'Vazao13', 'Vazao14',
       'Vazao15', 'Vazao16', 'Vazao17', 'Vazao18', 'Vazao19', 'Vazao20',
       'Vazao21', 'Vazao22', 'Vazao23', 'Vazao24', 'Vazao25', 'Vazao26',
       'Vazao27', 'Vazao28', 'Vazao29', 'Vazao30', 'Vazao31', 'Vazao01Status',
       'EstacaoCodigo','NivelConsistencia']

#...converte formato do arquivo de cotas
def convert_table_cota(df_table):
    print('...convertendo formato')
    df_table['Data'] = pd.to_datetime(df_table['Data'], format='%d/%m/%Y')    
    df_table = df_table[lista_dados]

    list_cota = []
    list_data = []
    list_estacao = []
    list_consistencia = []
    
    for n in range(0,len(df_table)):

        year_info=df_table['Data'][n].year
        month_info=df_table['Data'][n].month   

        for day in range(0,calendar.monthrange(year_info, month_info)[1]):
                number = day +1
                if number <10:
                    name_col = 'Cota0'+str(number)
                else:
                    name_col = 'Cota'+str(number)
                    
                cota = df_table.iloc[[n]][name_col]
                estacao = df_table.iloc[[n]]['EstacaoCodigo']
                consis = df_table.iloc[[n]]['NivelConsistencia']
                #vazao = df_table_vazao.iloc[[n]][name_col_vazao]
                data=datetime(year_info, month_info, day+1)
                list_cota.append(cota.values[0])
                list_estacao.append(estacao.values[0])
                list_consistencia.append(consis.values[0])
                #list_vazao.append(vazao.values[0])
                list_data.append(data)

    Tabela_Final=pd.DataFrame(list_data, columns=['Data'])
    Tabela_Final['Cotas']= list_cota
    Tabela_Final['EstacaoCodigo'] = list_estacao
    Tabela_Final['NivelConsistencia'] = list_consistencia
    #Tabela_Final['Vazao'] = list_vazao
    Tabela_reorder=Tabela_Final.sort_values(by=['Data']).reset_index()    
    return Tabela_reorder

#...converte formato do arquivo de vazão
def convert_table_vazao(df_table_vazao):
    print('...convertendo formato')
    df_table_vazao['Data'] = pd.to_datetime(df_table_vazao['Data'], format='%d/%m/%Y')    
    df_table_vazao = df_table_vazao[lista_dados_vazoes]
    
    list_vazao = []
    list_data = []
    list_estacao = []
    list_consistencia = []
    
    for n in range(0,len(df_table_vazao)):

        year_info=df_table_vazao['Data'][n].year
        month_info=df_table_vazao['Data'][n].month   

        for day in range(0,calendar.monthrange(year_info, month_info)[1]):
                number = day +1
                if number <10:
                    name_col_vazao = 'Vazao0'+str(number)
                else:
                    name_col_vazao = 'Vazao'+str(number)
                    
                estacao = df_table_vazao.iloc[[n]]['EstacaoCodigo']
                vazao = df_table_vazao.iloc[[n]][name_col_vazao]
                consis = df_table_vazao.iloc[[n]]['NivelConsistencia']
                data=datetime(year_info, month_info, day+1)
                
                list_estacao.append(estacao.values[0])
                list_vazao.append(vazao.values[0])
                list_consistencia.append(consis.values[0])
                list_data.append(data)

    Tabela_Final=pd.DataFrame(list_data, columns=['Data'])
    Tabela_Final['EstacaoCodigo'] = list_estacao
    Tabela_Final['Vazao'] = list_vazao
    Tabela_Final['NivelConsistencia'] = list_consistencia
    Tabela_reorder=Tabela_Final.sort_values(by=['Data']).reset_index() 
    
    return Tabela_reorder

# ...pós-processamento das séries de vazão para priorização dos dados consistidos 
def nc(df):
    print('...filtrando consistência')
    # result df
    result = pd.DataFrame()
    gauges = df['EstacaoCodigo'].unique()
    for g in gauges:
        #...seleciona dados da estação de interesse separando em dados consistidos e não consistidos
        nc1 = df[(df['EstacaoCodigo'] == g) & (df['NivelConsistencia'] == 1)].sort_values(by = 'Data')
        nc2 = df[(df['EstacaoCodigo'] == g) & (df['NivelConsistencia'] == 2)].sort_values(by = 'Data')
        #...indexa os dados a data
        nc1.set_index('Data', inplace=True)
        nc1 = nc1.resample('D').mean()
        nc2.set_index('Data', inplace=True)
        nc2 = nc2.resample('D').mean()

        #...junta dados consistidos e não consistidos priorizando os dados consistidos
        if len(nc1) == 0:
            dates=[nc2.index.min(), nc2.index.max()]
        elif len(nc2) == 0:
            dates=[nc1.index.min(), nc1.index.max()]
        else:
            dates=[nc1.index.min(), nc2.index.min(), nc1.index.max(), nc2.index.max()]
        date_list = pd.date_range(min(dates), max(dates), freq='D')
        df_final = pd.DataFrame(index=date_list, columns=nc2.columns)
        df_final.update(nc2,overwrite=False)
        df_final.update(nc1,overwrite=False)
        df_final['Data'] = df_final.index
        df_final.index = list(range(0,len(df_final)))
        result = pd.concat([result, df_final])
        
    return result


#------------------------------------------------------------------------------
# EXEMPLO
#------------------------------------------------------------------------------

#...quais estações serão extraidas da base de dados? (nesse exemplo a base contem apenas uma estação)
gauges = [75900000]

#...caminho para o arquivo de saída (.xlsx)
f = 'C:/Users/User/Downloads/Estacao_75900000_MDB/output.xlsx'

#------------------------------------------------------------------------------
# Conexão com a base de dados

db_driver = '{Microsoft Access Driver (*.mdb, *.accdb)}'

db_path = 'C:/Users/User/Downloads/Estacao_75900000_MDB/75900000.mdb'

conn_str = (rf'DRIVER={db_driver};'
            rf'DBQ={db_path};')

conn = pyodbc.connect(conn_str)

#------------------------------------------------------------------------------
# Leitura dos dados brutos:
# ...Vazoes
q = pd.read_sql(sql="select * from Vazoes", con=conn)
# ...Cotas
y = pd.read_sql(sql="select * from Cotas", con=conn)

conn.close()
#------------------------------------------------------------------------------
# Checa disponibilidade e exporta dados de vazão para cada posto:
unique_gauges = q['EstacaoCodigo'].unique()
unique_gauges.sort()

q_selected_gauges = gauges.copy()

for g in q_selected_gauges:
    check = g in unique_gauges
    if check:
        print(str(g), 'ok')
    else:
        print(str(g), 'indisponível')

q_aux = pd.DataFrame(columns=q.columns)
for g in q_selected_gauges:
    q = q[q['EstacaoCodigo'] == g]
    q_aux = pd.concat([q_aux, q])

# Processa os dados para os postos com disponibilidade na base selecionada
#...redefine índices do dataframe
q_aux.index = list(range(0,len(q_aux)))

#...converte o formato dos dados de matriz ANA para série temporal
q_series = convert_table_vazao(q_aux) # !!! Aplica função para converter formato de dados da ANA
q_series.sort_values(by = 'Data', inplace=True)

#...seleciona prioritariamente os dados consistidos e preenche o restante com dados não consistidos
q_series = nc(q_series) # !!! Aplica função para priorizar dados consistidos
q_series.dropna(axis=0, subset=['Vazao'], inplace=True)

# Configura dados de saída de vazão
df_out = pd.DataFrame(index=pd.date_range(q_series['Data'].min(), '2020-12-31'))
for i in gauges:
    # Filtra dados para a estação
    q = q_series[q_series['EstacaoCodigo'] == i]
    q = q[['Data', 'Vazao']]
    q.set_index('Data', inplace=True)
    df_out[i] = q

df_out.to_excel(f)
df_out.plot()

# OBS: para converter dados de cota é a mesma coisa, apenas substitua a "q" por "y" e a função
# "convert_table_vazao" por "convert_table_cota"

