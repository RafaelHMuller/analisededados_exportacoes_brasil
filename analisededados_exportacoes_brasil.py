#!/usr/bin/env python
# coding: utf-8

# # Mentoria 26/01/2023 - Projeto Análise de Dados
# 
# Base de dados: todas as exportações feitas pelo Brasil desde 2010 <br>
# Kaggle: https://www.kaggle.com/code/hugovallejo/exportations-brazil-to-afghanistan-pyspark/data
# 
# #### Objetivo:
# Analisar todas as exportações realizadas pelo Brasil a partir de 2018 e responder às perguntas:
# - 1. Como foi a evolução das exportações para cada país ao longo dos anos?
# Obs.: Dar um exemplo
# - 2. Quais os países que o Brasil mais exportou em cada ano?
# - 3. Quais os produtos mais exportados ao longo de todo o período?
# - 4. Em 2020 qual cidade mais exportou? 
# - 5. Maringá está em qual posição do ranking no período? Quais as principais exportações de Maringá no período?

# In[1]:


import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px


# In[ ]:


# importar base de dados
df = pd.read_csv(r'D:\Arquivos Aulas Python\Mentoria Análise de Dados\exportacao_full-001.csv')
#display(df)


# In[ ]:


# selecionar apenas as exportações a partir de 2018
df = df.loc[df['Year']>2017, :]
pd.options.display.max_colwidth = 500 # mostrar 500 caracteres das strings
display(df)
df.info()


# In[ ]:


# tratamento dos dados: remover linhas duplicadas
print(df.shape)
df = df.drop_duplicates(subset=['Year', 'Month', 'Country', 'City', 'SH4 Code', 'SH4 Description', 'SH2 Code', 'SH2 Description', 'US$ FOB', 'Net Weight'],
                       keep = 'last')
print(df.shape)


# ##### Análise de Dados:

# Como foi a evolução das exportações para cada país ao longo dos anos? Obs.: Dar um exemplo

# In[ ]:


dicionario = {}
for pais in df['Country'].unique():
    df_pais = df.loc[df['Country']==pais, :]
    dicionario[pais] = df_pais

print(len(dicionario))


# In[ ]:


# criar um df vazio
df_evolucao_exportacoes = pd.DataFrame()

# modificar os dfs dos países
dicionario_novo = {}
for pais in dicionario:
    df_pais = dicionario[pais][['Year', 'US$ FOB']].groupby('Year').sum()
    dicionario_novo[pais] = df_pais

# juntar os dfs dos países ao df vazio
for pais in dicionario_novo:
    df_evolucao_exportacoes = pd.concat([df_evolucao_exportacoes, dicionario_novo[pais]], axis=1)
    df_evolucao_exportacoes = df_evolucao_exportacoes.rename(columns={'US$ FOB':pais})
    #df_evolucao_exportacoes = df_evolucao_exportacoes.T

display(df_evolucao_exportacoes)
df_evolucao_exportacoes.info()


# In[ ]:


pais_escolhido = input(str('Digite um país:'))
pais_escolhido = pais_escolhido.title()

# criar um gráfico comparativos de um país (MATPLOTLIB)
plt.figure(figsize=(10,5))
plt.title(f'Evolução das exportações brasileiras para {pais_escolhido}')
ax = sns.barplot(data=df_evolucao_exportacoes, x=df_evolucao_exportacoes.index, y=f'{pais_escolhido}')
ax.bar_label(ax.containers[0])

# criar um gráfico comparativos de um país (PLOTLY)
fig = px.bar(df_evolucao_exportacoes, x=df_evolucao_exportacoes.index, y=f'{pais_escolhido}', text_auto=True,
              title=f'Evolução das exportações brasileiras para {pais_escolhido}')
fig.show()


# Quais os países que o Brasil mais exportou em cada ano?

# In[ ]:


df_ranking_ano = df[['Year', 'Country', 'US$ FOB']].groupby(['Year', 'Country']).sum().reset_index().sort_values(by='US$ FOB', ascending=False)

def dataframe_ano(ano):
    # dataframe do ano
    df_ano = df_ranking_ano.loc[df_ranking_ano['Year']==ano, :].set_index('Year')
    df_ano['%'] = 0
    for linha in df_ano.index:
        df_ano.loc[linha, '%'] = df_ano.loc[linha, 'US$ FOB'] / df_ano['US$ FOB'].sum()
    df_ano['%'] = df_ano['%'].map('{:.2%}'.format)
    display(df_ano.head(15))
    
    # gráfico do ano
    plt.figure(figsize=(10,5))
    plt.title(f'Comparativo das principais exportações brasileiras em {ano}')
    grafico = sns.barplot(data=df_ano.head(20), x='Country', y='US$ FOB')
    grafico.tick_params(axis='x', rotation=90) 
    
    
dataframe_ano(2018)
dataframe_ano(2019)
dataframe_ano(2020)


# Quais os produtos mais exportados, por volume e peso, ao longo de todo o período?

# In[ ]:


peso = 'Net Weight'
valor = 'US$ FOB'

def produtos_mais_exportados(peso_ou_valor):
    df_produtos = df[['SH2 Description', peso_ou_valor]].groupby('SH2 Description').sum().reset_index()
    df_produtos['%'] = 0
    for linha in df_produtos.index:
        df_produtos.loc[linha, '%'] = df_produtos.loc[linha, peso_ou_valor] / df_produtos[peso_ou_valor].sum()
    df_produtos['%'] = df_produtos['%'].map('{:.2%}'.format)
    df_produtos = df_produtos.set_index('SH2 Description')
    return df_produtos

# produtos mais exportados por peso
df_produtos_peso = produtos_mais_exportados(peso)

# produtos mais exportados por valor
df_produtos_valor = produtos_mais_exportados(valor)

# juntar os dataframes
df_produtos_peso = pd.concat([df_produtos_peso, df_produtos_valor], axis=1)

# display dos 10 produtos mais exportados por peso
display(df_produtos_peso.sort_values(by='Net Weight', ascending = False))

# display dos 10 produtos mais exportados por valor
display(df_produtos_peso.sort_values(by='US$ FOB', ascending = False))


# Em 2020 qual cidade mais exportou?

# In[ ]:


df_cidade = df[['Year', 'City', 'US$ FOB']].groupby(['Year', 'City']).sum().reset_index()

def dataframe_cidade(ano):
    # dataframe do ano
    df_cidade_ano = df_cidade.loc[df_cidade['Year']==ano, :].set_index('Year')
    df_cidade_ano['%'] = 0
    for i, linha in enumerate(df_cidade_ano.index):
        df_cidade_ano.loc[linha, '%'] = df_cidade_ano.loc[linha, 'US$ FOB'] / df_cidade_ano['US$ FOB'].sum()
    df_cidade_ano['%'] = df_cidade_ano['%'].map('{:.2%}'.format)
    df_cidade_ano = df_cidade_ano.sort_values(by='US$ FOB', ascending=False).reset_index()
    return df_cidade_ano
        
    
df_cidade_2018 = dataframe_cidade(2018)
df_cidade_2019 = dataframe_cidade(2019)
df_cidade_2020 = dataframe_cidade(2020)

display(df_cidade_2018.head(20), df_cidade_2019.head(20), df_cidade_2020.head(20))


# Quais as principais exportações de Maringá no período?

# In[ ]:


def dataframe_cidade(cidade):
    df_cidade = df.loc[df['City']==cidade, :]
    df_cidade = df_cidade[['SH2 Description', 'US$ FOB']].groupby('SH2 Description').sum().reset_index().sort_values(by='US$ FOB', ascending=False)
    df_cidade['%'] = 0
    for linha in df_cidade.index:
        df_cidade.loc[linha, '%'] = df_cidade.loc[linha, 'US$ FOB'] / df_cidade['US$ FOB'].sum()
    df_cidade['%'] = df_cidade['%'].map('{:.2%}'.format)
    display(df_cidade.head(20))

    # gráfico
    plt.figure(figsize=(10,5))
    plt.title(f'Comparativo das principais exportações de {cidade} no período')
    grafico = sns.barplot(data=df_cidade.head(20), x='SH2 Description', y='US$ FOB')
    grafico.tick_params(axis='x', rotation=90) 
    
dataframe_cidade('Curitiba - PR')


# Maringá está em qual posição do ranking no período?

# In[ ]:


maringa_2018 = df_cidade_2018.loc[df_cidade_2018['City']=='Maringá - PR', :]
maringa_2019 = df_cidade_2019.loc[df_cidade_2019['City']=='Maringá - PR', :]
maringa_2020 = df_cidade_2020.loc[df_cidade_2020['City']=='Maringá - PR', :]

display(maringa_2018, maringa_2019, maringa_2020)


# In[ ]:




