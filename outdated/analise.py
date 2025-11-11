# Script para análise completa e multissérie do histórico de 80 temporadas

import pandas as pd

# Configurações do Pandas
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 120) # Ajustado para caber mais colunas
pd.options.display.float_format = "{:,.2f}".format

print("--- Iniciando Análise Completa do Histórico ---")

# --- PASSO 1: CARREGAR E PREPARAR DADOS PARA TODAS AS SÉRIES ---
try:
    # Tente carregar o parquet primeiro, se não, o csv
    try:
        df_simulacao = pd.read_parquet('data/simulacao_40_anos.parquet')
    except (FileNotFoundError, ImportError):
        df_simulacao = pd.read_csv('simulacao_40_anos.csv')
    print("Arquivo de simulação carregado com sucesso.")
except FileNotFoundError:
    print("ERRO: Nenhum arquivo de simulação ('simulacao_40_anos.parquet' ou '.csv') foi encontrado.")
    exit()

# A lógica de transformação agora é aplicada ao DataFrame completo, sem filtrar a série
df_mandante = df_simulacao[['ano', 'serie', 'mandante_real', 'gols_mandante', 'gols_visitante', 'resultado']].rename(columns={'mandante_real': 'time', 'gols_mandante': 'gols_pro', 'gols_visitante': 'gols_contra'})
df_visitante = df_simulacao[['ano', 'serie', 'visitante_real', 'gols_visitante', 'gols_mandante', 'resultado']].rename(columns={'visitante_real': 'time', 'gols_visitante': 'gols_pro', 'gols_mandante': 'gols_contra'})

df_mandante['pontos'] = df_mandante['resultado'].map({'V': 3, 'E': 1, 'D': 0}); df_mandante['vitorias'] = df_mandante['resultado'].map({'V': 1, 'E': 0, 'D': 0}); df_mandante['empates'] = df_mandante['resultado'].map({'V': 0, 'E': 1, 'D': 0}); df_mandante['derrotas'] = df_mandante['resultado'].map({'V': 0, 'E': 0, 'D': 1})
df_visitante['pontos'] = df_visitante['resultado'].map({'D': 3, 'E': 1, 'V': 0}); df_visitante['vitorias'] = df_visitante['resultado'].map({'D': 1, 'E': 0, 'V': 0}); df_visitante['empates'] = df_visitante['resultado'].map({'E': 1, 'V': 0, 'D': 0}); df_visitante['derrotas'] = df_visitante['resultado'].map({'V': 1, 'E': 0, 'D': 0})

df_full_log = pd.concat([df_mandante, df_visitante])

# Agrupamos por ano, SÉRIE e time
tabela_geral = df_full_log.groupby(['ano', 'serie', 'time']).agg(pontos=('pontos', 'sum'), vitorias=('vitorias', 'sum'), empates=('empates', 'sum'), derrotas=('derrotas', 'sum'), gols_pro=('gols_pro', 'sum'), gols_contra=('gols_contra', 'sum')).reset_index()
tabela_geral['saldo_gols'] = tabela_geral['gols_pro'] - tabela_geral['gols_contra']
tabela_geral_ordenada = tabela_geral.sort_values(by=['ano', 'serie', 'pontos', 'vitorias', 'saldo_gols'], ascending=[True, True, False, False, False])

# --- PASSO 2: ANÁLISE COMPLETA DA SÉRIE A (G4 e Z4) ---
resumo_serie_a = []
num_anos = df_simulacao['ano'].max()
for ano in range(1, num_anos + 1):
    tabela_ano_a = tabela_geral_ordenada[(tabela_geral_ordenada['ano'] == ano) & (tabela_geral_ordenada['serie'] == 'A')].reset_index(drop=True)
    if len(tabela_ano_a) < 20: continue # Pula anos que possam ter dados incompletos
    
    resumo_serie_a.append({
        'Ano': ano,
        'Campeão': tabela_ano_a.iloc[0]['time'],
        'Vice': tabela_ano_a.iloc[1]['time'],
        '3º Lugar': tabela_ano_a.iloc[2]['time'],
        '4º Lugar': tabela_ano_a.iloc[3]['time'],
        '17º (Rebaixado)': tabela_ano_a.iloc[16]['time'],
        '18º (Rebaixado)': tabela_ano_a.iloc[17]['time'],
        '19º (Rebaixado)': tabela_ano_a.iloc[18]['time'],
        '20º (Rebaixado)': tabela_ano_a.iloc[19]['time'],
    })
df_resumo_a = pd.DataFrame(resumo_serie_a)
print("\n\n--- Resumo Histórico da Série A (G4 e Z4) ---")
print(df_resumo_a.to_string(index=False))
df_resumo_a.to_csv('resumo_serie_a_completo.csv', index=False)
print("\nResumo da Série A salvo em 'resumo_serie_a_completo.csv'")

# --- PASSO 3: ANÁLISE DE CAMPEÕES E PROMOVIDOS DA SÉRIE B ---
resumo_serie_b = []
for ano in range(1, num_anos + 1):
    tabela_ano_b = tabela_geral_ordenada[(tabela_geral_ordenada['ano'] == ano) & (tabela_geral_ordenada['serie'] == 'B')].reset_index(drop=True)
    if len(tabela_ano_b) < 20: continue
        
    resumo_serie_b.append({
        'Ano': ano,
        'Campeão (Promovido)': tabela_ano_b.iloc[0]['time'],
        '2º (Promovido)': tabela_ano_b.iloc[1]['time'],
        '3º (Promovido)': tabela_ano_b.iloc[2]['time'],
        '4º (Promovido)': tabela_ano_b.iloc[3]['time'],
    })
df_resumo_b = pd.DataFrame(resumo_serie_b)
print("\n\n--- Resumo Histórico da Série B (Promovidos) ---")
print(df_resumo_b.to_string(index=False))
df_resumo_b.to_csv('resumo_serie_b.csv', index=False)
print("\nResumo da Série B salvo em 'resumo_serie_b.csv'")

# --- PASSO 4: ANÁLISE DE CAMPEÕES E PROMOVIDOS DA SÉRIE C ---
resumo_serie_c = []
for ano in range(1, num_anos + 1):
    tabela_ano_c = tabela_geral_ordenada[(tabela_geral_ordenada['ano'] == ano) & (tabela_geral_ordenada['serie'] == 'C')].reset_index(drop=True)
    if len(tabela_ano_c) < 20: continue
        
    resumo_serie_c.append({
        'Ano': ano,
        'Campeão (Promovido)': tabela_ano_c.iloc[0]['time'],
        '2º (Promovido)': tabela_ano_c.iloc[1]['time'],
        '3º (Promovido)': tabela_ano_c.iloc[2]['time'],
        '4º (Promovido)': tabela_ano_c.iloc[3]['time'],
    })
df_resumo_c = pd.DataFrame(resumo_serie_c)
print("\n\n--- Resumo Histórico da Série C (Promovidos) ---")
print(df_resumo_c.to_string(index=False))
df_resumo_c.to_csv('resumo_serie_c.csv', index=False)
print("\nResumo da Série C salvo em 'resumo_serie_c.csv'")


# --- PASSO 5: ANÁLISES GERAIS (PONTOS E TÍTULOS DA SÉRIE A) ---
tabela_serie_a = tabela_geral_ordenada[tabela_geral_ordenada['serie'] == 'A']

print("\n\n--- Total de Pontos Acumulados na Série A (Top 20) ---")
total_pontos_por_clube = tabela_serie_a.groupby('time')['pontos'].sum().sort_values(ascending=False)
print(total_pontos_por_clube.head(20).to_string())

print("\n\n--- Ranking de Maiores Campeões da Série A ---")
maiores_campeoes = df_resumo_a['Campeão'].value_counts()
print(maiores_campeoes.to_string())

print("\n\nAnálise completa concluída!")