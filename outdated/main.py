# ==============================================================================
# PARTE 1: CONFIGURAÇÃO INICIAL E O "MOTOR" DA SIMULAÇÃO
# ==============================================================================
import pandas as pd
import random
import numpy as np
from tqdm import tqdm # Biblioteca para mostrar uma barra de progresso (instale com: pip install tqdm)

# Configurações do Pandas
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 1000)
pd.options.display.float_format = "{:,.2f}".format

# Seed para reprodutibilidade
random.seed(27)
np.random.seed(27)

# Carregando os dados base
df_clubes_inicial = pd.read_parquet(r"data/clubes.parquet")
df_calendario_template = pd.read_parquet(r"data/calendario.parquet")
df_clubes_inicial.columns = df_clubes_inicial.columns.str.upper()

# Dicionário de Parâmetros por Série (nossa última calibragem)
PARAMETROS_SERIE = {
    'A': {'escala_gols': 13.0, 'fator_casa': 1.20},
    'B': {'escala_gols': 22.0, 'fator_casa': 1.25},
    'C': {'escala_gols': 28.0, 'fator_casa': 1.30}
}

# A função de simulação de jogo final e completa
def simular_jogo(mandante_nome, visitante_nome, serie_jogo, estado_clubes, fator_equilibrio_classico=0.60, forca_minima=0.02):
    parametros = PARAMETROS_SERIE[serie_jogo]
    escala_gols, fator_casa = parametros['escala_gols'], parametros['fator_casa']
    mandante_dados, visitante_dados = estado_clubes[mandante_nome], estado_clubes[visitante_nome]
    forca_base_mandante_ajustada = mandante_dados['forca_base'] + forca_minima
    forca_base_visitante_ajustada = visitante_dados['forca_base'] + forca_minima
    forca_ataque_mandante = (forca_base_mandante_ajustada * escala_gols) * mandante_dados['momento'] * fator_casa
    forca_ataque_visitante = (forca_base_visitante_ajustada * escala_gols) * visitante_dados['momento']
    if mandante_dados['UF'] == visitante_dados['UF']:
        forca_media = (forca_ataque_mandante + forca_ataque_visitante) / 2
        forca_ataque_mandante = (forca_ataque_mandante * fator_equilibrio_classico) + (forca_media * (1 - fator_equilibrio_classico))
        forca_ataque_visitante = (forca_ataque_visitante * fator_equilibrio_classico) + (forca_media * (1 - fator_equilibrio_classico))
    gols_mandante = np.random.poisson(forca_ataque_mandante)
    gols_visitante = np.random.poisson(forca_ataque_visitante)
    if gols_mandante > gols_visitante: resultado = 'V'
    elif gols_visitante > gols_mandante: resultado = 'D'
    else: resultado = 'E'
    return {'resultado': resultado, 'gols_mandante': gols_mandante, 'gols_visitante': gols_visitante}

# ==============================================================================
# PARTE 2: PREPARAÇÃO PARA A GRANDE SIMULAÇÃO
# ==============================================================================
print("Iniciando a preparação para a simulação de 40 anos...")

# Lista principal para guardar todos os DataFrames de cada temporada
historico_completo_jogos = []

# Dicionário mestre que guardará a FORÇA BASE em evolução dos clubes
estado_mestre_clubes = df_clubes_inicial.rename(columns={'FORCA': 'forca_base'}).set_index('CLUBE').to_dict('index')

# Listas iniciais de clubes por divisão (para a Temporada 1)
clubes_serie_a = list(df_clubes_inicial['CLUBE'].iloc[0:20])
clubes_serie_b = list(df_clubes_inicial['CLUBE'].iloc[20:40])
clubes_serie_c = list(df_clubes_inicial['CLUBE'].iloc[40:60])

# ==============================================================================
# PARTE 3: O GRANDE LOOP DE 40 ANOS
# ==============================================================================
for ano in tqdm(range(1, 81), desc="Simulando Temporadas"):
    
    random.shuffle(clubes_serie_a)
    random.shuffle(clubes_serie_b)
    random.shuffle(clubes_serie_c)
    
    mapa_clubes = {}
    
    for i in range(20):
        mapa_clubes[f"clube {i+1:02d}"] = clubes_serie_a[i]
        
    for i in range(20):
        mapa_clubes[f"clube {i+21:02d}"] = clubes_serie_b[i]
        
    for i in range(20):
        mapa_clubes[f"clube {i+41:02d}"] = clubes_serie_c[i]
    
    df_temporada = df_calendario_template.copy()
    df_temporada['mandante_real'] = df_temporada['mandante'].map(mapa_clubes)
    df_temporada['visitante_real'] = df_temporada['visitante'].map(mapa_clubes)
    df_temporada['ano'] = ano

    estado_clubes_ano = {clube: dados.copy() for clube, dados in estado_mestre_clubes.items()}
    for clube in estado_clubes_ano:
        estado_clubes_ano[clube]['pontos'] = 0; estado_clubes_ano[clube]['vitorias'] = 0; estado_clubes_ano[clube]['empates'] = 0; estado_clubes_ano[clube]['derrotas'] = 0; estado_clubes_ano[clube]['gols_pro'] = 0; estado_clubes_ano[clube]['gols_contra'] = 0; estado_clubes_ano[clube]['momento'] = 1.0

    # b. Simulação dos Jogos da Temporada
    resultados_ano = []
    for index, jogo in df_temporada.iterrows():
        mandante_nome, visitante_nome, serie_do_jogo = jogo['mandante_real'], jogo['visitante_real'], jogo['serie']
        resultado_partida = simular_jogo(mandante_nome, visitante_nome, serie_do_jogo, estado_clubes_ano)
        resultados_ano.append(resultado_partida)
        
        # Atualização de estado e momento (lógica completa)
        estado_clubes_ano[mandante_nome]['gols_pro'] += resultado_partida['gols_mandante']; estado_clubes_ano[mandante_nome]['gols_contra'] += resultado_partida['gols_visitante']; estado_clubes_ano[visitante_nome]['gols_pro'] += resultado_partida['gols_visitante']; estado_clubes_ano[visitante_nome]['gols_contra'] += resultado_partida['gols_mandante']
        if resultado_partida['resultado'] == 'V':
            estado_clubes_ano[mandante_nome]['pontos'] += 3; estado_clubes_ano[mandante_nome]['vitorias'] += 1; estado_clubes_ano[visitante_nome]['derrotas'] += 1
            estado_clubes_ano[mandante_nome]['momento'] *= 1.02; estado_clubes_ano[visitante_nome]['momento'] *= 0.98
        elif resultado_partida['resultado'] == 'D':
            estado_clubes_ano[mandante_nome]['derrotas'] += 1; estado_clubes_ano[visitante_nome]['pontos'] += 3; estado_clubes_ano[visitante_nome]['vitorias'] += 1
            estado_clubes_ano[mandante_nome]['momento'] *= 0.95; estado_clubes_ano[visitante_nome]['momento'] *= 1.07
        else:
            estado_clubes_ano[mandante_nome]['pontos'] += 1; estado_clubes_ano[visitante_nome]['pontos'] += 1; estado_clubes_ano[mandante_nome]['empates'] += 1; estado_clubes_ano[visitante_nome]['empates'] += 1
            estado_clubes_ano[mandante_nome]['momento'] *= 0.97; estado_clubes_ano[visitante_nome]['momento'] *= 1.03
        estado_clubes_ano[mandante_nome]['momento'] = max(0.85, min(estado_clubes_ano[mandante_nome]['momento'], 1.25)); estado_clubes_ano[visitante_nome]['momento'] = max(0.85, min(estado_clubes_ano[visitante_nome]['momento'], 1.25))

    # c. Armazenamento dos Resultados
    df_resultados_ano = pd.DataFrame(resultados_ano)
    df_temporada_final = pd.concat([df_temporada.reset_index(drop=True), df_resultados_ano.reset_index(drop=True)], axis=1)
    historico_completo_jogos.append(df_temporada_final)

    # d. Pós-Temporada (Promoção/Rebaixamento e Entressafra)
    if ano < 80: # Não executa no último ano
        df_classificacao = pd.DataFrame.from_dict(estado_clubes_ano, orient='index')
        df_classificacao['serie'] = pd.Series({clube: 'A' for clube in clubes_serie_a} | {clube: 'B' for clube in clubes_serie_b} | {clube: 'C' for clube in clubes_serie_c})
        df_classificacao_ordenada = df_classificacao.sort_values(by=['serie', 'pontos'], ascending=[True, False])
        
        # Entressafra: Atualiza a forca_base no dicionário MESTRE
        forca_media_geral = df_classificacao['forca_base'].mean()
        for clube, dados in df_classificacao_ordenada.iterrows():
            posicao_geral = list(df_classificacao_ordenada.index).index(clube)
            # Lógica simples de evolução: Melhora se terminar no G4 de qualquer divisão, piora se ficar no Z4
            if posicao_geral in [0,1,2,3, 20,21,22,23, 40,41,42,43]: # G4s
                estado_mestre_clubes[clube]['forca_base'] *= 1.02
            elif posicao_geral in [16,17,18,19, 36,37,38,39, 56,57,58,59]: # Z4s
                estado_mestre_clubes[clube]['forca_base'] *= 0.98
            # Regressão à média
            estado_mestre_clubes[clube]['forca_base'] = estado_mestre_clubes[clube]['forca_base'] * 0.97 + forca_media_geral * 0.03

        # Promoção e Rebaixamento
        rebaixados_a = df_classificacao_ordenada.query("serie == 'A'").tail(4).index.tolist(); promovidos_b = df_classificacao_ordenada.query("serie == 'B'").head(4).index.tolist()
        rebaixados_b = df_classificacao_ordenada.query("serie == 'B'").tail(4).index.tolist(); promovidos_c = df_classificacao_ordenada.query("serie == 'C'").head(4).index.tolist()
        clubes_serie_a = [c for c in clubes_serie_a if c not in rebaixados_a] + promovidos_b
        clubes_serie_b = [c for c in clubes_serie_b if c not in promovidos_b and c not in rebaixados_b] + rebaixados_a + promovidos_c
        clubes_serie_c = [c for c in clubes_serie_c if c not in promovidos_c] + rebaixados_b

# ==============================================================================
# PARTE 4: CONSOLIDAÇÃO FINAL
# ==============================================================================
df_final = pd.concat(historico_completo_jogos, ignore_index=True)

print("\n--- SIMULAÇÃO DE 40 ANOS CONCLUÍDA ---")
print("Dimensões do DataFrame Final:", df_final.shape)
print("\nPrimeiras linhas do histórico:")
print(df_final.head())
print("\nÚltimas linhas do histórico:")
print(df_final.tail())

# Dica: Salve seu resultado para não precisar rodar tudo de novo!
print("\nSalvando o DataFrame final em 'simulacao_40_anos.parquet'...")
df_final.to_parquet(r'data/simulacao_40_anos.parquet')
df_final.to_excel(r'data/simulacao_40_anos.xlsx')
print("Salvo com sucesso!")