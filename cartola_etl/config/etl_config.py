# Dados, constantes e configurações do Cartola.
# Base 2023.

# Dados atualizados a cada rodada
dynamic_data_endpoints = {
    "mercado_status": "https://api.cartolafc.globo.com/mercado/status",
    "atletas_mercado": "https://api.cartolafc.globo.com/atletas/mercado",
    "mercado_destaques": "https://api.cartolafc.globo.com/mercado/destaques",
    "pos_rodada_destaques": "https://api.cartolafc.globo.com/pos-rodada/destaques",
    "partidas": "https://api.cartolafc.globo.com/partidas",
}

# Dados fixos
fixed_data_endpoints = {
    "clubes": "https://api.cartolafc.globo.com/clubes",
    "rodadas": "https://api.cartolafc.globo.com/rodadas",
}

# Scouts de Defesa e Scouts de Ataque
scouts_data = {
    "desarmes": {"abreviacao": "DS", "valor": 1.2},
    "falta_cometida": {"abreviacao": "FC", "valor": -0.3},
    "gol_contra": {"abreviacao": "GC", "valor": -3.0},
    "cartao_amarelo": {"abreviacao": "CA", "valor": -1.0},
    "cartao_vermelho": {"abreviacao": "CV", "valor": -3.0},
    "jogo_sem_sofrer_gol": {"abreviacao": "SG", "valor": 5.0},
    "defesa_dificil": {"abreviacao": "DE", "valor": 1.0},
    "defesa_penalti": {"abreviacao": "DP", "valor": 7.0},
    "gol_sofrido": {"abreviacao": "GS", "valor": -1.0},
    "penalti_cometido": {"abreviacao": "PC", "valor": -1.0},
    "falta_sofrida": {"abreviacao": "FS", "valor": 0.5},
    "assistencia": {"abreviacao": "A", "valor": 5.0},
    "finalizacao_trave": {"abreviacao": "FT", "valor": 3.0},
    "finalizacao_defendida": {"abreviacao": "FD", "valor": 1.2},
    "finalizacao_fora": {"abreviacao": "FF", "valor": 0.8},
    "gols": {"abreviacao": "G", "valor": 8.0},
    "impedimento": {"abreviacao": "I", "valor": -0.1},
    "penalti_perdido": {"abreviacao": "PP", "valor": -4.0},
    "penalti_sofrido": {"abreviacao": "PS", "valor": 1.0},
    "vitoria_tecnico": {"abreviacao": "V", "valor": 1.0},
}

# Métricas de interesse 
fact_table_selected_metrics = {
    "preco": "preco_num",
    "media": "media_num",
    "pontuacao_rodada": "pontos_num",
}
