# Dados, constantes e configurações do Cartola.
# Base 2023.

# Scouts de Ataque e Scouts de Defesa
scouts_data = {
    "desarmes": {"abreviacao": "DS", "pontuacao": 1.2},
    "falta_cometida": {"abreviacao": "FC", "pontuacao": -0.3},
    "gol_contra": {"abreviacao": "GC", "pontuacao": -3.0},
    "cartao_amarelo": {"abreviacao": "CA", "pontuacao": -1.0},
    "cartao_vermelho": {"abreviacao": "CV", "pontuacao": -3.0},
    "jogo_sem_sofrer_gol": {"abreviacao": "SG", "pontuacao": 5.0},
    "defesa_dificil": {"abreviacao": "DE", "pontuacao": 1.0},
    "defesa_de_penalti": {"abreviacao": "DP", "pontuacao": 7.0},
    "gol_sofrido": {"abreviacao": "GS", "pontuacao": -1.0},
    "penalti_cometido": {"abreviacao": "PC", "pontuacao": -1.0},
    "falta_sofrida": {"abreviacao": "FS", "pontuacao": 0.5},
    "passe_incompleto": {"abreviacao": "PE", "pontuacao": -0.1},
    "assistencia": {"abreviacao": "A", "pontuacao": 5.0},
    "finalizacao_na_trave": {"abreviacao": "FT", "pontuacao": 3.0},
    "finalizacao_defendida": {"abreviacao": "FD", "pontuacao": 1.2},
    "finalizacao_para_fora": {"abreviacao": "FF", "pontuacao": 0.8},
    "gols": {"abreviacao": "G", "pontuacao": 8.0},
    "impedimento": {"abreviacao": "I", "pontuacao": -0.1},
    "penalti_perdido": {"abreviacao": "PP", "pontuacao": -4.0},
    "penalti_sofrido": {"abreviacao": "PS", "pontuacao": 1.0},
}

# Dados atualizados a cada rodada
updated_data_endpoints = {
    "status": "https://api.cartolafc.globo.com/mercado/status",
    "atletas_mercado": "https://api.cartolafc.globo.com/atletas/mercado",
    "mercado_destaques": "https://api.cartolafc.globo.com/mercado/destaques",
    "atletas_pontuados": "https://api.cartolafc.globo.com/atletas/pontuados",
    "pontos_destaques": "https://api.cartolafc.globo.com/pos-rodada/destaques",
    "partidas": "https://api.cartolafc.globo.com/partidas",
}

# Dados fixos
static_data_endpoints = {
    "clubes": "https://api.cartolafc.globo.com/clubes",
    "rodadas": "https://api.cartolafc.globo.com/rodadas",
}
