CREATE TABLE
    IF NOT EXISTS dim_rodadas (
        rodada_id INT NOT NULL,
        inicio DATETIME NULL,
        fim DATETIME NULL,
        times_escalados INT NULL,
        media_cartoletas FLOAT NULL,
        media_pontos FLOAT NULL,
        PRIMARY KEY (rodada_id)
    );

CREATE TABLE
    IF NOT EXISTS dim_membros_equipes (
        membro_equipe_id INT NOT NULL,
        nome_membro VARCHAR(45) NULL,
        posicao VARCHAR(45) NULL,
        PRIMARY KEY (membro_equipe_id)
    );

CREATE TABLE
    IF NOT EXISTS dim_clubes (
        clube_id INT NOT NULL,
        nome_clube VARCHAR(45) NULL,
        abreviacao VARCHAR(3) NULL,
        PRIMARY KEY (clube_id)
    );

CREATE TABLE
    IF NOT EXISTS fact_pontuacao (
        rodada_id INT NOT NULL,
        clube_id INT NOT NULL,
        membro_equipe_id INT NOT NULL,
        desarmes INT NULL,
        falta_cometida INT NULL,
        gol_contra INT NULL,
        cartao_amarelo INT NULL,
        cartao_vermelho INT NULL,
        jogo_sem_sofrer_gol INT NULL,
        defesa_dificil INT NULL,
        defesa_penalti INT NULL,
        gol_sofrido INT NULL,
        penalti_cometido INT NULL,
        falta_sofrida INT NULL,
        assistencia INT NULL,
        finalizacao_trave INT NULL,
        finalizacao_defendida INT NULL,
        finalizacao_fora INT NULL,
        gols INT NULL,
        impedimento INT NULL,
        penalti_perdido INT NULL,
        penalti_sofrido INT NULL,
        vitoria_ponto_extra INT NULL,
        preco FLOAT NULL,
        media FLOAT NULL,
        pontuacao_rodada FLOAT NULL,
        FOREIGN KEY (rodada_id) REFERENCES dim_rodadas(rodada_id),
        FOREIGN KEY (clube_id) REFERENCES dim_clubes(clube_id),
        FOREIGN KEY (membro_equipe_id) REFERENCES dim_membros_equipes(membro_equipe_id)
    );