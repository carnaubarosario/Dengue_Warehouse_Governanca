-- ============================================================
-- DW GOVERNANÇA — EXECUTAR NO BANCO dw_dengue_qualidade
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_dataset (
    sk_dataset   INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nome_dataset VARCHAR(80) NOT NULL UNIQUE,
    descricao    VARCHAR(255),
    responsavel  VARCHAR(120),
    criado_em    TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dim_ativo (
    sk_ativo INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tabela   VARCHAR(120) NOT NULL,
    coluna   VARCHAR(120),
    CONSTRAINT uq_dim_ativo UNIQUE NULLS NOT DISTINCT (tabela, coluna)
);

CREATE TABLE IF NOT EXISTS dim_dimensao_qualidade (
    sk_dimensao   SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nome_dimensao VARCHAR(30) NOT NULL UNIQUE,
    descricao     VARCHAR(255)
);

INSERT INTO dim_dimensao_qualidade (nome_dimensao, descricao) VALUES
('completude', 'Proporção de valores não nulos em relação ao total'),
('unicidade', 'Ausência de duplicidade para uma chave de negócio'),
('validade_dominio', 'Valores pertencem ao domínio esperado'),
('validade_range', 'Valores estão no intervalo esperado')
ON CONFLICT (nome_dimensao) DO NOTHING;

CREATE TABLE IF NOT EXISTS dim_tempo_execucao (
    sk_tempo_execucao  INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    timestamp_execucao TIMESTAMP NOT NULL UNIQUE,
    data_execucao     DATE NOT NULL,
    ano               SMALLINT NOT NULL,
    mes               SMALLINT NOT NULL,
    dia               SMALLINT NOT NULL,
    hora              SMALLINT NOT NULL,
    dia_semana_num    SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS fato_dq_metrica (
    sk_metrica        BIGINT GENERATED ALWAYS AS IDENTITY,
    data_execucao     DATE NOT NULL,
    sk_dataset        INTEGER NOT NULL REFERENCES dim_dataset(sk_dataset),
    sk_tempo_execucao INTEGER NOT NULL REFERENCES dim_tempo_execucao(sk_tempo_execucao),
    sk_ativo          INTEGER NOT NULL REFERENCES dim_ativo(sk_ativo),
    sk_dimensao       SMALLINT NOT NULL REFERENCES dim_dimensao_qualidade(sk_dimensao),
    etapa             VARCHAR(20) NOT NULL,
    passou            BOOLEAN NOT NULL,
    taxa              NUMERIC(7,4),
    qtd_violacoes     BIGINT,
    total_linhas      BIGINT,
    detalhe           JSONB,

    PRIMARY KEY (sk_metrica, data_execucao)
) PARTITION BY RANGE (data_execucao);

CREATE TABLE IF NOT EXISTS fato_dq_metrica_default
PARTITION OF fato_dq_metrica DEFAULT;

CREATE INDEX IF NOT EXISTS idx_dq_data_brin
ON fato_dq_metrica USING BRIN (data_execucao);

CREATE INDEX IF NOT EXISTS idx_dq_dataset_data
ON fato_dq_metrica (sk_dataset, data_execucao);

CREATE INDEX IF NOT EXISTS idx_dq_falhas
ON fato_dq_metrica (data_execucao)
WHERE passou = FALSE;

CREATE INDEX IF NOT EXISTS idx_dq_ativo_dim_data
ON fato_dq_metrica (sk_ativo, sk_dimensao, data_execucao);

-- Função idempotente para criar partição mensal.
CREATE OR REPLACE FUNCTION ensure_particao_mensal(p_data DATE)
RETURNS VOID AS $$
DECLARE
    inicio DATE := date_trunc('month', p_data)::DATE;
    fim DATE := (date_trunc('month', p_data) + INTERVAL '1 month')::DATE;
    nome TEXT := format('fato_dq_metrica_%s', to_char(inicio, 'YYYY_MM'));
BEGIN
    IF to_regclass(nome) IS NULL THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF fato_dq_metrica
             FOR VALUES FROM (%L) TO (%L)',
            nome, inicio, fim
        );
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE VIEW vw_governanca_qualidade AS
SELECT
    m.sk_metrica,
    d.nome_dataset,
    t.timestamp_execucao,
    m.data_execucao,
    a.tabela,
    a.coluna,
    dq.nome_dimensao AS dimensao,
    m.etapa,
    m.passou,
    m.taxa,
    m.qtd_violacoes,
    m.total_linhas,
    m.detalhe
FROM fato_dq_metrica m
JOIN dim_dataset d ON d.sk_dataset = m.sk_dataset
JOIN dim_tempo_execucao t ON t.sk_tempo_execucao = m.sk_tempo_execucao
JOIN dim_ativo a ON a.sk_ativo = m.sk_ativo
JOIN dim_dimensao_qualidade dq ON dq.sk_dimensao = m.sk_dimensao;

-- ============================================================
-- CHECKS
-- ============================================================

-- No dw_dengue:
-- SELECT relname, pg_size_pretty(pg_total_relation_size(oid))
-- FROM pg_class
-- WHERE relname LIKE 'fato_dengue%'
-- ORDER BY pg_total_relation_size(oid) DESC;

-- Após a carga:
-- ANALYZE fato_dengue;

-- Teste de partition pruning:
-- EXPLAIN (ANALYZE, BUFFERS)
-- SELECT COUNT(*)
-- FROM fato_dengue
-- WHERE nu_ano = 2021;

-- A saída deve mostrar somente fato_dengue_2021.

SELECT COUNT(*) FROM fato_dq_metrica;

SELECT t.timestamp_execucao,
       COUNT(*) FILTER (WHERE m.etapa = 'bruto') AS qtd_bruto,
       COUNT(*) FILTER (WHERE m.etapa = 'pre_carga') AS qtd_pre_carga,
       COUNT(*) FILTER (WHERE m.etapa = 'pos_carga') AS qtd_pos_carga
FROM fato_dq_metrica m
JOIN dim_tempo_execucao t ON t.sk_tempo_execucao = m.sk_tempo_execucao
GROUP BY t.timestamp_execucao
ORDER BY t.timestamp_execucao;

DELETE FROM fato_dq_metrica
WHERE sk_tempo_execucao IN (
    SELECT t.sk_tempo_execucao
    FROM dim_tempo_execucao t
    WHERE NOT EXISTS (
        SELECT 1 FROM fato_dq_metrica m
        WHERE m.sk_tempo_execucao = t.sk_tempo_execucao AND m.etapa = 'pos_carga'
    )
);

REFRESH MATERIALIZED VIEW mv_governanca_qualidade;