# Arquitetura e Linhagem de Dados -- dengue_warehouse

Gerado automaticamente em 14/08/2026 16:22.

## Fluxo ponta a ponta

```mermaid
flowchart TD
    A[("SINAN / DATASUS<br/>CSV bruto por ano")] --> B["Download<br/>(Selenium + requests)"]
    IBGE[("IBGE<br/>Populacao por UF")] --> B
    B --> C["Extracao dos ZIPs"]
    C --> D{"Transformacao<br/>(PySpark ou pandas)"}

    D -- "snapshot ANTES de normalizar" --> Q1["Qualidade: etapa BRUTO<br/>completude / mal digitado"]
    D -- "apos normalize_uf, as_flag, etc" --> E["CSV consolidado<br/>(dengue_consolidada.csv)"]
    E -- "snapshot do consolidado" --> Q2["Qualidade: etapa PRE_CARGA<br/>completude / dominio / range"]

    E --> F["COPY --> staging_dengue<br/>(UNLOGGED, dw_dengue)"]
    F --> G["INSERT...SELECT DISTINCT<br/>--> dim_vitima"]
    F --> H["INSERT...SELECT + JOIN<br/>--> fato_dengue"]
    G --> H

    IBGE2["dim_uf"] --> H
    T["dim_tempo"] --> H

    H -- "SQL nas tabelas finais" --> Q3["Qualidade: etapa POS_CARGA<br/>completude / dominio / range"]

    Q1 --> GOV[("dw_dengue_qualidade<br/>(particionado por mes)")]
    Q2 --> GOV
    Q3 --> GOV

    GOV --> MV["View materializada<br/>mv_governanca_qualidade"]
    MV --> DASH["Dashboard HTML<br/>(Chart.js)"]
    MV --> PBI["Power BI<br/>(conexao direta)"]

    H --> DICT["Dicionario de dados<br/>(PDF, schema real via COMMENT ON)"]
    GOV --> DICT
```

## Bancos e responsabilidades

```mermaid
flowchart LR
    subgraph dw_dengue
        direction TB
        STG["staging_dengue<br/>(transiente)"]
        DU["dim_uf"]
        DT["dim_tempo"]
        DV["dim_vitima"]
        FD["fato_dengue"]
        STG --> DV
        STG --> FD
        DU --> FD
        DT --> FD
        DV --> FD
    end

    subgraph dw_dengue_qualidade
        direction TB
        DD["dim_dataset"]
        DA["dim_ativo"]
        DQ["dim_dimensao_qualidade"]
        DTE["dim_tempo_execucao"]
        FQ["fato_dq_metrica<br/>(particionada por mes)"]
        DD --> FQ
        DA --> FQ
        DQ --> FQ
        DTE --> FQ
    end

    dw_dengue -. "checagens de qualidade<br/>(bruto/pre_carga/pos_carga)" .-> dw_dengue_qualidade
```

## Legenda de etapas de qualidade


| Etapa | Quando roda | O que mede |
|---|---|---|
| `bruto` | Antes de qualquer normalizacao | Qualidade real do dado como sai do SINAN |
| `pre_carga` | Depois da limpeza (Spark/pandas), antes do DW | Efeito da transformacao na qualidade |
| `pos_carga` | Depois de carregado em `dw_dengue` | Fidelidade do processo de carga (staging + JOIN) |
