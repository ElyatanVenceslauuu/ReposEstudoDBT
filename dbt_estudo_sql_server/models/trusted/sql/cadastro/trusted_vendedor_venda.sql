{{ config(materialized='table', tags=['trusted','cadastro','vendedores']) }}

WITH base AS (
  SELECT *
  FROM {{ ref('stg_tabela_temporal') }}
),
ranked AS (
  SELECT
    ID_VENDEDOR,
    NOME_DO_VENDEDOR,
    FUNCAO_DO_VENDEDOR,
    DATA_DE_ADMISSAO_VENDEDOR,
    ROW_NUMBER() OVER (
      PARTITION BY ID_VENDEDOR
      ORDER BY DATA_DA_VENDA DESC, execution_date DESC
    ) AS rn
  FROM base
  WHERE ID_VENDEDOR IS NOT NULL
)
SELECT
  ID_VENDEDOR,
  NOME_DO_VENDEDOR,
  FUNCAO_DO_VENDEDOR,
  DATA_DE_ADMISSAO_VENDEDOR
FROM ranked
WHERE rn = 1;