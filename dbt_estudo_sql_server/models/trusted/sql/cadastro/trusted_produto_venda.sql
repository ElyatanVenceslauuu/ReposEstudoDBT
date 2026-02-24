{{ config(materialized='table', tags=['trusted','cadastro','produtos']) }}

WITH base AS (
  SELECT *
  FROM {{ ref('stg_tabela_temporal') }}
),
ranked AS (
  SELECT
    ID_PRODUTO,
    NOME_DO_PRODUTO,
    DESCONTO_MAXIMO,
    SALDO_EM_ESTOQUE,
    ROW_NUMBER() OVER (
      PARTITION BY ID_PRODUTO
      ORDER BY DATA_DA_VENDA DESC, execution_date DESC
    ) AS rn
  FROM base
)
SELECT
  ID_PRODUTO,
  NOME_DO_PRODUTO,
  DESCONTO_MAXIMO,
  SALDO_EM_ESTOQUE
FROM ranked
WHERE rn = 1;