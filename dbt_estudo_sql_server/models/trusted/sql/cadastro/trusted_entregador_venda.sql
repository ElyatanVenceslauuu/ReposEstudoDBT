{{ config(materialized='table', tags=['trusted','cadastro','entregadores']) }}

WITH base AS (
  SELECT *
  FROM {{ ref('stg_tabela_temporal') }}
),
ranked AS (
  SELECT
    ID_ENTREGADOR,
    NOME_ENTREGADOR,
    ROW_NUMBER() OVER (
      PARTITION BY ID_ENTREGADOR
      ORDER BY DATA_DA_VENDA DESC, execution_date DESC
    ) AS rn
  FROM base
  WHERE ID_ENTREGADOR IS NOT NULL
)
SELECT
  ID_ENTREGADOR,
  NOME_ENTREGADOR
FROM ranked
WHERE rn = 1;