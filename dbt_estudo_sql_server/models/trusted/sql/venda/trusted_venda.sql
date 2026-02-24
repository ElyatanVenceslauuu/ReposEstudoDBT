{{ config(materialized='table', tags=['trusted','vendas']) }}

WITH base AS (
  SELECT *
  FROM {{ ref('stg_tabela_temporal') }}
),
venda AS (
  SELECT
    ID_DA_VENDA,
    MAX(DATA_DA_VENDA)        AS DATA_DA_VENDA,
    MAX(TIPO_DE_VENDA)        AS TIPO_DE_VENDA,
    MAX(ID_CLIENTE)           AS ID_CLIENTE,
    MAX(ID_VENDEDOR)          AS ID_VENDEDOR,
    MAX(ID_VENDEDOR_SECUNDARIO) AS ID_VENDEDOR_SECUNDARIO,
    MAX(ID_ENTREGADOR)        AS ID_ENTREGADOR,
    MAX(execution_date)       AS execution_date
  FROM base
  GROUP BY ID_DA_VENDA
)
SELECT *
FROM venda;