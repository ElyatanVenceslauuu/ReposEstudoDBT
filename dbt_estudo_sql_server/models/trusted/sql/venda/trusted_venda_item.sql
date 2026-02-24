{{ config(materialized='table', tags=['trusted','vendas','itens']) }}

SELECT
  ID_DA_VENDA,
  ID_PRODUTO,
  PRECO_UNITARIO,
  QUANTIDADE,
  DESCONTO,
  PRECO_TOTAL,
  execution_date
FROM {{ ref('stg_tabela_temporal') }};