{{ config(materialized='table', tags=['refined','dim','produtos']) }}

WITH base AS (
  SELECT *
  FROM {{ ref('trusted_produto_venda') }}
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['ID_PRODUTO']) }} AS SK_PRODUTO,
  ID_PRODUTO,
  NOME_DO_PRODUTO,
  DESCONTO_MAXIMO,
  SALDO_EM_ESTOQUE
FROM base;