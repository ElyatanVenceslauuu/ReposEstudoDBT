{{ config(materialized='table', tags=['refined','dim','vendedores']) }}

WITH base AS (
  SELECT *
  FROM {{ ref('trusted_vendedor_venda') }}
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['ID_VENDEDOR']) }} AS SK_VENDEDOR,
  ID_VENDEDOR,
  NOME_DO_VENDEDOR,
  FUNCAO_DO_VENDEDOR,
  DATA_DE_ADMISSAO_VENDEDOR
FROM base;