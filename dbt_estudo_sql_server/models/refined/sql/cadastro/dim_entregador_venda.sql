{{ config(materialized='table', tags=['refined','dim','entregadores']) }}

WITH base AS (
  SELECT *
  FROM {{ ref('trusted_entregador_venda') }}
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['ID_ENTREGADOR']) }} AS SK_ENTREGADOR,
  ID_ENTREGADOR,
  NOME_ENTREGADOR
FROM base;