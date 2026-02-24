{{ config(materialized='table', tags=['refined','dim','clientes']) }}

WITH base AS (
  SELECT 
  *
  FROM {{ ref('trusted_cliente_venda') }}
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['ID_CLIENTE']) }} AS SK_CLIENTE,
  ID_CLIENTE,
  NOME_DO_CLIENTE,
  RUA,
  NUMERO,
  BAIRRO,
  REFERENCIA,
  CIDADE,
  ESTADO,
  CEP,
  REGIAO
FROM base;