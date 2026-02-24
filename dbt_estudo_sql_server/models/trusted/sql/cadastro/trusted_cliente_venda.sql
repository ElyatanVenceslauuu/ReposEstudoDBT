{{ config(materialized='table', tags=['trusted','cadastro','clientes']) }}

WITH base AS (
  SELECT *
  FROM {{ ref('stg_tabela_temporal') }}
),
ranked AS (
  SELECT
    ID_CLIENTE,
    NOME_DO_CLIENTE,
    RUA,
    NUMERO,
    BAIRRO,
    REFERENCIA,
    CIDADE,
    ESTADO,
    CEP,
    REGIAO,
    ROW_NUMBER() OVER (
      PARTITION BY ID_CLIENTE
      ORDER BY DATA_DA_VENDA DESC, execution_date DESC
    ) AS rn
  FROM base
)
SELECT
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
FROM ranked
WHERE rn = 1;