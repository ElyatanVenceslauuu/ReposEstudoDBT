{{ config(materialized='table', tags=['refined','cadastro','clientes']) }}

WITH ultima_versao AS (
  SELECT
    id_cliente,
    nome_do_cliente,
    rua,
    numero,
    bairro,
    referencia,
    cidade,
    estado,
    cep,
    regiao,
    dt_atualizacao
  FROM {{ ref('snap_cadastro_cliente_seed') }}
  WHERE dbt_valid_to IS NULL
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['id_cliente']) }} AS sk_cliente,
  id_cliente,
  nome_do_cliente,
  rua,
  numero,
  bairro,
  referencia,
  cidade,
  estado,
  cep,
  regiao,
  dt_atualizacao
FROM ultima_versao;