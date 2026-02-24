{{ config(materialized='table', tags=['refined','fact','vendas']) }}

WITH v AS (
  SELECT *
  FROM {{ ref('trusted_venda') }}
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['v.ID_DA_VENDA']) }} AS SK_VENDA,
  v.ID_DA_VENDA,
  v.DATA_DA_VENDA,
  v.TIPO_DE_VENDA,
  dc.SK_CLIENTE,
  dv.SK_VENDEDOR,
  de.SK_ENTREGADOR,
  v.ID_CLIENTE,
  v.ID_VENDEDOR,
  v.ID_ENTREGADOR,
  v.execution_date
FROM v
LEFT JOIN {{ ref('dim_cliente_venda') }} dc ON v.ID_CLIENTE = dc.ID_CLIENTE
LEFT JOIN {{ ref('dim_vendedor_venda') }} dv ON v.ID_VENDEDOR = dv.ID_VENDEDOR
LEFT JOIN {{ ref('dim_entregador_venda') }} de ON v.ID_ENTREGADOR = de.ID_ENTREGADOR;