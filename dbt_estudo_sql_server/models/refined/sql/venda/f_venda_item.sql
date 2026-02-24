{{ config(materialized='table', tags=['refined','fact','vendas','itens']) }}

WITH i AS (
  SELECT *
  FROM {{ ref('trusted_venda_item') }}
)
SELECT
  -- SK do item (venda + produto) = chave natural do grão
  {{ dbt_utils.generate_surrogate_key(['i.ID_DA_VENDA','i.ID_PRODUTO']) }} AS SK_VENDA_ITEM,
  fv.SK_VENDA,
  dp.SK_PRODUTO,
  i.ID_DA_VENDA,
  i.ID_PRODUTO,
  i.PRECO_UNITARIO,
  i.QUANTIDADE,
  i.DESCONTO,
  i.PRECO_TOTAL,
  i.execution_date
FROM i
LEFT JOIN {{ ref('f_venda') }} fv ON i.ID_DA_VENDA = fv.ID_DA_VENDA
LEFT JOIN {{ ref('dim_produto_venda') }} dp ON i.ID_PRODUTO = dp.ID_PRODUTO;