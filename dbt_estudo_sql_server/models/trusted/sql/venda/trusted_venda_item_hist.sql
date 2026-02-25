{{ 
    config(
        materialized='incremental', 
        incremental_strategy='append',
        tags=['trusted','vendas','itens']
    ) 
}}
WITH BASE AS (
    SELECT
        ID_DA_VENDA,
        ID_PRODUTO,
        PRECO_UNITARIO,
        QUANTIDADE,
        DESCONTO,
        PRECO_TOTAL,
        execution_date
    FROM {{ ref('stg_tabela_temporal') }}
),
FINAL AS (
    SELECT
        ID_DA_VENDA,
        ID_PRODUTO,
        PRECO_UNITARIO,
        QUANTIDADE,
        DESCONTO,
        PRECO_TOTAL,
        cast(execution_date as datetime2) as dt_execucao,
        '{{ invocation_id }}' as RUN_ID
    FROM BASE
)
SELECT 
    *
FROM FINAL
{% if is_incremental() %}
-- evita duplicar se o MESMO run for reexecutado (retry)
WHERE NOT EXISTS (
  SELECT 1
  FROM {{ this }} t
  WHERE t.RUN_ID = '{{ invocation_id }}'
)
{% endif %};