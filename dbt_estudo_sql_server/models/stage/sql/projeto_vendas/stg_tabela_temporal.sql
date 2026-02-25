{{ config(materialized='table', tags=['stg','pedidos_estage']) }}
  
  with consulta as (
    select 
        ID_DA_VENDA,
        DATA_DA_VENDA,
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
        ID_PRODUTO,
        NOME_DO_PRODUTO,
        PRECO_UNITARIO,
        DESCONTO_MAXIMO,
        SALDO_EM_ESTOQUE,
        QUANTIDADE,
        DESCONTO,
        PRECO_TOTAL,
        ID_VENDEDOR,
        ID_VENDEDOR_SECUNDARIO,
        NOME_DO_VENDEDOR,
        FUNCAO_DO_VENDEDOR,
        DATA_DE_ADMISSAO_VENDEDOR,
        TIPO_DE_VENDA,
        ID_ENTREGADOR,
        NOME_ENTREGADOR,
        --SYSUTCDATETIME() AS execution_date,
        cast('{{ run_started_at }}' as datetime2) as execution_date,
        ROW_NUMBER() OVER (
          PARTITION BY ID_DA_VENDA, ID_PRODUTO
          ORDER BY (SELECT 1)
        ) AS rn
    from {{ source('raw', 'tabela_temporal') }}
  )
  select 
  *
  from consulta
  where 
    rn = 1;