{{ 
    config(
        materialized='table', tags=['stg','cadastro','cliente','seed']
    ) 
}}

SELECT
  id_cliente,
  UPPER(ltrim(rtrim(nome_do_cliente))) AS nome_do_cliente,
  ltrim(rtrim(rua)) AS rua,
  ltrim(rtrim(numero)) AS numero,
  ltrim(rtrim(bairro)) AS bairro,
  referencia,
  ltrim(rtrim(cidade)) AS cidade,
  ltrim(rtrim(estado)) AS estado,
  ltrim(rtrim(cep)) AS cep,
  ltrim(rtrim(regiao)) AS regiao,
  dt_atualizacao
FROM {{ ref('cadastro_cliente') }};