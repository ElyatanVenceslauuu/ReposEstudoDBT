{% snapshot snap_cadastro_cliente_seed %}
{{
  config(
    target_schema='snapshots',
    unique_key='id_cliente',
    strategy='check', 
    check_cols='all' 
  )
}}

select
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
from {{ ref('stg_cadastro_cliente_seed') }}

{% endsnapshot %}