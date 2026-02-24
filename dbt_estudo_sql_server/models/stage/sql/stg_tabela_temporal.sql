select 
*
from {{ source('raw', 'tabela_temporal') }}