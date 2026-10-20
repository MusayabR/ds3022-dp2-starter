-- One clean row per fragment. DISTINCT collapses a redelivered message.
select distinct
    cast(order_no as integer) as order_no,   -- arrives as a string; as text '10' would sort before '2'
    trim(word)                as word        -- strip any stray spaces around the word
from {{ source('raw', 'fragments') }}        -- source(), never the table name by hand, so dbt draws the lineage
