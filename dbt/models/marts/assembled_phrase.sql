-- One row: how many fragments, and the phrase in order.
-- Only gets built if every test on stg_fragments passed first.
select
    count(*)                                as fragment_count,
    string_agg(word, ' ' order by order_no) as phrase   -- joins words with a space, in numeric order
from {{ ref('stg_fragments') }}
