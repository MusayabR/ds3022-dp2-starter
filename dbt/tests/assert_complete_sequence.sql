-- Returns a row (= the test fails) unless there are exactly
-- 21 fragments and no gaps in order_no.
-- Worked out on paper: 1..21 -> no row (PASS) | 7 missing -> 20 vs 21 (FAIL) | two words for 7 -> 22 vs 21 (FAIL)
select count(*) as n, min(order_no) as lo, max(order_no) as hi
from {{ ref('stg_fragments') }}              -- runs on the cleaned rows, not raw
having count(*) <> {{ var('expected_fragments', 21) }}
    or max(order_no) - min(order_no) + 1 <> count(*)
