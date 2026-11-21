select * replace (cast(total_amount as decimal(10, 2)) as total_amount)
from ({{ dedupe_changes(source('cdc', 'orders_changes')) }})
