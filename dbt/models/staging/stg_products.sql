select * replace (cast(price as decimal(10, 2)) as price)
from ({{ dedupe_changes(source('cdc', 'products_changes')) }})
