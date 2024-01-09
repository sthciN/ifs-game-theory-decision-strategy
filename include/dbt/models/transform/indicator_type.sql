WITH indicator_type AS (
    SELECT DISTINCT i.indicator_name
    FROM {{ ref('indicator') }} AS i
)
SELECT indicator_name
from indicator_type