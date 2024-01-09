WITH indicator AS (
    SELECT _Country_Name_ as country_name,
    Indicator_Name as indicators,
    FROM ifs.ifs_table
)
SELECT country_name, indicator_name
FROM indicator
CROSS JOIN UNNEST(split(indicator.indicators, ', ')) AS indicator_name