{{ config(materialized='table') }}

SELECT i._Country_Name_ as country_name, indicator_name
FROM ifs.ifs_table as i
CROSS JOIN UNNEST(split(i.indicator_name, ', ')) AS indicator_name