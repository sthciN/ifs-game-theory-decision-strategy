{{ config(materialized='table') }}

SELECT DISTINCT i.indicator_name as indicator_name
FROM {{ ref('indicator') }} AS i