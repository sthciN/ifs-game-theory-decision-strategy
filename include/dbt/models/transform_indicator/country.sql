{{ config(materialized='table') }}

SELECT DISTINCT i._Country_Name_ as country_name
FROM ifs.ifs_table AS i
