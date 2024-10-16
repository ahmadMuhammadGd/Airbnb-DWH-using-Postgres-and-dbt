{{ config(
    indexes=[
      {'columns': ['row_id'], 'unique': True},
    ],
    unique_key="row_id", 
    incremental_strategy='append'
)}}

WITH CTE_neighbourhoods AS (
	SELECT
    MD5(neighbourhood || neighbourhood_group) AS row_id
		, neighbourhood_group::TEXT AS neighbourhood_group
		, neighbourhood::TEXT AS neighbourhood 
		, geometry::geometry AS geometry
	FROM
		{{ source('airbnb', 'neighbourhoods') }}
)
SELECT
    DISTINCT ON (s.row_id) s.* 
FROM 
    CTE_neighbourhoods s

{% if is_incremental() %}
LEFT JOIN
    {{ this }} d
ON
    d.row_id = s.row_id
WHERE
    d.row_id IS NULL 
{% endif %}