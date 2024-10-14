{{ config(
    indexes=[
      {'columns': ['neighbourhood_id'], 'unique': True},
    ],
    unique_key='neighbourhood_id',
    incremental_strategy='merge',
)}}


WITH CTE_neighbourhoods AS (
	SELECT
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS neighbourhood_id
		, neighbourhood_group::TEXT AS neighbourhood_group
		, neighbourhood::TEXT AS neighbourhood 
		, geometry::geometry AS geometry
	FROM
		{{ ref('stg_neighbourhoods') }}
)
SELECT
    * 
FROM CTE_neighbourhoods
