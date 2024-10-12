WITH CTE_neighbourhoods AS (
	SELECT
        neighbourhood_id
		, neighbourhood_group
		, neighbourhood 
		, geometry
	FROM
		{{ ref('int_neightbourhoods') }}
)
SELECT
    * 
FROM CTE_neighbourhoods
