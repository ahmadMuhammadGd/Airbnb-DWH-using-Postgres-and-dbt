WITH CTE_neighbourhoods AS (
	SELECT
		neighbourhood_group::TEXT AS neighbourhood_group
		, neighbourhood::TEXT AS neighbourhood 
		, geometry::geometry AS geometry
	FROM
		{{ source('airbnb', 'neighbourhoods') }}
)
SELECT
    * 
FROM CTE_neighbourhoods
