
WITH filtered_listing AS (
	SELECT 
		listing_id
		, bathrooms
		, bedrooms
		, beds
		, neighbourhood_id
	FROM 
		dwh.dim_listing
	WHERE
		bathrooms = 1
		AND
		bedrooms = 2 OR bedrooms = 2 
		AND
		beds = 2 OR beds = 3
)
,
transform_query AS(
    SELECT
        fc.listing_id
		, bathrooms
		, bedrooms
		, beds
		, dn.neighbourhood_group
		, dn.neighbourhood
        , fc.price 
        , fc.date
		, dd.week_start_date
		, dd.week_end_date
    FROM
        dwh.fact_listing fc
    LEFT JOIN 
        filtered_listing fl
    ON 
        fl.listing_id = fc.listing_id
	LEFT JOIN
		dwh.dim_date dd
	ON
		fc.date = dd.date_day
	LEFT JOIN 
		dwh.dim_neighbourhood dn
	ON
		fl.neighbourhood_id = dn.neighbourhood_id
    WHERE
        fl.listing_id IS NOT NULL
        AND dd.month_of_year = 11
        AND dd.year_number = 2024
)
,
final_result AS (
    SELECT 
		DISTINCT
        listing_id
		, bathrooms
		, bedrooms
		, beds
		, neighbourhood_group
		, neighbourhood        
        , AVG(price) AS avg_price
        , week_start_date
        , week_end_date
    FROM transform_query
    GROUP BY 
	      listing_id
		, bathrooms
		, bedrooms
		, beds
		, neighbourhood_group
		, neighbourhood        
        , week_start_date
        , week_end_date
	ORDER BY AVG(price)
)
	SELECT * FROM final_result