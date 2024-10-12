-- cheapest available listings during october 2024
WITH ranked_listings AS (
    SELECT
        listing_id,
        MIN(price) AS min_price,
        MAX(price) AS max_price,
        COUNT(available) AS availability_count,
        RANK() OVER (
            ORDER BY COUNT(available) DESC, MIN(price), MAX(price)
        ) AS rank
    FROM
        dwh.fact_listing
    WHERE 
        EXTRACT(MONTH FROM date) = 10
        AND EXTRACT(YEAR FROM date) = 2024
        AND available = True
    GROUP BY
        listing_id
),

top_10_listings AS (
    SELECT 
        listing_id, 
        min_price, 
        max_price,
        availability_count
    FROM 
        ranked_listings
    WHERE rank <= 10
)

SELECT 
    * 
FROM 
    top_10_listings;



-- most reviewed listing in month = 10
WITH listing_review_cnt AS (
	SELECT
		listing_id
		, COUNT(listing_id) as review_cnt
	FROM
		dwh.fact_reviews
	WHERE
		EXTRACT(MONTH FROM DATE) = 10
	GROUP BY listing_id
)
,
ranked_reviews_cnt AS (
	SELECT 
		*
		, RANK() OVER(ORDER BY review_cnt DESC) as rnk
	FROM
		listing_review_cnt
)
,
ranked_reviews_in_october AS (
	SELECT
		*
	FROM 
		ranked_reviews_cnt

)
SELECT * FROM ranked_reviews_in_october LIMIT 10;



--What is the most expensive neighbourhood in Barcelona ?
WITH denormalized_listing AS (
    SELECT 
        dl.listing_id, 
        dn.neighborhood
    FROM 
        dwh.dim_listing dl
    LEFT JOIN 
        dwh.dim_neighborhood dn
    ON 
        dl.neighborhood_id = dn.neighborhood_id
),
fact_listing_with_neighborhood AS (
    SELECT
        fl.price,
        dl.neighborhood
    FROM 
        dwh.fact_listing fl
    LEFT JOIN 
        denormalized_listing dl
    ON
        dl.listing_id = fl.listing_id
),
final_price_by_neighborhood AS (
    SELECT
        ROUND(AVG(price), 2) AS avg_price,
        neighborhood,
        RANK() OVER (ORDER BY AVG(price) DESC) AS rank
    FROM 
        fact_listing_with_neighborhood
    GROUP BY 
        neighborhood
)
SELECT * 
FROM final_price_by_neighborhood;

