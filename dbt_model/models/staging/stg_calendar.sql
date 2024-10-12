WITH CTE_calendar AS (
    SELECT
        listing_id::bigint AS listing_id,
        date::DATE AS date,
        available::BOOLEAN AS available,
        price::NUMERIC AS price,
        adjusted_price::NUMERIC AS adjusted_price,
        minimum_nights::INTEGER AS minimum_nights,
        maximum_nights::INTEGER AS maximum_nights
    FROM 
        {{source('airbnb', 'calendar')}}
)
SELECT *
FROM CTE_calendar