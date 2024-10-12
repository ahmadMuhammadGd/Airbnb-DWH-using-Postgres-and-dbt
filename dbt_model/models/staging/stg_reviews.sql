-- WITH reviews_clean AS (
--     SELECT 
--         listing_id,
--         id,
--         date,
--         reviewer_id,
--         INITCAP(
--             TRIM(
--                 REGEXP_REPLACE(LOWER(reviewer_name), '[,-]', ' ', 'g')
--             )
--         ) AS reviewer_name,
--         comments
--     FROM
--         {{ source('airbnb', 'reviews') }}
--     WHERE 
--         listing_id IS NOT NULL
--         AND reviewer_id IS NOT NULL 
--         AND id IS NOT NULL 
--         AND reviewer_name NOT LIKE '%@%'
-- ),
-- reviews_final AS (
--     SELECT
--         listing_id::bigint AS listing_id,
--         id::bigint AS id,
--         date::DATE AS date,
--         reviewer_id::bigint AS reviewer_id,
--         SPLIT_PART(reviewer_name, ' ', 1) AS reviewer_first_name,
--         SPLIT_PART(reviewer_name, ' ', 2) AS reviewer_last_name,
--         comments
--     FROM
--         reviews_clean
-- )
-- SELECT * 
-- FROM reviews_final 
WITH CTE_reviews AS(
    SELECT
        listing_id::bigint AS listing_id,
        id::bigint AS id,
        date::DATE AS date,
        reviewer_id::bigint AS reviewer_id,
        reviewer_name::TEXT AS reviewer_name,
        comments::TEXT AS comments
    FROM 
        {{ source('airbnb', 'reviews') }}
)
SELECT
    *
FROM
    CTE_reviews