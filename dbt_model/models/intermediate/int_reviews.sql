
{{ config(
    indexes=[
      {'columns': ['id'], 'unique': True},
      {'columns': ['listing_id'], 'unique': False},
      {'columns': ['reviewer_id'], 'unique': False}
    ],
    unique_key='id',
    incremental_strategy='merge',
)}}

WITH CTE_reviews AS(
    SELECT
        listing_id,
        id,
        date,
        reviewer_id,
        reviewer_name,
        comments
    FROM 
        {{ ref('stg_reviews') }}
)
,
CTE_listing_host AS (
    SELECT
      listing_id
    , host_id
    FROM 
        {{ ref('stg_listing') }}
    WHERE
        listing_id IS NOT NULL
        AND
        host_id IS NOT NULL 
)
,
CTE_pre_cleaned_reviews AS (
    SELECT
        DISTINCT 
             id
            , r.listing_id
            , lh.host_id
            , date
            , reviewer_id
            , comments
            , INITCAP(LOWER(reviewer_name)) AS reviewer_name
    FROM
        CTE_reviews r
    LEFT JOIN
        CTE_listing_host lh         
    ON
        lh.listing_id = r.listing_id
)
,
CTE_cleaned_reviews AS (
    SELECT
        *
    FROM
        CTE_pre_cleaned_reviews
    WHERE
        listing_id IS NOT NULL
        AND
        id IS NOT NULL
        AND
        date IS NOT NULL
        AND
        reviewer_id IS NOT NULL
        AND
        reviewer_name IS NOT NULL
)
SELECT
    *
FROM
    CTE_cleaned_reviews