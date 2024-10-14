{{ config(
    indexes=[
      {'columns': ['id'], 'unique': False},
      {'columns': ['listing_id'], 'unique': False}
    ],
    unique_key='id',
    incremental_strategy='merge',
)}}

WITH CTE_int_reviews AS (
    SELECT
        listing_id
        , id
        , date
        , reviewer_id
        , reviewer_name
        , comments
        , host_id
    FROM
        {{ ref('int_reviews') }}
),

CTE_fact_reviews AS (
    SELECT DISTINCT
        id 
        , listing_id
        , host_id
        , date
        , reviewer_id
        , comments
    FROM 
        CTE_int_reviews
)
SELECT
    *
FROM
    CTE_fact_reviews