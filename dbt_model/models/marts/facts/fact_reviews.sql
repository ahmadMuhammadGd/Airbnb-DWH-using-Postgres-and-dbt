{{ config(
    indexes=[
      {'columns': ['review_id'], 'unique': True},
      {'columns': ['listing_id'], 'unique': False},
      {'columns': ['date'], 'unique': False},
    ],
    unique_key='review_id',
    incremental_strategy='append',
)}}

WITH CTE_int_reviews AS (
    SELECT
        s.listing_id
        , s.id
        , s.date
        , s.reviewer_id
        , s.reviewer_name
        , s.comments
        , s.host_id
    FROM
        {{ ref('int_reviews') }} s       
    

    {% if is_incremental() %}
    LEFT JOIN
        {{ this }} d
    ON
        s.id = d.review_id
    WHERE
        d.review_id IS NULL
    {% endif %}

),

CTE_fact_reviews AS (
    SELECT
        id AS review_id
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