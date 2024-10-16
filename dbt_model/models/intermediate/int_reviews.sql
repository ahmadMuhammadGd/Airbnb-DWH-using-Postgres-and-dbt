
{{ config(
    indexes=[
      {'columns': ['id'], 'unique': True},
      {'columns': ['listing_id'], 'unique': False},
      {'columns': ['date'], 'unique': False},
      {'columns': ['reviewer_id'], 'unique': False}
    ],
    unique_key='id',
    incremental_strategy='append',
)}}

WITH CTE_reviews AS(
    SELECT
        DISTINCT 
        s.listing_id,
        s.id,
        s.date,
        s.reviewer_id,
        s.reviewer_name,
        s.comments
    FROM 
        {{ ref('stg_reviews') }} s


    {% if is_incremental() %}
    LEFT JOIN
        {{ this }} d
    ON
        s.reviewer_id = d.reviewer_id
    WHERE
        d.reviewer_id IS NULL
    {% endif %}
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
             r.id
            , r.listing_id
            , lh.host_id
            , r.date
            , r.reviewer_id
            , r.comments
            , INITCAP(LOWER(r.reviewer_name)) AS reviewer_name
    FROM
        CTE_reviews r
    LEFT JOIN
        CTE_listing_host lh         
    ON
        lh.listing_id = r.listing_id

    {% if is_incremental() %}
    LEFT JOIN
        {{ this }} d  
    ON
        d.id = r.id    
    WHERE
        d.id IS NULL
    {% endif %}
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