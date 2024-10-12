
{{ config(
    indexes=[
      {'columns': ['id'], 'unique': True},
      {'columns': ['listing_id'], 'unique': False},
      {'columns': ['reviewer_id'], 'unique': False}
    ]
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
CTE_pre_cleaned_reviews AS (
    SELECT
        DISTINCT 
            {{ dbt_utils.star(from=ref('stg_reviews'), except=["reviewer_name"]) }},
            INITCAP(LOWER(reviewer_name)) AS reviewer_name
    FROM
        CTE_reviews
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