{{ config(
    indexes=[
      {'columns': ['reviewer_id'], 'unique': False},
    ],
    unique_key='reviewer_id',
    incremental_strategy='append',
)}}

WITH CTE_reviewer AS(
    SELECT DISTINCT ON( s.reviewer_id )
          s.reviewer_id
        , s.reviewer_name
    FROM 
        {{ ref('int_reviews') }} s

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
CTE_cleaned_reviewers AS (
    SELECT DISTINCT
          reviewer_id
        , REGEXP_REPLACE(LOWER(reviewer_name), '[,-]', ' ', 'g') AS reviewer_name
    FROM
        CTE_reviewer
)
,
CTE_enriched_reviewers AS (
    SELECT 
          reviewer_id
        , SPLIT_PART(reviewer_name, ' ', 1) AS reviewer_first_name
        , SPLIT_PART(reviewer_name, ' ', 2) AS reviewer_last_name
    FROM 
        CTE_cleaned_reviewers
)
SELECT 
    *
FROM
    CTE_enriched_reviewers
WHERE
    LENGTH(reviewer_first_name) > 1 
    AND LENGTH(reviewer_last_name) > 1

