WITH CTE_reviewer AS(
    SELECT DISTINCT
          reviewer_id
        , reviewer_name
    FROM 
        {{ ref('int_reviews') }}
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