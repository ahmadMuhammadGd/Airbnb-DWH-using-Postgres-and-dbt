{{ config(
    indexes=[
      {'columns': ['listing_id'], 'unique': False},
      {'columns': ['date'], 'unique': False},
      {'columns': ['id'], 'unique': True},
    ],
    unique_key="id"
)}}

WITH CTE_reviews AS(
    SELECT
        id::bigint AS id,
        listing_id::bigint AS listing_id,
        date::DATE AS date,
        reviewer_id::bigint AS reviewer_id,
        reviewer_name::TEXT AS reviewer_name,
        comments::TEXT AS comments
    FROM 
        {{ source('airbnb', 'reviews') }}
)
SELECT
    s.*
FROM
    CTE_reviews s
{% if is_incremental() %}
LEFT JOIN
    {{ this }} d                      
ON  
    d.id = s.id              
WHERE
    d.id IS NULL
{% endif %}
