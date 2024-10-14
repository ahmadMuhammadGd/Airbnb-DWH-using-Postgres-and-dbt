{{ config(
    indexes=[
      {'columns': ['host_id'], 'unique': True},
    ],
    unique_key='id',
    incremental_strategy='merge',
)}}

WITH CTE_host AS (
    SELECT
        DISTINCT
        host_id
        , host_url
        , host_name
        , host_since
        , host_location
        , host_about
        , host_response_time
        , host_response_rate
        , host_acceptance_rate
        , host_is_superhost
        , host_thumbnail_url
        , host_picture_url
        , host_neighbourhood
        , host_listings_count
        , host_total_listings_count
        , host_verifications
        , host_has_profile_pic
        , host_identity_verified
    FROM
        {{ ref('stg_listing') }}
)
SELECT
    *
FROM    
    CTE_host
