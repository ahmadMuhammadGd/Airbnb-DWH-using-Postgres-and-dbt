{{ config(
    indexes=[
      {'columns': ['listing_id'], 'unique': True},
      {'columns': ['neighbourhood_id'], 'unique': False},
    ],
    unique_key='listing_id',
    incremental_strategy='append',
)}}

WITH CTE_stg_listing AS (
    SELECT
        DISTINCT ON (s.listing_id)
        s.listing_id
        , s.listing_url
        , s.source 
        , s.name
        , s.description  
        , s.neighborhood_overview
        , s.picture_url
        , s.latitude
        , s.longitude
        , s.property_type
        , s.room_type
        , s.accommodates
        , s.bathrooms
        , s.bathrooms_text
        , s.bedrooms
        , s.beds
        , s.amenities::jsonb
        , s.neighbourhood_cleansed
    FROM 
        {{ ref('stg_listing') }} s

    {% if is_incremental() %}
    LEFT JOIN
        {{ this }} d
    ON
        s.listing_id = d.listing_id
    WHERE
        d.listing_id IS NULL
    {% endif %}

)
,
CTE_WITH_neighbourhood_id AS (
    SELECT 
        listing_id
        , dm.neighbourhood_id::BIGINT as neighbourhood_id 
        , listing_url
        , source 
        , name
        , description  
        , neighborhood_overview
        , picture_url
        , latitude
        , longitude
        , property_type
        , room_type
        , accommodates
        , bathrooms
        , bathrooms_text
        , bedrooms
        , beds
        , amenities::jsonb
    FROM 
        CTE_stg_listing sl,
        {{ ref('dim_neighbourhood') }} dm
    WHERE
        ST_Contains(dm.geometry, ST_SetSRID(ST_MakePoint(sl.longitude, sl.latitude), 4326))
)

SELECT
    *
FROM 
    CTE_WITH_neighbourhood_id 

