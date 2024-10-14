{{ config(
    indexes=[
      {'columns': ['listing_id'], 'unique': True},
      {'columns': ['neighbourhood_id'], 'unique': False},
    ],
    unique_key='listing_id',
    incremental_strategy='merge',
)}}

WITH CTE_stg_listing AS (
    SELECT
        DISTINCT
        listing_id
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
        , neighbourhood_cleansed
    FROM 
        {{ ref('stg_listing') }}
)
,
CTE_WITH_neighbourhood_id AS (
    SELECT 
        listing_id
        , dm.neighbourhood_id as neighbourhood_id 
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

