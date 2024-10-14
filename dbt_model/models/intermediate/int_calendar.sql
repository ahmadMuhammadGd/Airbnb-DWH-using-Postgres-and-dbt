{{ config(
    indexes=[
      {'columns': ['date'], 'unique': False},
      {'columns': ['listing_id'], 'unique': False}
    ],
    unique_key='listing_id',
    incremental_strategy='append',
)}}


WITH CTE_calendar AS (
    SELECT
        listing_id, 
        date,
        available,
        price,
        adjusted_price,
        minimum_nights,
        maximum_nights
    FROM 
        {{ ref('stg_calendar') }}        
)
,
CTE_clean_calendar AS ( 
    SELECT
        DISTINCT 
        *
    FROM 
        CTE_calendar
    WHERE
        listing_id IS NOT NULL
        AND
        maximum_nights <= 365
        AND
        available IS NOT NULL
        AND
        price IS NOT NULL
        AND
        date IS NOT NULL
)
SELECT 
    *
FROM
    CTE_clean_calendar
