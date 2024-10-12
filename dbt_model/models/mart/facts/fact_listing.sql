{{ config(
    indexes=[
      {'columns': ['pk'], 'unique': True},
      {'columns': ['date'], 'unique': False},
      {'columns': ['host_id'], 'unique': False},
      {'columns': ['listing_id'], 'unique': False}
    ]
)}}


WITH CTE_stg_listing AS (
    SELECT
        host_id,
        listing_id
    FROM
        {{ ref('stg_listing') }}
)
,
CTE_int_calendar AS (
    SELECT
        listing_id, 
        date,
        available,
        price,
        adjusted_price,
        minimum_nights,
        maximum_nights
    FROM
        {{ ref('int_calendar') }}
)
,
CTE_fact_listing AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY c.date, c.listing_id) AS pk
        , c.listing_id 
        , l.host_id 
        , c.date 
        , c.available 
        , c.price 
        , c.adjusted_price 
        , c.minimum_nights 
        , c.maximum_nights 
    FROM
        CTE_int_calendar c
    LEFT JOIN
        CTE_stg_listing l   
    ON  
        c.listing_id = l.listing_id
)
SELECT * FROM CTE_fact_listing