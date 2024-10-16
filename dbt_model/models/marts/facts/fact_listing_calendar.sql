{{ config(
    indexes=[
      {'columns': ['transaction_id'], 'unique': True},
      {'columns': ['date'], 'unique': False},
      {'columns': ['host_id'], 'unique': False},
      {'columns': ['listing_id'], 'unique': False}
    ],
    unique_key='transaction_id',
    incremental_strategy='append',
    pre_hook=[
        "CREATE SEQUENCE IF NOT EXISTS transaction_id_seq AS BIGINT START WITH 1;"
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
        s.listing_id, 
        s.date,
        s.available,
        s.price,
        s.adjusted_price,
        s.minimum_nights,
        s.maximum_nights
    FROM
        {{ ref('int_calendar') }} s
    {% if is_incremental() %}
    LEFT JOIN 
        {{ this }} d    
    ON
        s.listing_id = d.listing_id
        AND
        s.date = d.date
    WHERE
        d.listing_id IS NULL
        AND
        d.date IS NULL
    {% endif %}
)
,
CTE_fact_listing AS (
    SELECT
        NEXTVAL('transaction_id_seq') AS transaction_id
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