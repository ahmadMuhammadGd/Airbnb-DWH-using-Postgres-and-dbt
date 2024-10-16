{{ config(
    indexes=[
      {'columns': ['date'], 'unique': False},
      {'columns': ['listing_id'], 'unique': False},
      {'columns': ['calendar_id'], 'unique': True},
    ],
    unique_key='calendar_id',
    incremental_strategy='append',
)}}


WITH CTE_calendar AS (
    SELECT
        s.calendar_id,
        s.listing_id, 
        s.date,
        s.available,
        s.price,
        s.adjusted_price,
        s.minimum_nights,
        s.maximum_nights
    FROM 
        {{ ref('stg_calendar') }} s 


    {% if is_incremental() %}
    LEFT JOIN 
        {{ this }} d
    ON 
        s.calendar_id=d.calendar_id
    WHERE
        d.calendar_id IS NULL 
    {% endif %}


)
,
CTE_clean_batch AS ( 
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
        minimum_nights <= 365
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
    CTE_clean_batch 