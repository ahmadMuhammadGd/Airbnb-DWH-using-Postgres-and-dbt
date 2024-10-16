{{ config(
    indexes=[
      {'columns': ['listing_id'], 'unique': False},
      {'columns': ['date'], 'unique': False},
      {'columns': ['calendar_id'], 'unique': True},
    ],
    unique_key=['calendar_id']
)}}

SELECT 
    s.calendar_id::bigint AS calendar_id,
    s.listing_id::bigint AS listing_id,
    s.date::DATE AS date,
    s.available::BOOLEAN AS available,
    s.price::NUMERIC AS price,
    s.adjusted_price::NUMERIC AS adjusted_price,
    s.minimum_nights::INTEGER AS minimum_nights,
    s.maximum_nights::INTEGER AS maximum_nights
FROM 
    {{ source('airbnb', 'calendar') }} s

{% if is_incremental() %}
LEFT JOIN 
    {{ this }} d
ON 
    s.calendar_id=d.calendar_id
WHERE
    d.calendar_id IS NULL 
{% endif %}