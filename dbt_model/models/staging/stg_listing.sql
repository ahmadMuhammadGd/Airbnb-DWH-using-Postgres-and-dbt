{{ config(
    indexes=[
      {'columns': ['listing_id'], 'unique': True},
      {'columns': ['host_id'], 'unique': False},
    ],
    unique_key="listing_id"
)}}
WITH CTE_listing AS (
    SELECT 
        id::BIGINT AS listing_id, 
        listing_url::TEXT AS listing_url, 
        scrape_id::BIGINT AS scrape_id, 
        last_scraped::DATE AS last_scraped, 
        source::TEXT AS source, 
        name::TEXT AS name, 
        description::TEXT AS description, 
        neighborhood_overview::TEXT AS neighborhood_overview, 
        picture_url::TEXT AS picture_url, 
        host_id::BIGINT AS host_id, 
        host_url::TEXT AS host_url, 
        host_name::TEXT AS host_name, 
        host_since::DATE AS host_since, 
        host_location::TEXT AS host_location, 
        host_about::TEXT AS host_about, 
        host_response_time::TEXT AS host_response_time, 
        host_response_rate::TEXT AS host_response_rate, 
        host_acceptance_rate::TEXT AS host_acceptance_rate, 
        host_is_superhost::BOOLEAN AS host_is_superhost, 
        host_thumbnail_url::TEXT AS host_thumbnail_url, 
        host_picture_url::TEXT AS host_picture_url, 
        host_neighbourhood::TEXT AS host_neighbourhood, 
        host_listings_count::INTEGER AS host_listings_count, 
        host_total_listings_count::INTEGER AS host_total_listings_count, 
        host_verifications::TEXT AS host_verifications, 
        host_has_profile_pic::BOOLEAN AS host_has_profile_pic, 
        host_identity_verified::BOOLEAN AS host_identity_verified, 
        neighbourhood::TEXT AS neighbourhood, 
        neighbourhood_cleansed::TEXT AS neighbourhood_cleansed, 
        neighbourhood_group_cleansed::TEXT AS neighbourhood_group_cleansed, 
        latitude::NUMERIC AS latitude, 
        longitude::NUMERIC AS longitude, 
        property_type::TEXT AS property_type, 
        room_type::TEXT AS room_type, 
        accommodates::INTEGER AS accommodates, 
        bathrooms::INTEGER AS bathrooms, 
        bathrooms_text::TEXT AS bathrooms_text, 
        bedrooms::INTEGER AS bedrooms, 
        beds::INTEGER AS beds, 
        amenities::JSON AS amenities, 
        price::TEXT AS price, 
        minimum_nights::INTEGER AS minimum_nights, 
        maximum_nights::INTEGER AS maximum_nights, 
        minimum_minimum_nights::INTEGER AS minimum_minimum_nights, 
        maximum_minimum_nights::INTEGER AS maximum_minimum_nights, 
        minimum_maximum_nights::INTEGER AS minimum_maximum_nights, 
        maximum_maximum_nights::INTEGER AS maximum_maximum_nights, 
        minimum_nights_avg_ntm::FLOAT AS minimum_nights_avg_ntm, 
        maximum_nights_avg_ntm::FLOAT AS maximum_nights_avg_ntm, 
        calendar_updated::TEXT AS calendar_updated, 
        has_availability::BOOLEAN AS has_availability, 
        availability_30::INTEGER AS availability_30, 
        availability_60::INTEGER AS availability_60, 
        availability_90::INTEGER AS availability_90, 
        availability_365::INTEGER AS availability_365, 
        calendar_last_scraped::DATE AS calendar_last_scraped, 
        number_of_reviews::INTEGER AS number_of_reviews, 
        number_of_reviews_ltm::INTEGER AS number_of_reviews_ltm, 
        number_of_reviews_l30d::INTEGER AS number_of_reviews_l30d, 
        first_review::DATE AS first_review, 
        last_review::DATE AS last_review, 
        review_scores_rating::FLOAT AS review_scores_rating, 
        review_scores_accuracy::FLOAT AS review_scores_accuracy, 
        review_scores_cleanliness::FLOAT AS review_scores_cleanliness, 
        review_scores_checkin::FLOAT AS review_scores_checkin, 
        review_scores_communication::FLOAT AS review_scores_communication, 
        review_scores_location::FLOAT AS review_scores_location, 
        review_scores_value::FLOAT AS review_scores_value, 
        license::TEXT AS license, 
        instant_bookable::BOOLEAN AS instant_bookable, 
        calculated_host_listings_count::INTEGER AS calculated_host_listings_count, 
        calculated_host_listings_count_entire_homes::INTEGER AS calculated_host_listings_count_entire_homes, 
        calculated_host_listings_count_private_rooms::INTEGER AS calculated_host_listings_count_private_rooms, 
        calculated_host_listings_count_shared_rooms::INTEGER AS calculated_host_listings_count_shared_rooms, 
        reviews_per_month::FLOAT AS reviews_per_month
    FROM 
        {{ source('airbnb', 'listings') }}
)
SELECT 
    s.* 
FROM
    CTE_listing s
{% if is_incremental() %}

LEFT JOIN 
    {{ this }} d
ON 
    s.listing_id=d.listing_id
WHERE
    d.listing_id IS NULL 

{% endif %}