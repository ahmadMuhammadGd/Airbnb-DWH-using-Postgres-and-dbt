{{ config(
    indexes=[
      {'columns': ['neighbourhood_id'], 'unique': True},
    ],
    unique_key=['neighbourhood_group', 'neighbourhood'],
    incremental_strategy='append',
    pre_hook=[
        "CREATE SEQUENCE IF NOT EXISTS neighbourhoods_id_seq AS BIGINT START WITH 1;"
    ]
) }}

WITH CTE_neighbourhoods AS (
	SELECT
        NEXTVAL('neighbourhoods_id_seq') AS neighbourhood_id, 
		s.neighbourhood_group::TEXT AS neighbourhood_group,
		s.neighbourhood::TEXT AS neighbourhood,
		s.geometry::geometry AS geometry,
        s.row_id
	FROM
		{{ ref('lookup_neighbourhoods') }} s
    
    {% if is_incremental() %}

    LEFT JOIN
        {{ this }} d    
    ON
        d.row_id = s.row_id
    WHERE
        d.row_id IS NULL
    
    {% endif %}
)
SELECT
    * 
FROM CTE_neighbourhoods
