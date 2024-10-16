{% macro generate_seq_id(id_key) %}
    {% if is_incremental() %}
        (SELECT COALESCE(MAX({{ id_key }}), 0) FROM {{ this }}) + 
    {% endif %}
    ROW_NUMBER() OVER(ORDER BY NULL) AS {{ id_key }}
{% endmacro %}
