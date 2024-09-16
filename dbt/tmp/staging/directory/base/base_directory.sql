


{% set sql_statement %}
    SELECT DISTINCT jsonb_object_keys(v) AS vars
	from {{source("raw_jm", "directory")}}
{% endset %}

{% set unique_fields = dbt_utils.get_query_results_as_dict(sql_statement) %}

with original_data as (
    SELECT 
        updated_at,
    {% for field in unique_fields['vars'] %}
        (v ->> '{{ field }}') as {{ field }}
        {% if not loop.last %} , {% endif %}
    {% endfor %}
    FROM  {{source("raw_jm", "directory")}}
)

select * 
from original_data
