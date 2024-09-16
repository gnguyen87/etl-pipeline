-- ## Dynamic JSON Extraction 
-- ## Code source: https://edanalytics.slite.com/app/docs/M_ykCBCOczcVwV 
-- ## Code source: https://www.postgresql.org/docs/current/datatype-json.html

{% set sql_statement %}
    SELECT DISTINCT jsonb_object_keys(v) AS vars
	from {{source("raw_jm", "enrollment")}}
{% endset %}

{% set unique_fields = dbt_utils.get_query_results_as_dict(sql_statement) %}

with original_data as (
    SELECT 
        updated_at,
    {% for field in unique_fields['vars'] %}
        (v ->> '{{ field }}') as {{ field }}
        {% if not loop.last %} , {% endif %}
    {% endfor %}
    FROM  {{source("raw_jm", "enrollment")}}
)

select 
    updated_at,
    enrollment::integer,
    fips,
    grade,
    leaid,
    race,
    sex,
    year
from original_data
