


{% set sql_statement %}
    SELECT DISTINCT jsonb_object_keys(v) AS vars
	from {{source("raw_jm","assessment")}}
{% endset %}

{% set unique_fields = dbt_utils.get_query_results_as_dict(sql_statement) %}

WITH unnested_data as (
    SELECT 
        updated_at,
    {% for field in unique_fields['vars'] %}
        (v ->> '{{ field }}') as {{ field }}
        {% if not loop.last %} , {% endif %}
    {% endfor %}
     FROM {{source("raw_jm","assessment")}}

),

final as (
    SELECT 
        updated_at,
        disability,
        econ_disadvantaged,
        fips,
        foster_care,
        grade_edfacts,
        homeless,
        leaid,
        leaid_num,
        lea_name,
        lep,
        migrant, 
        military_connected,
        race,
        sex,
        year,
        math_test_num_valid::float,
        math_test_pct_prof_high::float,
        math_test_pct_prof_low::float,
        math_test_pct_prof_midpt::float,
        read_test_pct_prof_high::float,
        read_test_num_valid::float,
        read_test_pct_prof_low::float,
        read_test_pct_prof_midpt::float
    FROM unnested_data
        )

SELECT * from final
