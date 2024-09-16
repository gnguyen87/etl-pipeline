  -- depends_on: {{ ref('stg_directory') }}

-- ## A list of field names from `stg_directory` except for those that hold no values to include in the `stg_directory_with_no_null_fields` CTE via a loop
{% set column_names = dbt_utils.get_filtered_columns_in_relation(from=ref("stg_directory"),except=["updated_at","agency_ed_level",  "cmsa", "migrant_students", "necta",
"other_staff_fte", "school_counselors_fte",  "school_psychologists_fte", "lea_staff_total_fte", "school_staff_total_fte", "staff_total_fte", "state_leg_district_lower", 
"state_leg_district_upper", "support_staff_stu_wo_psych_fte", "zip4_location", "zip4_mailing", "zip_mailing"]) %}

-- ## A list of field names from `stg_directory` except for `year` & those that hold no values  to include in the `keyed` CTE via a loop
-- ## Does not include year due to a join for most updated year for each school district
{% set column_names_final = dbt_utils.get_filtered_columns_in_relation(from=ref("stg_directory"),except=["updated_at","agency_ed_level",  "cmsa", "migrant_students", "necta",
"other_staff_fte", "school_counselors_fte",  "school_psychologists_fte", "lea_staff_total_fte", "school_staff_total_fte", "staff_total_fte", "state_leg_district_lower", 
"state_leg_district_upper", "support_staff_stu_wo_psych_fte", "zip4_location", "zip4_mailing", "zip_mailing", "year"]) %}


with stg_directory_with_no_null_fields as (
	select 
        {% for column_name in column_names %}
        {{ column_name }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ref('stg_directory')}}
), 
 
 -- ## A CTE for each school district leaid and its most updated directory's
most_updated_directory_leaid_year as (
	select leaid, max(year) as max
	from stg_directory_with_no_null_fields
	group by leaid
), 

final as (
    select 
        stg.*
	from stg_directory_with_no_null_fields stg
	join most_updated_directory_leaid_year myear
	on stg.leaid = myear.leaid
	and stg.year = myear.max
    
),

keyed as (
  select
        {{ dbt_utils.generate_surrogate_key(
            ['leaid'] ) }} 
            as k_school_district,
        {% for column_name in column_names_final %}
        {{ column_name }}{% if not loop.last %},{% endif %}
        {% endfor %}
        
  from final
)


select * from keyed


