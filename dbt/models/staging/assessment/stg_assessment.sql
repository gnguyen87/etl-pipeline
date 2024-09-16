 -- ## A list of column names to include in the `final` CTE with exceptions through a loop iteration 

{% set column_names = dbt_utils.get_filtered_columns_in_relation(from=ref('base_assessment'), except=["updated_at","econ_disadvantaged",
"foster_care", "homeless", "migrant", "military_connected", "sex", "lep", "fips", "race", "disability", "grade_edfacts" ]) %}


with 
	sex_code as (select * from {{ref('sex_code')}}),
	race_code as (select * from {{ref('race_code')}}),
	grade_code as (select * from {{ref('grade_code')}}),
	lang_prof_code as (select * from {{ref('lang_prof_code')}}),
	fip_state_code as (select * from {{ref('fip_state_code')}}),
	disability_code as (select * from {{ref('disability_code')}}),
    base as (select * from {{ ref('base_assessment') }}),


cross_walked as (

	select  base.*,
            s.student_sex,
            r.student_race,
            g.student_grade,
            l.lang_prof,
            f.state,
            d.student_disability
		from base
		left join  sex_code s
		using (sex)
		left join race_code r
		using (race)
		left join grade_code g
		using (grade_edfacts)
		left join lang_prof_code l
		using (lep)
		left join fip_state_code f
		using (fips)
		left join disability_code d
		using (disability)



),

final as (
    select updated_at,
        CASE 
            WHEN econ_disadvantaged = '99' THEN 'Total'
            ELSE econ_disadvantaged
        END AS econ_disadvantaged,
        CASE 
            WHEN foster_care = '99' THEN 'Total'
            ELSE foster_care
        END AS foster_care,
        CASE 
            WHEN homeless = '99' THEN 'Total'
            ELSE homeless
        END AS homeless,
        CASE 
            WHEN migrant = '99' THEN 'Total'
            ELSE migrant
        END AS migrant,
        CASE 
            WHEN military_connected = '99' THEN 'Total'
            ELSE military_connected
        END AS military_connected,
		student_sex,
		student_race,
		student_grade,
		lang_prof,
		state,
		student_disability,
        {% for column_name in column_names %}
        {{ column_name }}{% if not loop.last %},{% endif %}
        {% endfor %}
		from cross_walked
),

-- ## Assessment grain: `leaid`, `student_race`, `student_grade`, `year` 
-- ## `year` is left out of surrogate key formations
keyed as (
  select
      {{ dbt_utils.generate_surrogate_key(
              ['student_race',
			  	'student_grade'
              ]
          ) }} as k_grade_race,
      {{ dbt_utils.generate_surrogate_key(
            ['leaid'
            ]
        ) }} as k_school_district,
    *
  from final
)



select * from keyed 
