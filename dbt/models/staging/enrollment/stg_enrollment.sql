-- ## A list of column names to include in the `final` CTE through a loop iteration with exceptions

{% set column_names = dbt_utils.get_filtered_columns_in_relation(from=ref("base_enrollment"),except=["updated_at","fips",  "race", "sex" ]) %}

with 
    base as (
        select * from {{ ref('base_enrollment') }}
    ),
    sex_code as (
        select * from {{ref('sex_code')}}
    ),
    state_code as (
        select * from {{ref('fip_state_code')}}
    ),
    grade as (
        select * from {{ref('grade_offered_code')}}
    ), 
    race as (
        select * from {{ref('race_code')}}
    ),

cross_walked as (
   select base.updated_at,
        base.year,
        base.leaid,
        base.enrollment,
        g.meaning as grade,
        s.student_sex,
        r.student_race,
        st.state
		from base
		left join  sex_code s
		using (sex)
        left join grade g
        on base.grade = g.meaning
		left join race r
		using (race)
		left join state_code st
		using (fips)
),

final as (
    select updated_at,
        {% for column_name in column_names %}
        {{ column_name }}{% if not loop.last %},{% endif %}
        {% endfor %},
        state,
        student_race,
        student_sex
    from cross_walked


),

-- ## Enrollment grain: `leaid`, `grade`, `sex`, `year`
-- ## `year` is left out of surrogate key formations
keyed as (
  select
      {{ dbt_utils.generate_surrogate_key(
              ['student_race',
			  	    'grade'
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

