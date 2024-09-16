  -- depends_on: {{ ref('stg_assessment') }}

select k_grade_race,
             student_grade,
             student_race
from  {{ref('stg_assessment')}}
    group by k_grade_race,
             student_grade,
             student_race
        