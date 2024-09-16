  
-- depends_on: {{ ref('stg_enrollment') }}

select   k_grade_race,
         k_school_district,
         state,
         year,
         grade,
         student_race,
         enrollment
from {{ ref('stg_enrollment') }}
order by year
