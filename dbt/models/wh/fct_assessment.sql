  -- depends_on: {{ ref('stg_assessment') }}

with stg_assessment as (
    select k_grade_race,
        k_school_district,
        year,
        student_grade,
        student_race,
        math_test_num_valid,
        math_test_pct_prof_high,
        math_test_pct_prof_low,
        math_test_pct_prof_midpt,
        read_test_num_valid,
        read_test_pct_prof_high,
        read_test_pct_prof_low,
        read_test_pct_prof_midpt
    from {{ ref('stg_assessment' )}}
),

-- ## Pivot the table longer by assessment subject 
-- ## Code source: https://www.reddit.com/r/learnSQL/comments/sgqmi2/postgres_pivot_from_wide_to_long/

pivot_longer_by_subject as (

    select
        k_grade_race,
        k_school_district,
        student_grade,
        student_race,
        year,
        'Reading' as subject,
        read_test_num_valid as num_students_assessed,
        read_test_pct_prof_low as pct_proficient_low_end,
        read_test_pct_prof_high as pct_proficient_high_end,
        read_test_pct_prof_midpt as pct_proficient_midpoint
    from
        stg_assessment
    where
        read_test_num_valid >= 0
        and read_test_pct_prof_low >= 0
        and read_test_pct_prof_high >= 0
        and read_test_pct_prof_midpt >= 0
    union all
    select
        k_grade_race,
        k_school_district,
        student_grade,
        student_race,
        year,
        'Math' as subject,
        math_test_num_valid as num_students_assessed,
        math_test_pct_prof_low as pct_proficient_low_end,
        math_test_pct_prof_high as pct_proficient_high_end,
        math_test_pct_prof_midpt as pct_proficient_midpoint
    from
        stg_assessment
    where
        math_test_num_valid >= 0
        and math_test_pct_prof_low >= 0
        and math_test_pct_prof_high >= 0
        and math_test_pct_prof_midpt >= 0
)

select  * 
from pivot_longer_by_subject
order by year