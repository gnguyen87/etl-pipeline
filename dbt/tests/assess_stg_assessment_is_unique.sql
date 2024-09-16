with stg_assessment as (
    select * from {{ ref('stg_assessment')}}
)

--## Test to make sure each school district's assessment results by  grade, race, and year is unique 

select 
    count(*) 
from stg_assessment sa 
group by k_school_district,k_grade_race,year
having count(*) >1