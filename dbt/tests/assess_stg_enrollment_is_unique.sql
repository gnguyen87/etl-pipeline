with stg_enrollment as (
    select * from {{ ref('stg_enrollment')}}
)

--## Test to make sure each school district's enrollment results by grade, race, and year is unique 
select 
    count(*) 
from stg_enrollment 
group by k_school_district,k_grade_race,year
having count(*) >1