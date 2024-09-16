-- depends_on: {{ ref('base_directory') }}

-- ## A list of column names to include in the `null` CTE with exceptions through a loop iteration 
-- ## The fields in the exception lists contain blank strings and to be filled with `NULL`
{% set column_names_null = dbt_utils.get_filtered_columns_in_relation(from=ref('base_directory'), except=["state_leg_district_lower",
 "state_leg_district_upper", "zip4_location", "zip4_mailing", "updated_at" ]) %}

-- ## A list of column names to include in the `final` CTE with exceptions through a loop iteration 
-- ## The fields in the exception lists are to be replaced with new fields that have been crosswalked through seeds via join
{% set column_names_final = dbt_utils.get_filtered_columns_in_relation(from=ref('base_directory'), except=["updated_at","agency_charter_indicator", 
"agency_level", "agency_type", "boundary_change_indicator", "cbsa_type", "highest_grade_offered", "lowest_grade_offered", "fips", "leaid",
"urban_centric_locale", "grade_offered_ccd", "cbsa"]) %}

 with nullfied as (
    select
         updated_at,   
         nullif(state_leg_district_lower,'') as state_leg_district_lower,
         nullif(state_leg_district_upper,'') as state_leg_district_upper,
         nullif(zip4_location,'') as zip4_location,
         nullif(zip4_mailing,'') as zip4_mailing,
         {% for column_name in column_names_null %}
        {{ column_name }}{% if not loop.last %}, {% endif %}
         {% endfor %}
    from {{ref("base_directory")}}
 ),

 agency_charter_code as ( 
   select * from {{ref('agency_charter_code')}}
 ),

 agency_level_code as (
   select * from {{ref('agency_level_code')}}

 ),

 agency_type_code as (
   select * from {{ref('agency_type_code')}}

 ),

 boundary_change_code as (
   select * from {{ref('boundary_change_code')}}
 ),

 cbsa_code as (
   select * from {{ref('cbsa_code')}}
 ),

 grade_offered_code as (
   select * from {{ref('grade_offered_code')}}
 ),

 urban_centric_locale_code as (
   select * from {{ref('urban_centric_locale_code')}}
 ),

 fip_state_code as (
  select * from {{ref('fip_state_code')}}
 ),

 cross_walked as (
   select n.*,
        a1.agency_charter_type,
        a2.agency_ed_level,
        a3.agency_ed_type,
        b1.boundary_change_indic,
        c1.meaning as cbsa_metropol_micropol_type,
        g1.meaning AS highest_grade_level_offered,
        g2.meaning AS lowest_grade_level_offered,
        f1.state,
        u1.urban_centric_degree
   from nullfied n
   left join agency_charter_code a1
   using (agency_charter_indicator)
   left join agency_level_code a2
   using (agency_level)
   left join agency_type_code a3 
   using (agency_type)
   left join boundary_change_code b1
   using (boundary_change_indicator)
   left join cbsa_code c1
   on c1.cbsa_type = n.cbsa_type
   left join grade_offered_code g1
   on g1.grade_offered_ccd = n.highest_grade_offered
    left join grade_offered_code g2
   on g2.grade_offered_ccd = n.lowest_grade_offered
   left join urban_centric_locale_code u1
   using (urban_centric_locale)
   left join fip_state_code f1
   on f1.fips = n.fips
 ),



 final as (
   select updated_at,
      leaid,
      state,
      agency_charter_type,
      agency_ed_level,
      agency_ed_type,
      boundary_change_indic,
      cbsa_metropol_micropol_type,
      highest_grade_level_offered,
      lowest_grade_level_offered,
      urban_centric_degree,
      {% for column_name in column_names_final %}
      {{column_name}} {% if not loop.last%}, {% endif%}
      {% endfor%}
      from cross_walked

 )

 select * from final
 