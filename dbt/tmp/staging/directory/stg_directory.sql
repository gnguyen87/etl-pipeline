-- Returns a list of the columns from a relation, so you can then iterate in a for loop
{% set column_names = dbt_utils.get_filtered_columns_in_relation(from=ref('base_directory'), except=["state_leg_district_lower",
 "state_leg_district_upper", "zip4_location", "zip4_mailing", "updated_at" ]) %}

 with final as (
    select
         updated_at,   
         nullif(state_leg_district_lower,'') as state_leg_district_lower,
         nullif(state_leg_district_upper,'') as state_leg_district_upper,
         nullif(zip4_location,'') as zip4_location,
         nullif(zip4_mailing,'') as zip4_mailing,
         {% for column_name in column_names %}
        {{ column_name }}{% if not loop.last %}, {% endif %}
         {% endfor %}
    from {{ref("base_directory")}}
 )

 select * from final