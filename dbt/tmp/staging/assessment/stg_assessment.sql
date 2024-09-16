-- Returns a list of the columns from a relation, so you can then iterate in a for loop
{% set column_names = dbt_utils.get_filtered_columns_in_relation(from=ref('base_assessment'), except=["updated_at", "disability","econ_disadvantaged",
"foster_care", "homeless", "lep", "migrant", "military_connected", "sex" ]) %}

with final as (
    select
        updated_at,
        case 
            when disability = '99' then 'Total'
            else disability
        end as disability,
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
            WHEN lep = '99' THEN 'Total'
            ELSE lep
        END AS lep,
        CASE 
            WHEN migrant = '99' THEN 'Total'
            ELSE migrant
        END AS migrant,
        CASE 
            WHEN military_connected = '99' THEN 'Total'
            ELSE military_connected
        END AS military_connected,
        CASE 
            WHEN sex = '99' THEN 'Total'
            ELSE sex
        END AS sex,
        {% for column_name in column_names %}
        {{ column_name }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ref("base_assessment")}}
)

select * from final