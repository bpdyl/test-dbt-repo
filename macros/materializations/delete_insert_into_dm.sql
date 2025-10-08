{% materialization delete_insert_into_dm, adapter='snowflake' %}
    {# 
        OVERVIEW:
        Custom materialization for performing a delete and insert operation into the datamart table. This materialization
        handles pre-hooks, post-hooks, and executes delete and insert statements to update records
        in the target table based on the specified conditions.
        
        The parameters it uses are passed from the model inside the config block that uses this materialization.
        - target_table (string, optional): The target table to delete and insert data into. If not provided, defaults to the current model.
        - fact_cde (string): The fact code used for the delete condition.
        - delete_condition (string, optional): Additional condition for the delete statement when load type is DAILY.
        - load_type (string, optional): The type of load operation, either 'DAILY' or 'FULL'. Defaults to 'DAILY'.
        
        OUTPUTS:
        - Executes delete and insert operations on the target table.
        - Logs the delete and insert operation details.
        - Returns the target relation after the operations.
        
        EXAMPLE USAGE:
        {{ config(
            materialized='custom_delete_insert',
            target_table='DM_F_MEAS_FACT_ILD_B',
            schema='DM_MERCH',
            fact_cde = 'SLS_DSC',
            tags = ['dm_f_sls_dsc_meas_fact_ild'],
        ) }}
    #}

    {% if config.get('target_table') is not none %}
        {# -- Purpose: Create a relation object for the target table if specified in config, otherwise use the current model's relation -- #}
        {% set target_relation = adapter.Relation.create(
                database=this.database,
                schema=config.get('schema'),
                identifier=config.get('target_table')
            ) %}
    {% else %}
        {# -- Purpose: Use the current model's relation as the target if no target_table is specified -- #}
        {%- set target_relation = this -%}
    {% endif %}
    {% set load_type = var('load_type', 'DAILY') %}
    {# while passing the load_type parameter as None from python code, it is consumed as string here
    thus checking the str None value and assigning DAILY as default load type #}
    {% if load_type == 'None' %}
        {% set load_type =  'DAILY' %}
    {% endif %}
    {% set delete_condition = config.get('delete_condition') %}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% if load_type not in ['DAILY','FULL'] %}
        {{ exceptions.raise_compiler_error("Invalid LOAD_TYPE parameter value: "~load_type) }}
    {% endif %}    
    {{log('Load type: '~load_type,info=True)}}
    {% if load_type == 'FULL' %}
        {% set delete_query %}
            -- Purpose: Delete all records for the specified FACT_CDE from the target table (FULL load)
            DELETE FROM {{ target_relation }} 
            WHERE FACT_CDE = '{{ config.get("fact_cde") }}'
        {% endset %}
    {% elif load_type == 'DAILY' and delete_condition %}
        {% set delete_query %}
            -- Purpose: Delete records for the specified FACT_CDE and additional condition from the target table (DAILY load with condition)
            DELETE FROM {{ target_relation }}
            WHERE FACT_CDE = '{{ config.get("fact_cde") }}' 
            AND {{ delete_condition }}
        {% endset %}
    {% else %}
        {% set delete_query %}
            -- Purpose: Delete records for the specified FACT_CDE and current business date from the target table (DAILY load, default condition)
            DELETE FROM {{ target_relation }}
            WHERE FACT_CDE = '{{ config.get("fact_cde") }}' 
            AND POST_DT = '{{ robling_product.get_business_date() }}'
        {% endset %}
    {% endif %}
    {{log('Deleting the '~config.get("fact_cde")~" data from Datamart using query: \n",info=True)}}
    {% do run_query(delete_query) %}

    {% set columns_list = get_columns_in_query(sql) %}
    
    {% set insert_query  %}
        -- Purpose: Insert new records into the target table using the columns from the model's select statement
        INSERT INTO {{ target_relation }} (
        {%- for col in columns_list -%}
        {{ ", " if not loop.first else "" }}{{ col }}{{"\n"}}
        {%- endfor -%}
        )
        {#- keep select statement from the model as is -#}
        {{ sql }}
        
    {% endset %}
    
    {%- call statement('main') -%}
            {{ insert_query }}
    {%- endcall -%}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {{ adapter.commit() }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}