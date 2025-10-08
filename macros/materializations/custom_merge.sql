{% materialization custom_merge, adapter='snowflake' %}
    {# 
        OVERVIEW:
        Custom materialization for performing a merge operation in Snowflake. This materialization
        handles pre-hooks, post-hooks, and executes a merge statement to update or insert records
        into the target table based on the specified key and hash columns.
        
        INPUTS:
        - target_table (string, optional): The target table to merge data into. If not provided, defaults to the current model.
        - key_columns (list): List of columns used for the matching condition (Primary keys).
        - hash_columns (list): List of columns that should be part of the hash check (all non-key columns).
        - insert_columns (list): List of columns that should be part of the insert statement.
        - model_name (string, optional): Used to determine the model for which the merge statement is used.
        
        OUTPUTS:
        - Executes a merge operation on the target table.
        - Logs the merge operation details.
        - Returns the target relation after the merge.
    #}

    {% if config.get('target_table') is not none %}
        {% set target_relation = adapter.Relation.create(
                database=this.database,
                schema=config.get('schema'),
                identifier=config.get('target_table')
            ) %}
    {% else %}
        {# Create the target model if it does not exist #}
        {%- set existing_relation = load_cached_relation(this) -%}
        {%- set target_relation = this.incorporate(type='table') -%}
        {% if existing_relation is none %}
            {% set build_sql = get_create_table_as_sql(False, target_relation, sql) %}
            {% call statement("main") %}
                {{ build_sql }}
            {% endcall %}
        {% endif %}
    {% endif %}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}
    
    {# Define key and hash columns. #}
    {% set key_columns = config.get('key_columns') %}
    {% set hash_columns = config.get('hash_columns') %}
    {% set insert_columns = config.get('insert_columns') %}
    {% set model_name = config.get('model_name') %}
    
    {# Build and run the merge query #}
    {% if model_name == "Inventory" %}
        {% set merge_query = robling_product.generate_merge_statement_inv(target_relation, key_columns) %}
    {% else %}
        {% set merge_query = robling_product.generate_merge_statement(target_relation, key_columns, hash_columns, insert_columns) %}
    {% endif %}
    {{ log("Running merge query:\n" , info=True) }}
    {# Execute the merge query in a main block #}
    {%- call statement('main') -%}
        {{ merge_query }}
    {%- endcall -%}
    
    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}
    
    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
