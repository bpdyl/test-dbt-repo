{% materialization insert_overwrite, adapter='snowflake' %}
{#
    Custom materialization: insert_overwrite (Snowflake)

    OVERVIEW:
    - This materialization is atomic and is a safer alternative to truncate + insert.
    - If the target relation does not exist, it will be created with a CTAS statement.
    - If the target relation exists, it will use an 'INSERT OVERWRITE' statement to atomically
    replace the data in the target table.

    USAGE:
    In model config block:

    {{ config(
        materialized='insert_overwrite',
        transient=false
    ) }}

#}


    {# relations #}
    {%- set target_relation = this -%}
    {%- set existing_relation = load_relation(this) -%}
    {%- set temp_relation = make_temp_relation(target_relation)-%}

    -- configs
    {%- set unique_key = config.get('unique_key') -%}
    {%- set full_refresh_mode = (should_full_refresh()  or existing_relation.is_view) -%}
    {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}

    -- `BEGIN` will be committed at the end of the materialization, if there are no exceptions.
    -- The commit is handled by the `adapter.commit()` call at the end.
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {# build model sql #}
    {% if existing_relation is none %}
        {# First run: The table does not exist, so we create it. #}
        {% set build_sql = create_table_as(False, target_relation, sql) %}

    {% else %}
        {% do run_query(get_create_table_as_sql(True, temp_relation, sql)) %}
        {% set relation_for_indexes = temp_relation %}
        {% set contract_config = config.get('contract') %}
        {% if not contract_config or not contract_config.enforced %}
        {% do adapter.expand_target_column_types(
                from_relation=temp_relation,
                to_relation=target_relation) %}
        {% endif %}
        {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
        {% set dest_columns = process_schema_changes(on_schema_change, temp_relation, existing_relation) %}
        {% if not dest_columns %}
        {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
        {% endif %}
        {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
        {# Subsequent runs: The table exists, so we atomically replace the data. #}
        {% set build_sql %}
            --following statement overwrites the existing data from the target table (truncate + insert) in a single transaction
            INSERT OVERWRITE INTO {{ target_relation }} (
                {{ dest_cols_csv }}
            )
            SELECT {{dest_cols_csv}} FROM {{ temp_relation }}
        {% endset %}

    {% endif %}

    {# execute the build sql #}
    {% call statement('main') %}
        {{ build_sql }}
    {% endcall %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {% do persist_docs(target_relation, model) %}

    {% if config.get('grants') %}
    {% do apply_grants(target_relation, config.get('grants')) %}
    {% endif %}

    {{ adapter.commit() }}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
