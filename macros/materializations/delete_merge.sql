{% materialization delete_merge, adapter='snowflake' %}
    {#
        OVERVIEW:
        Custom materialization for performing a delete-merge operation in Snowflake. This materialization handles pre-hooks,
        post-hooks, and executes a delete-merge process that first deletes records present in/ not in the source table and
        then merges the remaining records into the target table based on the specified fact fields.

        INPUTS:
        - target_table (string, optional): The target table to merge data into. If not provided, defaults to the current model.
        - key_columns (list): List of columns used for the matching condition (Primary keys).
        - delete_matching_keys (list) : List of columns used to identify matching records between the target table and the source table.
        - source_constraints (list) : List of constraints namely in order: source schema, source table, and delete constraint, to facilitate deletion from source.
        - maintenance_columns (list) : List of columns used for table maintenance.
        - hash_columns (list): List of columns that should be part of the hash check (all non-key columns).
        - insert_columns (list): List of columns that should be part of the insert statement.

        OUTPUTS:
        - Executes a delete-merge operation on the target table.
        - Logs the delete-merge operation details.
        - Returns the target relation after the delete-merge.
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
            {# execute the build sql #}
        {% call statement('main') %}
            {{ build_sql }}
        {% endcall %}

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

        {# Define key and hash columns. #}
        {% set source_relation = get_upstream_relations(this.name) | first %}
        {% set key_columns = config.get('unique_key') or [] %}
        {% set delete_matching_keys = config.get('delete_matching_keys') %}
        {% set delete_constraint = config.get('delete_constraint') %}
        {% set maintenance_columns = ['RCD_INS_TS','RCD_UPD_TS'] %}
        {% set hash_columns = [] %}

        {% for col in dest_columns %}
            {% if col.name not in maintenance_columns + key_columns %}
                {% do hash_columns.append(col.name) %}
            {% endif %}
        {% endfor %}
        {% set insert_columns = key_columns + hash_columns %}

        {# Build and run the merge query #}
        {% set delete_query = robling_product.generate_delete_statement(source_relation,key_columns,delete_matching_keys,delete_constraint) %}
        {% set merge_query = robling_product.generate_merge_statement(target_relation, key_columns, hash_columns, insert_columns) %}

        {# Execute the delete query in a main block #}
        {%- call statement('main') -%}
            {{ delete_query }}
        {%- endcall -%}
        {# Execute the merge query in a main block #}
        {{ log("Running merge query:\n" , info=True) }}
        {%- call statement('main') -%}
            {{ merge_query }}
        {%- endcall -%}

    {% endif %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}