{% macro generate_hash_expression(alias, columns) %}
    {# 
        OVERVIEW:
        Generates a hash expression for a given set of columns in a table.
        
        INPUTS:
        - alias (str): The alias of the table.
        - columns (list): The list of columns to generate the hash expression for.
        
        OUTPUTS:
        - Returns a hash expression for the given set of columns.
    #}
    HASH(
    {%- for col in columns -%}
        {{ alias }}.{{ col }}{{ ", " if not loop.last else "" }}
    {%- endfor -%}
    )
{% endmacro %}


{% macro generate_merge_statement(target_relation, key_columns, hash_columns, insert_columns) %}
    {# 
        OVERVIEW:
        Generates a SQL merge statement for merging data from a source dataset into a target table.
        This macro is designed to be used within a custom materialization and cannot be used independently.
        
        INPUTS:
        - target_relation (relation): Fully qualified target table (db.schema.table).
        - key_columns (list): List of columns used for the matching condition (Primary keys).
        - hash_columns (list): List of columns that should be part of the hash check (all non-key columns).
        - insert_columns (list): List of columns that should be part of the insert statement.
        
        OUTPUTS:
        - Returns a SQL merge statement string.
        - Intended for use within a custom materialization.
    #}

    MERGE INTO {{ target_relation }} AS TGT
    USING (
        {{ sql }}
    ) AS SRC
    ON 
    {% if not key_columns %}
    --if there are no primary key columns then this condition is always false so we perform inserts only 
    1=0 
    {% else %}
    (
        {%- for key in key_columns -%}
            tgt.{{ key }} = src.{{ key }}{{ " and " if not loop.last else "" }}
        {%- endfor -%}
    )
    {% endif %}
    WHEN MATCHED AND (
        {{ robling_product.generate_hash_expression('TGT', hash_columns) }} <> {{ robling_product.generate_hash_expression('SRC', hash_columns) }}
    )
    THEN UPDATE SET 
        {% for col in hash_columns %}
            TGT.{{ col }} = SRC.{{ col }}{{ ", " if not loop.last else "" }}
        {% endfor %},
        TGT.RCD_UPD_TS = CURRENT_TIMESTAMP::TIMESTAMP_NTZ
    WHEN NOT MATCHED THEN INSERT (
        {% for col in insert_columns %}
            {{ col }}{{ ", " if not loop.last else "" }}
        {% endfor %},
        RCD_INS_TS, RCD_UPD_TS
    )
    VALUES (
        {%- for col in insert_columns -%}
            SRC.{{ col }}{{ ", " if not loop.last else "" }}
        {%- endfor -%},
        CURRENT_TIMESTAMP::TIMESTAMP_NTZ, CURRENT_TIMESTAMP::TIMESTAMP_NTZ
    )
{% endmacro %}
