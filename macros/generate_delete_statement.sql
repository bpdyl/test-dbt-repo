{% macro generate_delete_statement(source_relation,key_columns,delete_matching_keys,delete_constraint) %}
    {#
        OVERVIEW:
        Generates a SQL delete statement for deleting data from target table.
        This macro is designed to be used within a custom materialization and cannot be used independently.

        INPUTS:
        - source_relation (table): Fully qualified name of upstream model used to identify records that should be deleted from the target table.
        - key_columns (list): List of columns used for the matching condition (Primary keys).
        - delete_matching_keys (list): List of columns that should be part of the delete statement.
        - delete_constraint (string): Constraint to determine the type of deletion (in tmp/ not in tmp).

        OUTPUTS:
        - Returns a SQL delete statement string.
        - Intended for use within a custom materialization.
    #}
    {% if delete_constraint == "NOT_IN_SRC" %}
        DELETE FROM {{ this }} AS TGT
        WHERE ({{ delete_matching_keys | join(', ') }}) NOT IN
        (SELECT {{ delete_matching_keys | join(', ') }} FROM {{ source_relation }})
    {% else %}
        DELETE FROM {{ this }} AS TGT
        WHERE ({{ delete_matching_keys | join(', ') }}) IN
        (SELECT {{ delete_matching_keys | join(', ') }} FROM {{ source_relation }})
    {% endif %}
{% endmacro %}

