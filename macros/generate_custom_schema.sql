{% macro generate_schema_name(custom_schema_name, node) %}
    {# 
        OVERVIEW:
        Customizes the schema name generation for dbt models. This macro overrides the default dbt behavior
        to allow for custom schema naming logic. It uses the target schema as a base and appends the custom
        schema name if provided.
        
        INPUTS:
        - custom_schema_name (string): The configured value of schema in the specified node, or none if not supplied.
        - node (dict): The node that is currently being processed by dbt, containing metadata about the model.
        
        OUTPUTS:
        - Returns the schema name as a string, either the default schema or a combination of the default and custom schema.
    #}
    {% set default_schema = target.schema %}
    {% if custom_schema_name is none %}

        {{ default_schema }}

    {% else %}

        {{ custom_schema_name | trim }}

    {% endif %}

{% endmacro %}