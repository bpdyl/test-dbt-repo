{% macro get_coalesced_surrogate_key(sk_column, hash_column)
%}
    {#
        INPUTS:
        - sk_column: The surrogate key column name.
        - hash_column: The column to generate the surrogate key for. 
        (Pass as string: e.g., 'SRC.ITM_ID')
        
        OUTPUT:
        - Returns a COALESCE SQL expression depending on SURROGATE_KEY_TYPE:
            - If SURROGATE_KEY_TYPE = 'MD5', generate MD5 hash surrogate key using dbt_utils if sk_column is null.
            - If SURROGATE_KEY_TYPE = 'Sequence', simply fallback to numeric surrogate key logic if sk_column is null.     
    #}
    {% set surrogate_key_type = var("SURROGATE_KEY_TYPE") %}

    {% if surrogate_key_type == "MD5" %}
        {% set sk_gen_logic %}
    COALESCE({{sk_column}},
    {{ dbt_utils.generate_surrogate_key([hash_column]) }})   
        {% endset %}

    {% elif surrogate_key_type == "Sequence" %}
        {% set sk_gen_logic %}
    COALESCE({{sk_column}}, -1) 
        {% endset %}
    
    {% else %}
        {% do exceptions.raise_compiler_error("Invalid SURROGATE_KEY_TYPE: " ~ surrogate_key_type ~ ". Must be either MD5 or Sequence.") %}
    {% endif %}

    {{ return(sk_gen_logic) }}

{% endmacro %}


{% macro get_key_with_fallback_value(key_column)
%}
    {#
        INPUTS:
        - key_column: The key column name with fallback value needed.
        (Pass as string: e.g., 'ITM.DIV_KEY')
        
        OUTPUT:
        - Returns a COALESCE expression for the key with default fallback values depending on surrogate key type.       
    #}
    {% set surrogate_key_type = var("SURROGATE_KEY_TYPE") %}

    {% if surrogate_key_type == "MD5" %}
        {% set fallback_expression %}
    COALESCE({{key_column}}, '-1')   
        {% endset %}

    {% elif surrogate_key_type == "Sequence" %}
        {% set fallback_expression %}
    COALESCE({{key_column}}, -1) 
        {% endset %}
    
    {% else %}
        {% do exceptions.raise_compiler_error("Invalid SURROGATE_KEY_TYPE: " ~ surrogate_key_type ~ ". Must be either MD5 or Sequence.") %}
    {% endif %}

    {{ return(fallback_expression) }}

{% endmacro %}