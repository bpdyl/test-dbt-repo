{% macro select_stg_source(
    base_source_name,
    curr_day,
    chg_suffix = '_CHG',
    switch_date = '2023-12-28'
) %}
    {#
        OVERVIEW:
        Dynamically selects between LND and LND_CHG schemas based on business date, base source name 
        and switch date specified.

        NOTE:
        - This macro is used to switch between LND and LND_CHG schemas based on business date.
        - It is applicable for robling product only.
        - It ensures that first load in daily batch is done using LND schema and second load in daily batch is done using LND_CHG schema.
        
        PARAMETERS:
        - base_source_name: The base source name (e.g. 'SALES_AND_RETURNS_SRC')
        - chg_suffix: Suffix for change data capture source (default '_CHG')
        - get_business_date: Function reference to get business date
        - switch_date: The date when we switch to CDC source
        
        OUTPUT:
        src_name (string): Selected source name
    #}
    
    {% set src_name = base_source_name ~ chg_suffix if curr_day == switch_date else base_source_name %}

    {{ return(src_name) }}
{% endmacro %}