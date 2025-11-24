{% macro get_recon_checkpoint() %}
    {#
        OVERVIEW:
        This macro retrieves the checkpoint values for each source system from the DW_DWH.DWH_D_RECON_CHKPNT_LU table.
        It returns a dictionary where the keys are the source systems and the values are the corresponding checkpoint values.

        INPUTS:
        - None

        OUTPUTS:
        - A dictionary with source systems as keys and checkpoint values as values
    #}
    {% if execute %}
        {% set query %}
            SELECT SRC_SYSTEM,CHKPNT FROM DW_DWH.DWH_D_RECON_CHKPNT_LU
        {% endset %}
        {% set checkpoint = {} %}
        {% set query_result = run_query(query) %}
        {# {% set checkpoint[query_result.columns[0].values()[0]] = query_result.columns[1].values()[0] %}  #}
        {% for row in query_result.rows %}
            {% do checkpoint.update({ row[0]: row[1] }) %}
        {% endfor %}
        {{ return(checkpoint) }}
    {% endif %}
{% endmacro %}

{% macro check_recon_variance(fact_typ,curr_day, recon_step, validation_range, format_description, recon_post_dt_filter, no_data_warning=False) %}
    {# 
        OVERVIEW:
        This macro performs data reconciliation checks between two source systems for a given fact type.
        It queries the reconciliation table (DW_DWH.DWH_F_RECON_LD) and computes variance percentages
        for FACT_QTY, FACT_CST_LCL, and FACT_RTL_LCL. If any variance falls outside the acceptable range,
        it raises an exception.
        
        INPUTS:
        - fact_typ (string): The fact type to check (e.g., 'Sales', 'Inventory')
        - curr_day (date): Current business date for the reconciliation check
        - recon_step (integer): Reconciliation step identifier:
            0 = Compare LND and STG
            1 = Compare STG_V and DWH
            2 = Compare DWH and DM
        - validation_range (list): Two-element list containing the lower and upper acceptable variance 
                                percentage bounds (e.g., [-1, 1] for ±1%)
        - format_description (string): A comma-separated list of format descriptions for the columns
        
        OUTPUTS:
        - Prints reconciliation table results
        - Logs reconciliation status
        - Raises exception if variances are outside acceptable range
    #}

    {% if recon_step == 0 %}
        {% set src_system1 = 'LND' %}
        {% set src_system2 = 'STG_V' %}
    {% elif recon_step==1 %}
        {% set src_system1 = 'STG_V' %}
        {% set src_system2 = 'DWH' %}
    {% elif recon_step==2 %}
        {% set src_system1 = 'DWH' %}
        {% set src_system2 = 'DM' %}
    {% else %}
        {{ exceptions.raise_compiler_error("Invalid recon step passed. Valid options are 0,1 and 2") }}
    {% endif %}
    {# Unpack the acceptable range (assumed to be a two-element list) #}
    {% set lower = validation_range[0] | float %}
    {% set upper = validation_range[1] | float %}

    {# Build the query by replacing source system placeholders using Jinja templating #}
    {% set query %}
        SELECT
          COALESCE(SUM(CASE WHEN SRC_SYSTEM='{{ src_system1 }}' THEN FACT_QTY ELSE 0 END),0) AS {{ src_system1 }}_UNT,
          COALESCE(SUM(CASE WHEN SRC_SYSTEM='{{ src_system2 }}' THEN FACT_QTY ELSE 0 END),0) AS {{ src_system2 }}_UNT,
          COALESCE((SUM(CASE WHEN SRC_SYSTEM='{{ src_system1 }}' THEN FACT_QTY ELSE 0 END) - SUM(CASE WHEN SRC_SYSTEM='{{ src_system2 }}' THEN FACT_QTY ELSE 0 END)),0) AS DIFF_UNT,
          COALESCE(DIV0(
             (SUM(CASE WHEN SRC_SYSTEM='{{ src_system1 }}' THEN FACT_QTY ELSE 0 END) - 
              SUM(CASE WHEN SRC_SYSTEM='{{ src_system2 }}' THEN FACT_QTY ELSE 0 END)
             ),
             SUM(CASE WHEN SRC_SYSTEM='{{ src_system1 }}' THEN FACT_QTY ELSE 0 END)
          ),0)*100 AS VAR_PERCENT_UNT,
          COALESCE(SUM(CASE WHEN SRC_SYSTEM='{{ src_system1 }}' THEN FACT_CST_LCL ELSE 0 END),0) AS {{ src_system1 }}_CST,
          COALESCE(SUM(CASE WHEN SRC_SYSTEM='{{ src_system2 }}' THEN FACT_CST_LCL ELSE 0 END),0) AS {{ src_system2 }}_CST,
          COALESCE((SUM(CASE WHEN SRC_SYSTEM='{{ src_system1 }}' THEN FACT_CST_LCL ELSE 0 END) - SUM(CASE WHEN SRC_SYSTEM='{{ src_system2 }}' THEN FACT_CST_LCL ELSE 0 END)),0) AS DIFF_CST,
          COALESCE(DIV0(
             (SUM(CASE WHEN SRC_SYSTEM='{{ src_system1 }}' THEN FACT_CST_LCL ELSE 0 END) - 
              SUM(CASE WHEN SRC_SYSTEM='{{ src_system2 }}' THEN FACT_CST_LCL ELSE 0 END)
             ),
             SUM(CASE WHEN SRC_SYSTEM='{{ src_system1 }}' THEN FACT_CST_LCL ELSE 0 END)
          ),0)*100 AS VAR_PERCENT_CST,
          COALESCE(SUM(CASE WHEN SRC_SYSTEM='{{ src_system1 }}' THEN FACT_RTL_LCL ELSE 0 END),0) AS {{ src_system1 }}_RTL,
          COALESCE(SUM(CASE WHEN SRC_SYSTEM='{{ src_system2 }}' THEN FACT_RTL_LCL ELSE 0 END),0) AS {{ src_system2 }}_RTL,
          COALESCE((SUM(CASE WHEN SRC_SYSTEM='{{ src_system1 }}' THEN FACT_RTL_LCL ELSE 0 END) - SUM(CASE WHEN SRC_SYSTEM='{{ src_system2 }}' THEN FACT_RTL_LCL ELSE 0 END)),0) AS DIFF_RTL,
          COALESCE(DIV0(
             (SUM(CASE WHEN SRC_SYSTEM='{{ src_system1 }}' THEN FACT_RTL_LCL ELSE 0 END) - 
              SUM(CASE WHEN SRC_SYSTEM='{{ src_system2 }}' THEN FACT_RTL_LCL ELSE 0 END)
             ),
             SUM(CASE WHEN SRC_SYSTEM='{{ src_system1 }}' THEN FACT_RTL_LCL ELSE 0 END)
          ),0)*100 AS VAR_PERCENT_RTL
        FROM DW_DWH.DWH_F_RECON_LD
        WHERE FACT_TYP = '{{ fact_typ }}'
        {% if recon_post_dt_filter %}
            {#- If the custom post date filter is provided, use it -#}
            --filtering the reconciliation data based on the custom post date filter provided in the configuration 
            AND {{ recon_post_dt_filter }}
        {% else %}
            {#- use the default filter of LOAD_DT = curr_day for deletion -#}
            --filtering the reconciliation data based on the default filter: LOAD_DT = CURR_DAY 
            AND LOAD_DT = '{{ curr_day }}'
        {% endif %}
        
    {% endset %}

    {% if execute %}
        {% set checkpoint = robling_product.get_recon_checkpoint() %}
        {{ log("Running variance check query:\n", info=True) }}
        {% set result = run_query(query) %}
        {{ log("Result length: "~ (result | length) ~" and result: "~result, info=True) }}
        {% if not result or result | length == 0 %}
            {{log('No data warning above: '~no_data_warning,info=True)}}
            {# If no data found, handle based on no_data_warning flag. If true, emit a JSON result with status 'no_data' and do NOT raise an error. Otherwise raise as before. #}
            {% if no_data_warning ==True %}
                {{log('No data warning here: '~no_data_warning,info=True)}}
                {% set json_result = {
                    "fact_type": fact_typ,
                    "current_day": curr_day | string,
                    "checkpoint": checkpoint,
                    "source_systems": {
                        "system1": 'LND' if recon_step == 0 else ('STG_V' if recon_step == 1 else 'DWH'),
                        "system2": 'STG_V' if recon_step == 0 else ('DWH' if recon_step == 1 else 'DM')
                    },
                    "format_description": format_description,
                    "validation_range": {
                        "lower": validation_range[0],
                        "upper": validation_range[1]
                    },
                    "results": {
                        "unt": {"system1_value": 0, "system2_value": 0, "difference": 0, "variance_percent": 0},
                        "cst": {"system1_value": 0, "system2_value": 0, "difference": 0, "variance_percent": 0},
                        "rtl": {"system1_value": 0, "system2_value": 0, "difference": 0, "variance_percent": 0}
                    },
                    "status": "no_data",
                    "no_data_warning": true,
                    "out_of_range_columns": [] ,
                    "log_file_name": var('log_file_name') if var('log_file_name') is defined else ''
                } %}
                {{ log("RECON_JSON_RESULT: " ~ json_result | tojson, info=True) }}
                {{ log("No reconciliation data found for " ~ fact_typ ~ " on " ~ curr_day ~ ". Emitting no-data warning JSON and continuing.", info=True) }}
                {{ return(json_result) }}
            {% else %}
                {{ exceptions.raise_compiler_error("No reconciliation data found for " ~ fact_typ ~ " on " ~ curr_day) }}
            {% endif %}
        {% endif %}
        {% set col_names = result.column_names %}
        {% set var_columns = [] %}
        {% for col in col_names %}
            {% if col.startswith("VAR_") %}
                {% do var_columns.append(col) %}
            {% endif %}
        {% endfor %}

        {{ log("Found variance columns: " ~ var_columns | string, info=True) }}
        {% set row = result.rows[0] %}
        {% set out_of_range = [] %}

        {# Check if all relevant columns are zero (no data case) #}
        {% set all_zero = (
            row[src_system1 ~ "_UNT"] | float == 0 and
            row[src_system2 ~ "_UNT"] | float == 0 and
            row[src_system1 ~ "_CST"] | float == 0 and
            row[src_system2 ~ "_CST"] | float == 0 and
            row[src_system1 ~ "_RTL"] | float == 0 and
            row[src_system2 ~ "_RTL"] | float == 0
        ) %}

        {% if all_zero and no_data_warning %}
            {% set json_result = {
                "fact_type": fact_typ,
                "current_day": curr_day | string,
                "checkpoint": checkpoint,
                "source_systems": {
                    "system1": src_system1,
                    "system2": src_system2
                },
                "format_description": format_description,
                "validation_range": {
                    "lower": lower,
                    "upper": upper
                },
                "results": {
                    "unt": {"system1_value": 0, "system2_value": 0, "difference": 0, "variance_percent": 0},
                    "cst": {"system1_value": 0, "system2_value": 0, "difference": 0, "variance_percent": 0},
                    "rtl": {"system1_value": 0, "system2_value": 0, "difference": 0, "variance_percent": 0}
                },
                "status": "no_data",
                "no_data_warning": true,
                "out_of_range_columns": [],
                "log_file_name": var('log_file_name') if var('log_file_name') is defined else ''
            } %}
            {{ log("RECON_JSON_RESULT: " ~ json_result | tojson, info=True) }}
            {{ log("All reconciliation columns are zero for " ~ fact_typ ~ " on " ~ curr_day ~ ". Emitting no-data warning JSON and continuing.", info=True) }}
            {{ return(json_result) }}
        {% endif %}

        {% for col in var_columns %}
            {% set val = row[col] | float %}
            {{ log("Variance for " ~ col ~ " is: " ~ val, info=True) }}
            {% if val < lower or val > upper %}
                {% do out_of_range.append(col) %}
            {% endif %}
        {% endfor %}
        {# Create JSON result #}
        {% set json_result = {
            "fact_type": fact_typ,
            "current_day": curr_day | string,
            "checkpoint": checkpoint,
            "source_systems": {
                "system1": src_system1,
                "system2": src_system2
            },
            "format_description": format_description,
            "validation_range": {
                "lower": lower,
                "upper": upper
            },
            "results": {
                "unt": {
                    "system1_value": row[src_system1 ~ "_UNT"] | float,
                    "system2_value": row[src_system2 ~ "_UNT"] | float,
                    "difference": row["DIFF_UNT"] | float,
                    "variance_percent": row["VAR_PERCENT_UNT"] | float
                },
                "cst": {
                    "system1_value": row[src_system1 ~ "_CST"] | float,
                    "system2_value": row[src_system2 ~ "_CST"] | float,
                    "difference": row["DIFF_CST"] | float,
                    "variance_percent": row["VAR_PERCENT_CST"] | float
                },
                "rtl": {
                    "system1_value": row[src_system1 ~ "_RTL"] | float,
                    "system2_value": row[src_system2 ~ "_RTL"] | float,
                    "difference": row["DIFF_RTL"] | float,
                    "variance_percent": row["VAR_PERCENT_RTL"] | float
                }
            },
            "status": "success" if out_of_range | length == 0 else "failure",
            "out_of_range_columns": out_of_range
        } %}
        {# Here RECON_JSON_RESULT acts as the key for the json which is later captured from the
            dbt_runner script in order to parse it and send the reconciliation email  #}
        {{ log("RECON_JSON_RESULT: " ~ json_result | tojson, info=True) }}
        {% if out_of_range | length > 0 %}
            {{ exceptions.raise_compiler_error("Variance for columns " ~ out_of_range | join(", ") ~ " out of acceptable range (" ~ lower ~ " to " ~ upper ~ ") for " ~ fact_typ) }}
        {% else %}
            {{ log("All variance columns are within acceptable range for " ~ fact_typ, info=True) }}
        {% endif %}
        {{ result.print_table() }}
        {{ log("Reconciliation check completed for subject area: " ~ fact_typ ~ " between " ~ src_system1 ~ " and " ~ src_system2, info=True) }}

        
    {% endif %}
{% endmacro %}


{% macro load_recon_data(fact_typ,recon_config_macro, recon_step=0) %}
    {#--
        OVERVIEW:
        This macro orchestrates the reconciliation process for a given subject area and reconciliation step.
        It reads configuration, cleans existing reconciliation data, loads new data, and validates the results.
        
        INPUTS:
        - fact_typ (string): The fact type/subject area to reconcile (e.g., 'Sales', 'Inventory')
        - recon_config_macro (string): Name of the macro that provides reconciliation configuration
        - recon_step (integer, default=0): Reconciliation step identifier:
            0 = Compare LND and STG
            1 = Compare STG_V and DWH
            2 = Compare DWH and DM
        
        OUTPUTS:
        - Deletes existing reconciliation data for the current day
        - Loads new reconciliation data
        - Performs variance checks
        - Returns empty string on success
        - Raises exception on failure
    --#}
    {% if execute %}
        {{log('Recon config macro: '~recon_config_macro,info=True)}}
        {# Get the configuration from the subject-area specific macro #}
        {% set config_dict = context.get(recon_config_macro)() %}
        {# Extract the subject area configuration from the parsed yaml configuration  #}
        {% set subject_config = config_dict.get(fact_typ) %}
        {# Get the custom post date filter from the recon YAML config, if it exists. #}
        {% set recon_post_dt_filter = subject_config.get('recon_post_dt_filter') %}
        
        {# Find all matching entries #}
        {% set recon_entries = [] %}
        {% for entry in subject_config.recon_steps %}
            {% if entry.recon_step == recon_step  %}

                {{ recon_entries.append(entry) }}
            {% endif %}

        {% endfor %}

        {% if recon_entries | length == 0  %}
            {{ exceptions.raise_compiler_error("No recon configuration found for subject area " ~ fact_typ ~ " and recon step " ~ recon_step) }}
        {% endif %}

        {# Delete existing recon rows for this subject area and current business date #}
        {% set curr_day = robling_product.get_business_date() %}
            {% for entry in recon_entries %}
                {% set delete_query %}
                    DELETE FROM DW_DWH.DWH_F_RECON_LD
                    WHERE FACT_TYP = '{{ entry.fact_typ }}'
                    {% if recon_post_dt_filter %}
                        {#- If the custom post date filter is provided, use it -#}
                        --deleting the reconciliation data based on the custom post date filter provided in the configuration 
                        AND {{ recon_post_dt_filter }}
                    {% else %}
                        {#- use the default filter of LOAD_DT = curr_day for deletion -#}
                        --deleting the reconciliation data based on the default filter: LOAD_DT = CURR_DAY 
                        AND LOAD_DT = '{{ curr_day }}'
                    {% endif %}
                    AND SRC_SYSTEM = '{{ entry.src_system }}'
                {% endset %}
                {{ log("Executing delete for recon: ") }}
                {% do run_query(delete_query) %}
                {# Replace any templated variables in the recon SQL (e.g., CURR_DAY) #}
                {% set recon_sql = entry.sql | replace('${CURR_DAY}', curr_day) %}
                {# Execute the recon SQL command #}
                {{ log("Executing reconciliation SQL for subject area: " ~ fact_typ ~ " step: " ~ entry.src_system ~ "\n", info=True) }}
                {% do run_query(recon_sql) %}
            {% endfor %}
        {% set format_description = recon_entries[0].format.split(',') %}
        {% set validation_range_str = recon_entries[0].validation_range %}
        {# Read optional no_data_warning flag from recon configuration (default False) #}
        {% set no_data_warning = recon_entries[0].get('no_data_warning') if recon_entries[0].get('no_data_warning') is not none else false %}
        {{log('This is no data warning flag: '~no_data_warning,info=True)}}
        {% set range_parts = validation_range_str.split(',') %}
        {% set validation_range = [range_parts[0]|float, range_parts[1]|float] %}

        {{ robling_product.check_recon_variance(fact_typ,curr_day,recon_step, validation_range, format_description, recon_post_dt_filter, no_data_warning) }}

        {{ return('') }}
    {% endif %}
{% endmacro %}
