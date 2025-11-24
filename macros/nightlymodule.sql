{% macro get_job_detail(script_name) %}
    {# 
        OVERVIEW:
        Retrieves the module type and job ID for a given script name from the batch scripts configuration.
        
        INPUTS:
        - script_name (string): Name of the script to look up
        
        OUTPUTS:
        - Returns dictionary with:
            - job_id: The associated job ID
    #}
    {% set query %}
        -- Purpose: Retrieve the JOB_ID for the given script name from the batch scripts configuration table
        SELECT JOB_ID
        FROM DW_DWH_V.V_DWH_C_BATCH_SCRIPTS
        WHERE LOWER(SCRIPT_NAME) = LOWER('{{ script_name }}')
    {% endset %}
    {% set result = run_query(query) %}
    {% if execute %}
        {% if result|length > 0 %}
            {% set job_id = result.columns[0].values()[0] %}
        {% else %}
            {{ exceptions.raise_compiler_error("Job detail not found in DWH_C_BATCH_SCRIPTS table for the job : '" ~ script_name~"' ." ) }}
        {% endif %}
        {{ return({'job_id': job_id}) }}
    {% endif %}
{% endmacro %}

{% macro get_business_date() %}
    {# 
        OVERVIEW:
        Retrieves the current business date from the DWH_C_PARAM table.
        
        INPUTS:
        - None
        
        OUTPUTS:
        - Returns date: The current business date from DWH_C_PARAM
    #}
    {% set query %}
        -- Purpose: Retrieve the current business date from the DWH_C_PARAM table
        SELECT TO_DATE(PARAM_VALUE) AS CURR_DAY
        FROM DW_DWH_V.V_DWH_C_PARAM
        WHERE PARAM_NAME = 'CURR_DAY'
    {% endset %}
    {% set result = run_query(query) %}
    {% if execute %}
        {% set curr_day = result.columns[0].values()[0] %}
        {{ return(curr_day) }}
    {% endif %}
{% endmacro %}

{% macro get_batch_id() %}
    {# 
        OVERVIEW:
        Retrieves the maximum batch ID for a specified module from the batch log.
        
        INPUTS:
        - None
        
        OUTPUTS:
        - Returns integer: The maximum batch ID from the batch log
    #}
    {% set query %}
        -- Purpose: Retrieve the maximum BATCH_ID from the batch log table
        SELECT COALESCE(MAX(BATCH_ID), 0) AS BATCH_ID
        FROM DW_DWH_V.V_DWH_C_BATCH_LOG
    {% endset %}

    {% set result = run_query(query) %}

    {% if execute %}
        {% set batch_id = result.columns[0].values()[0] %}
        {{ return(batch_id) }}
    {% endif %}
{% endmacro %}

{% macro get_run_id(script_name,batch_id) %}
    {# 
        OVERVIEW:
        Retrieves or initializes the RUN_ID for a given script.
        
        INPUTS:
        - script_name (string, optional): The name of the script. If not provided, defaults to the value of "SCRIPT_NAME" variable.
        - batch_id (integer): The batch id to get the run id for.
        
        OUTPUTS:
        - Returns the RUN_ID for the given script and batch id.
    #}
    {% if execute %}
        {% set run_id_query %}
            -- Purpose: Retrieve the latest RUN_ID for the given script and batch from the batch log table
            SELECT MAX(NVL(RUN_ID, 0)) AS RUN_ID
            FROM DW_DWH_V.V_DWH_C_BATCH_LOG
            WHERE job_name = '{{ script_name }}' 
            AND batch_id = '{{ batch_id }}' 
            AND MODULE_NAME = '{{ var("module_name","NTLY") }}'
            GROUP BY BATCH_ID
        {% endset %}

        {% set result = run_query(run_id_query) %}
        {% if result|length > 0  %}
            {% set run_id = result.columns[0].values()[0] %}
        {% else %}
            {% set run_id = 0 %}
        {% endif %}
        {% if run_id is none or run_id == 0 %}
            {% set new_run_id = 1 %}
        {% elif script_name == 'dwh_start_etl_batch' %}
            {% set new_run_id = run_id %}
        {% else %}
            {% set new_run_id = run_id + 1 %}
        {% endif %}
        
        {{ return(new_run_id) }}
    {% endif %}
{% endmacro %}

{% macro get_last_run_status(job_id, curr_day) %}
    {# 
        OVERVIEW:
        Retrieves the last execution status for a job on a specific business date.
        
        INPUTS:
        - job_id (string): The job identifier
        - curr_day (date): The business date to check
        
        OUTPUTS:
        - Returns dictionary with:
            - batch_id: Last batch ID
            - batch_status: Status code (-1=COMPLETE, 1=RESTART, 2=OTHER)
            - batch_bookmark: Last bookmark value
        - Logs the retrieved status details
    #}
    {% set query %}
        -- Purpose: Retrieve the last batch ID, status, and bookmark for a job on a specific business date
        SELECT
            COALESCE(MAX(BATCH_ID), 0) AS BATCH_ID,
            COALESCE(MAX(CASE 
                WHEN STATUS = 'COMPLETE' THEN -1 
                WHEN STATUS = 'RESTART' THEN 1 
                ELSE 2 END), 0) AS STATUS,
            COALESCE(MAX(BOOKMARK), '') AS BOOKMARK
        FROM DW_DWH_V.V_DWH_C_BATCH_LOG
        WHERE JOB_ID = '{{ job_id }}'
          AND BUSINESS_DATE = '{{ curr_day }}'
    {% endset %}

    {% set result = run_query(query) %}

    {% if execute %}
        {% set batch_id = result.columns[0].values()[0] %}
        {% set batch_status = result.columns[1].values()[0] %}
        {% set batch_bookmark = result.columns[2].values()[0] %}
        {{ log("Last run status - Batch ID: " ~ batch_id ~ ", Status: " ~ batch_status ~ ", Bookmark: " ~ batch_bookmark, info=True) }}
        {{ return({'batch_id': batch_id, 'batch_status': batch_status, 'batch_bookmark': batch_bookmark}) }}
    {% endif %}
{% endmacro %}

{% macro start_script(script_name, status, bookmark) %}
    {# 
        OVERVIEW:
        Initializes a new batch execution for a script by creating a batch log entry.
        
        INPUTS:
        - script_name (string): Name of the script to execute
        - status (string): Initial status for the batch
        - bookmark (string): Initial bookmark value
        
        OUTPUTS:
        - Creates new entry in DWH_C_BATCH_LOG if no existing run is found
        - Logs the batch initialization details
    #}
    {% if execute %}
    {% set recon_config_macro = model.config.meta.get('recon_config_macro', None) %}
    {{ log('Recon config macro: ' ~ recon_config_macro, info=True) }}
    {# Get the configuration from the subject-area specific macro safely #}
    {% if recon_config_macro and context.get(recon_config_macro) %}
        {% set config_callable = context.get(recon_config_macro) %}
        {% set config_dict = config_callable() if config_callable is callable else {} %}
    {% else %}
        {% set config_dict = {} %}
    {% endif %}
    {# Convert values to a list and grab the first item (actual config object of reconciliation) safely #}
    {% set recon_config = (config_dict.values() | list)[0] if (config_dict is mapping and config_dict | length > 0) else {} %}
    {# Extract the fact type/key from the config_dict (first key) if available #}
    {% set fact_typ = (config_dict.keys() | list)[0] if (config_dict is mapping and config_dict | length > 0) else None %}

    {# If the recon config specifies a missing_data_config, check the source and skip early if empty #}
    {% set missing_cfg = recon_config.get('missing_data_config', []) if recon_config is mapping else [] %}
    {% if missing_cfg %}
        {% set script_tag = model.config.tags[0] if (model.config.tags is defined and model.config.tags | length > 0) else model.name %}
        {% for m in missing_cfg %}
            {% set skip_flag = m.get('skip_if_no_records', False) if (m is mapping) else False %}
            {% set src_view = m.get('source_relation') if (m is mapping) else None %}
            {# Grab all models and their properties; we will pull out what we need #}
            {% set all_models = graph.nodes.values() %}
            {# Extract properties of the current model in this iteration #}
            {% set model_properties = (all_models | selectattr('name', 'equalto', src_view) | list).pop() %}
            {% set source_relation = adapter.get_relation(
                database=model_properties.database,
                schema=model_properties.schema,
                identifier=src_view
            ) %}
            {% if skip_flag and source_relation %}
                {% set chk_query = "SELECT COUNT(*) AS CNT FROM " ~ source_relation %}
                {{ log('Checking missing data for relation: ' ~ source_relation, info=True) }}
                {% set chk_res = run_query(chk_query) %}
                {% set row_count = 0 %}
                {% if chk_res is not none %}
                    {% set vals = chk_res.columns[0].values() if (chk_res.columns is defined and chk_res.columns[0] is defined) else [] %}
                    {% if vals | length > 0 %}
                        {% set row_count = vals[0] %}
                    {% endif %}
                {% endif %}
                {% if row_count | int == 0 %}
                    {{ log('No data found in ' ~ source_relation ~ '. Raising RECON_NO_DATA_WARNING.', info=True) }}
                    {% set exc_msg = (
                        'RECON_NO_DATA_WARNING: '
                        ~ 'fact_typ=' ~ (fact_typ if fact_typ else '') ~ ';'
                        ~ 'script=' ~ script_tag ~ ';'
                        ~ 'query=' ~ chk_query ~ ';'
                        ~ 'reason=No records in source ' ~ source_relation
                    ) %}
                    {{ exceptions.raise_compiler_error(exc_msg) }}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endif %}
    {# Get job details for the script #}
    {% set job_details = robling_product.get_job_detail(script_name) %}
    {% set job_id = job_details.job_id %}
    {# Get current business day #}
    {% set curr_day = robling_product.get_business_date() %}

    {# Get last run status for this job #}
    {% set last_status = robling_product.get_last_run_status(job_id, curr_day) %}
    {% set batch_status = last_status.batch_status %}

    {% set batch_id = robling_product.get_batch_id() %}
    {% set run_id = robling_product.get_run_id(script_name,batch_id) %}
    {% set batch_log %}
        -- Purpose: Insert a new batch log entry for the script execution
        INSERT INTO DW_DWH.DWH_C_BATCH_LOG (
            BATCH_ID,
            JOB_ID,
            RUN_ID,
            MODULE_NAME,
            JOB_NAME,
            BUSINESS_DATE,
            START_TIMESTAMP,
            END_TIMESTAMP,
            STATUS,
            ERROR_DETAIL,
            BOOKMARK,
            LOGFILE
        )
        SELECT 
            {{ batch_id }},
            '{{ job_id }}',
            '{{ run_id }}',
            '{{ var("module_name","NTLY") }}',
            '{{ script_name }}',
            '{{ curr_day }}',
            CURRENT_TIMESTAMP,
            NULL,
            'RUNNING',
            '',
            '{{ bookmark }}',
            '{{ var("log_file_name","") }}'
    {% endset %}

        {{ log("Inserting batch log with query:\n", info=True) }}
        {% do run_query(batch_log) %}
    {% endif %}
{% endmacro %}

{% macro log_script_success(this) %}
    {# 
        OVERVIEW:
        Marks a script execution as successfully completed in the batch log.
        
        INPUTS:
        - this (dbt.node): Current model reference
        
        OUTPUTS:
        - Updates DWH_C_BATCH_LOG with completion status
        - Logs the completion status
    #}
    {%if execute %}
        {#-- Retrieve model config to get the script name from tags --#}
        {% set cfg_val = robling_product.get_model_config_values(this) %}
        {% set script_name = cfg_val['tags'][0] %}
        
        {#-- Get job details and batch id using the helper macros --#}
        {% set job_details = robling_product.get_job_detail(script_name) %}
        {% set job_id = job_details.job_id %}
        {% set batch_id = robling_product.get_batch_id() %}
        {# Fetch the latest run_id for this batch_id and job_id #}
        {% set get_latest_run_id_query %}
            -- Purpose: Retrieve the latest RUN_ID for the given batch and job from the batch log
            SELECT COALESCE(MAX(RUN_ID), 1) AS RUN_ID
            FROM DW_DWH_V.V_DWH_C_BATCH_LOG
            WHERE BATCH_ID = '{{ batch_id }}' 
            AND JOB_ID = '{{ job_id }}'
            AND MODULE_NAME = '{{ var("module_name","NTLY") }}'
        {% endset %}

        {% set result = run_query(get_latest_run_id_query) %}
        {% set run_id = result.columns[0].values()[0] %}

        {% set update_sql %}
            -- Purpose: Mark the script execution as complete in the batch log
            UPDATE DW_DWH.DWH_C_BATCH_LOG
            SET END_TIMESTAMP = CURRENT_TIMESTAMP(),
                STATUS = 'COMPLETE',
                BOOKMARK = 'COMPLETE'
            WHERE BATCH_ID = '{{ batch_id }}' 
            AND JOB_ID = '{{ job_id }}' AND RUN_ID = '{{ run_id }}';
        {% endset %}
        
        {% do run_query(update_sql) %}
        {% do log("#################### End Script: Successful ####################",info=True) %}
    {% endif %}
{% endmacro %}

{% macro get_model_config_values(model_ref) %}
    {# 
        OVERVIEW:
        Retrieves configuration values for a given dbt model.
        
        INPUTS:
        - model_ref (dbt.node): Reference to the dbt model
        
        OUTPUTS:
        - Returns dictionary: Model configuration values
    #}
    {%- set table_name = model_ref.identifier -%}
    {% for node in graph.nodes.values() %}
        {%- set model_name = node.unique_id.split('.')[-1] -%}
        {%- if table_name == model_name -%}
            {%- set model_config = node.config -%}
            {{ return(model_config) }}
        {%- endif -%}
    {% endfor %}
{% endmacro %}

{% macro log_dml_audit(this, source, activity_type) %}
    {# 
        OVERVIEW:
        Records DML activity details in the audit log, including row counts and operation type.
        
        INPUTS:
        - this (dbt.node): Current model reference
        - source (relation): Source table reference
        - activity_type (string): Type of DML operation (INSERT, UPDATE, MERGE, DELETE)
        
        OUTPUTS:
        - Creates entry in DWH_C_LOAD_AUDIT_LOG with activity details
        - Logs the audit entry details
    #}
    {#-- Query the query history for the last DML statement of the given type --#}
    {% set history_query %}
        -- Purpose: Retrieve the most recent DML query of the specified type from the session query history
        SELECT QUERY_ID,
               ROWS_INSERTED,
               ROWS_WRITTEN_TO_RESULT,
               QUERY_TYPE
        FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION())
        WHERE QUERY_TYPE = '{{ activity_type }}'
        ORDER BY END_TIME DESC
        LIMIT 1
    {% endset %}
    {% set last_query_result = run_query(history_query) %}
    
    {% if execute %}
        {# Use the length filter to verify whether run_query returned any rows or not #}
        {% if last_query_result|length > 0 %}
            {#-- Extract values from query history --#}
            {% set query_id = last_query_result.columns[0].values()[0] %}
            {% set rows_inserted = last_query_result.columns[1].values()[0] %}
            {% set query_type = last_query_result.columns[3].values()[0] %}
        {% else %}
            {#-- Set default values --#}
            {% set query_id = 'NULL' %}
            {% set rows_inserted = 0 %}
            {% set query_type = 'NULL' %}
        {% endif %}
        
        {#-- Determine the activity_count based on the query type --#}
        {% if query_type == 'MERGE' %}
            {% set merge_activity_query %}
                -- Purpose: Get the total number of rows inserted and updated for the last MERGE operation
                SELECT COALESCE("number of rows inserted", 0) + COALESCE("number of rows updated", 0) AS activity_count
                FROM TABLE(RESULT_SCAN('{{ query_id }}'))
            {% endset %}
            {% set merge_result = run_query(merge_activity_query) %}
            {% set activity_count = merge_result.rows[0]['ACTIVITY_COUNT'] %}
        {% elif query_type == 'UPDATE' %}
            {% set update_activity_query %}
                -- Purpose: Get the number of rows updated for the last UPDATE operation
                SELECT COALESCE("number of rows updated", 0) AS activity_count
                FROM TABLE(RESULT_SCAN('{{ query_id }}'))
            {% endset %}
            {% set update_result = run_query(update_activity_query) %}
            {% set activity_count = update_result.rows[0]['ACTIVITY_COUNT'] %}
        {% elif query_type == 'DELETE' %}
            {% set update_activity_query %}
                -- Purpose: Get the number of rows deleted for the last DELETE operation
                SELECT COALESCE("number of rows deleted", 0) AS activity_count
                FROM TABLE(RESULT_SCAN('{{ query_id }}'))
            {% endset %}
            {% set update_result = run_query(update_activity_query) %}
            {% set activity_count = update_result.rows[0]['ACTIVITY_COUNT'] %}
        {% else %}
            {% set activity_count = rows_inserted %}
        {% endif %}
        
        {#-- Retrieve model config to get the script name from tags --#}
        {% set cfg_val = robling_product.get_model_config_values(this) %}
        {% set script_name = cfg_val['tags'][0] %}
    
        {#-- Get job details and batch id using the helper macros --#}
        {% set job_details = robling_product.get_job_detail(script_name) %}
        {% set batch_id = robling_product.get_batch_id() %}
        {% set target_name = this.identifier %}

        {#-- Build the audit log INSERT SQL --#}
        {% set audit_sql %}
            -- Purpose: Insert a new audit log entry for the DML activity
            INSERT INTO DW_DWH.DWH_C_LOAD_AUDIT_LOG
                (AUDIT_TIMESTAMP, SCRIPT_NAME, BATCH_ID, SOURCE, TARGET, ACTIVITY_TYPE, ACTIVITY_COUNT, COUNT_IN_SOURCE)
            SELECT
                CURRENT_TIMESTAMP()
                ,'{{ script_name }}'
                ,{{ batch_id }}
                ,'{{ source.identifier }}'
                ,'{{ this.identifier }}'
                ,'{{ query_type }}'
                ,{{ activity_count }}
                {%-if query_type in ('MERGE', 'INSERT')-%}
                ,(SELECT COUNT(*) FROM {{ source }})
                {%-else-%}
                ,NULL
                {%-endif-%}
        {% endset %}
            {{ log("Executing audit log SQL: ", info=True) }}
            {% do run_query(audit_sql) %}
    {% endif %}
    {{ return('') }}
{% endmacro %}


{% macro update_batch_log_on_failure(results) %}
    {#
        OVERVIEW:
        Logs the failure of a script execution in the batch log.

        INPUTS:
        - results (list): A list of dbt run results containing status and error messages.

        OUTPUTS:
        - Updates DWH_C_BATCH_LOG with failure status and error details if any model fails.
        - Logs the error message.
        - Special handling: If error contains RECON_NO_DATA_WARNING, logs as success instead.
    #}
    {% if execute %}
        {% for res in results %}
            {% if res.status == 'error' %}
                {% set script_name = res.node.config.tags[0] %}
                {# While running the dbt jobs from the command line (not using dbt_runner.py) user may not always use tags
                and can use individual model/test's name. In that case script_name variable is empty and might throw error
                thus we exit early if script name is not available #}
                {% if not script_name %}
                    {{ return('') }}
                {% endif %}
                {% set error_message = res.message.replace("'", "''") | default('SCRIPT FAILED, SEE LOG FOR DETAIL') %}
                {# Check if this is a RECON_NO_DATA_WARNING case #}
                {% if 'RECON_NO_DATA_WARNING' in error_message %}
                    {% do log("#################### Reconciliation Warning: No Data Found ####################", info=True) %}
                    {% do log("WARNING: Reconciliation data for " ~ script_name ~" is empty. Script execution and batch log update are skipped.", info=True) %}
                    {# do nothing #}
                {% else %}
                    {% set job_details = robling_product.get_job_detail(script_name) %}
                    {% set job_id = job_details.job_id %}
                    {% set batch_id = robling_product.get_batch_id() %}
                    
                    {# Fetch the latest run_id for this batch_id and job_id #}
                    {% set get_latest_run_id_query %}
                        -- Purpose: Retrieve the latest RUN_ID for the given batch and job from the batch log (on failure)
                        SELECT COALESCE(MAX(RUN_ID), 1) AS RUN_ID
                        FROM DW_DWH_V.V_DWH_C_BATCH_LOG
                        WHERE BATCH_ID = '{{ batch_id }}' 
                        AND JOB_ID = '{{ job_id }}'
                        AND MODULE_NAME = '{{ var("module_name","NTLY") }}'
                    {% endset %}

                    {% set run_id_result = run_query(get_latest_run_id_query) %}
                    {% set run_id = run_id_result.columns[0].values()[0] %}
                    
                    {% set update_sql %}
                        -- Purpose: Mark the script execution as failed in the batch log and record the error message
                        UPDATE DW_DWH.DWH_C_BATCH_LOG
                        SET END_TIMESTAMP = CURRENT_TIMESTAMP(),
                            STATUS = 'ERROR',
                            ERROR_DETAIL = '{{ error_message }}'
                        WHERE BATCH_ID = '{{ batch_id }}' 
                        AND JOB_ID = '{{ job_id }}' AND RUN_ID = '{{ run_id }}';
                    {% endset %}
                    
                    {% do run_query(update_sql) %}
                    {% do log("#################### End Script: Error ####################", info=True) %}
                    {% do log("Error in " ~ script_name ~ ": " ~ error_message, info=True) %}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}

{% macro close_dimension_using_temp(target_table, temp_table, dim_key_column=None) %}
{# 
    This macro updates the RCD_CLOSE_FLG to 1 and sets RCD_CLOSE_DT to current date
    for records in the target table that no longer exist in the temp table.
    
    Args:
        target_table: The target dimension table to update
        temp_table: The temporary table containing current records
        dim_key_column: The surrogate key column (e.g., 'CHN_KEY' for Chain dimension)
        
    Returns:
        SQL statement to close dimension records 
#}
    {% if execute %}
        {#-- Retrieve model config to get the unique key values to create the join condition --#}
        {% set cfg_val = robling_product.get_model_config_values(target_table) %}
        {% set dim_join_columns = cfg_val['unique_key'] %}
        
        {% set join_conditions = [] %}
        {% for column in dim_join_columns %}
            {% do join_conditions.append("tgt." + column + " = src." + column) %}
        {% endfor %}
        
        {% set join_list = join_conditions | join(" AND ") %}
        {% set curr_day = robling_product.get_business_date() %}
        {% set query %}
            -- Purpose: Close dimension records in the target table that do not exist in the temp table by setting RCD_CLOSE_FLG and RCD_CLOSE_DT
            UPDATE {{ target_table }} tgt
            SET RCD_UPD_TS = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
                ,RCD_CLOSE_FLG = 1
                ,RCD_CLOSE_DT = '{{ curr_day }}'
            WHERE NOT EXISTS
                (SELECT 1 FROM {{ temp_table }} src WHERE {{ join_list }})
            AND tgt.RCD_CLOSE_FLG = 0
        {% endset %}
        
        {% do run_query(query) %}
        
        {{ log("Executed close_dimension_using_temp for " ~ target_table ~ " using " ~ temp_table, info=True) }}
    {% endif %}
    
{% endmacro %}

{% macro update_closed_dimension_using_rollup(target_table, rollup_table) %}
{# 
    This macro updates closed dimension records with the latest values from the rollup table.
    
    Args:
        target_table: The target dimension table to update
        rollup_table: The rollup table containing the parent dimension data
        
    Returns:
        SQL statement to update closed dimension records
#}
    {% if execute %}
        {% set rollup_update_columns_list = [] %}
        {#-- Retrieve model config to get the rollup fields values to create the join condition --#}
        {% set cfg_val = robling_product.get_model_config_values(target_table) %}
        {% set rollup_key_column = cfg_val['rollup_key'][0] %}
        {% set rollup_fields = cfg_val['rollup_fields'] %}
        {% for field in rollup_fields %}
            {% do rollup_update_columns_list.append("tgt." + field + " = src." + field) %}
        {% endfor %}
        {% set update_columns = rollup_update_columns_list | join(", ") %}
        
        {% set query %}
            -- Purpose: Update closed dimension records in the target table with the latest values from the rollup table
            UPDATE {{ target_table }} tgt
            SET {{ update_columns }},
                RCD_UPD_TS = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
            FROM {{ rollup_table }} src
            WHERE tgt.{{ rollup_key_column }} = src.{{ rollup_key_column }}
            AND tgt.RCD_CLOSE_FLG = 1
        {% endset %}
        
        {% do run_query(query) %}
        
        {{ log("Executed update_closed_dimension_using_rollup for " ~ target_table ~ " using " ~ rollup_table, info=True) }}
    {% endif %}
    
{% endmacro %}

{% macro get_rendered_sql(model_name) %}
{#
    OVERVIEW:
    Retrieves and renders the final SQL for a given dbt model using its raw code.
    This macro is useful for dynamically generating the compiled SQL of a model
    at runtime — for example, to materialize a table definition or preview logic
    without directly referencing the model in a dbt DAG.

    INPUTS:
    - model_name (string): The name of the dbt model to retrieve and render.

    OUTPUTS:
    - Returns the rendered SQL (string) for the specified model.
    - Raises a compiler error if the model is not found in the dbt graph.

    USAGE:
    {{ robling_product.get_rendered_sql('my_model_name') }}
  #}
  {% if execute %}
  {# -- Locate the target model node from the dbt graph by name -- #}
    {% set target_node = graph.nodes.values()
      | selectattr("resource_type", "equalto", "model")
      | selectattr("name", "equalto", model_name)
      | list
      | first
    %}

    {% if target_node %}
    {# -- Return the rendered SQL from the model's raw code -- #}
      {{ return(render(target_node.raw_code)) }}
    {% else %}
      {{ exceptions.raise_compiler_error("Could not find model '" ~ model_name ~ "' in the dbt graph.") }}
    {% endif %}
  {% else %}
  {# -- In compile-only or dry-run mode, return an empty string -- #}
    {{ return("") }}
  {% endif %}
{% endmacro %}

{% macro check_if_model_exists(model_name, package_name=none) %}
    {#
    Checks if a model exists in the project codebase.

    Args:
        model_name (str): Name of the model to check (without .sql extension)
        package_name (str, optional): Package name if checking in a dependency

    Returns:
        bool: True if model exists, False otherwise
    #}
    {% if execute %}
        {% if not package_name %}
            {% set package_name = project_name %}
        {% endif %}

        {% set model_identifier = package_name ~ '.' ~ model_name %}
        {% set target_node = graph.nodes.values()
        | selectattr("resource_type", "equalto", "model")
        | selectattr("name", "equalto", model_name)
        | selectattr("package_name","equalto",package_name )
        | list
        | first
        %}

        {% if target_node %}
            {{return(true)}}
        {# {% for node in graph.nodes.values() %}
            {% if node.resource_type == 'model'
                and node.name == model_name
                and node.package_name == package_name %}
                {{ return(true) }}
            {% endif %}
        {% endfor %} #}
        {% endif %}
        {{ return(false) }}
    {% endif %}
{% endmacro %}

{% macro get_upstream_relations(model_name) %}
{#
    OVERVIEW:
    Retrieves the upstream model(s) as dbt Relation objects for a given model.
    This is useful when you need to query or reference the upstream models directly
    inside custom materializations (e.g., to dynamically access the staging/temp model).

    INPUTS:
    - model_name (string): The name of the dbt model whose upstream relations
      you want to fetch.

    OUTPUTS:
    - Returns a list of Relation objects (database.schema.table).
    - If no upstream models are found, returns an empty list.
    - Raises a compiler error if the target model is not found in the dbt graph.

    USAGE:
    -- Get all upstream relations for the current model
    {% set parents = get_upstream_relations(this.name) %}

    -- Get the first upstream relation (common case)
    {% set parent = (get_upstream_relations(this.name) | first) %}

    -- Use in SQL
    select * from {{ parent }}
#}

    {% if execute %}
    {# -- Locate the target model node from the dbt graph by name -- #}
    {% set target_node = graph.nodes.values()
        | selectattr("resource_type", "equalto", "model")
        | selectattr("name", "equalto", model_name)
        | list
        | first
    %}

    {% if target_node %}
        {# -- Extract only model parents (ignore sources/seeds/tests) -- #}
        {% set parent_nodes = target_node.depends_on.nodes
            | list
        %}
        {{log('Parent nodes: '~parent_nodes,info=True)}}
        {# -- Strip "model." prefix to get model names -- #}
        {% set parent_models = parent_nodes
            | map("replace", "model.", "")
            | list
        %}

        {# -- Build Relation objects for each parent -- #}
        {% set parent_relations = [] %}
        {% for parent_model in parent_models %}
            {% set parent_node = graph.nodes["model." ~ parent_model] %}
            {% set rel = api.Relation.create(
                database=parent_node.database,
                schema=parent_node.schema,
                identifier=parent_node.alias
            ) %}
            {% do parent_relations.append(rel) %}
        {% endfor %}

        {{ return(parent_relations) }}
    {% else %}
        {{ exceptions.raise_compiler_error("Could not find model '" ~ model_name ~ "' in the dbt graph.") }}
    {% endif %}
    {% else %}
    {{ return([]) }}
    {% endif %}

{% endmacro %}