{%- test run_general_integrity_checks(
    model,
    primary_key_columns=[],
    foreign_key_checks=[],
    snapshot_column=None,
    key_fields=[],
    optional_fields=[]
) -%}
{#-
    OVERVIEW:
    This macro performs general integrity checks on a given table. It supports validations for:
    - Total record count
    - Primary key uniqueness
    - Foreign key relationships
    - Single snapshot column consistency
    - Required field population

    INPUTS:
    - model (dbt model object): The table or view being tested.
    - primary_key_columns (list): List of columns that together define a unique key for the table.
    - foreign_key_checks (list of structs): Each entry should have `column_name`, `parent_table`, and `parent_pk_column` defined.
    - snapshot_column (string or None): The column used to validate a single-snapshot check.
    - key_fields (list): Columns that are required to be populated (non-null, non-empty).
    - optional_fields (list): Optional fields that are checked for completeness but not required.

    OUTPUTS:
    - Returns a result set with integrity violations, if any, for the given model.
-#}
    {% if execute %}
    {#- Get the configuration values from the config dictionary -#}
    {%- set sub_area_name = config.get('sub_area_name') -%}
    {%- set table_name_to_test = config.get('table_name_to_test',model.identifier) -%}
    {%- set business_date = robling_product.get_business_date() -%}

    {# Define severity mapping for different test types #}
    {%- set severity_config = config.get('custom_severity_mapping', default= {
        'Total Records': 'Notice',
        'PK violations': 'Critical',
        'FK violations': 'Critical',
        'Single-snapshot violations': 'Warning',
        'Key Field': 'Warning',
        'Optional Field': 'Notice'
    }) -%}
    
    {# Create the relation(full namespace of the table) so that it can be queried 
    This handles cases where `table_name_to_test` might be explicitly provided or defaults to the model's identifier.#}
    {%- if table_name_to_test is not none -%}
        {%- set table_to_test  = api.Relation.create(
                database=database,  
                schema=model.schema,
                identifier=table_name_to_test
            ) -%}
    {%- else -%}
        {%- set table_to_test  = model -%}
    {%- endif -%}

    {# Prepare a comma-separated string of primary key columns with table alias for using it in integrity check query below #}
    {%- set pk_list = [] -%}
    {%- for col in primary_key_columns -%}
        {%- do pk_list.append("src." ~ col | trim) -%}
    {%- endfor -%}
    {%- set primary_key_cols_str = pk_list | join(",") -%}

    {# Construct the test descriptions and set them in the config dictionary.
        These descriptions will be used in the test materialization for deleting test records
        from the integrity lookup table for the current business date. #}
    {%- set generated_test_descriptions = [] -%}
    {%- do generated_test_descriptions.append("Total Records") -%}
    {%- if primary_key_columns -%}
    {%- do generated_test_descriptions.append("PK violations") -%}
    {%- endif -%}
    {%- for fk_check in foreign_key_checks -%}
    {%- do generated_test_descriptions.append("FK violations on Column " ~ fk_check.column_name) -%}
    {%- endfor -%}
    {%- if snapshot_column -%}
    {%- do generated_test_descriptions.append("Single-snapshot violations") -%}
    {%- endif -%}
    {%- for col in key_fields -%}
    {%- do generated_test_descriptions.append(col ~ " not populated (Key Field)") -%}
    {%- endfor -%}
    {%- for col in optional_fields -%}
    {%- do generated_test_descriptions.append(col ~ " not populated (Optional Field)") -%}
    {%- endfor -%}

    {%- do config.set('generated_test_descs', generated_test_descriptions) -%}

    {# Start of the SQL query to perform integrity checks. #}
    WITH checks AS (
        SELECT
            0 AS "dummy",
            COUNT(*) :: INT AS "Total Records"

            {# PK Violations #}
            {% if primary_key_columns %}
            , (COUNT(*) - COUNT(DISTINCT {{ primary_key_cols_str }})) :: INT AS "PK violations"
            {% endif %}

            {# FK Violations #}
            {% for fk_check in foreign_key_checks %}
            , COUNT_IF({{ fk_check.parent_table.split('.')[-1] }}.{{ fk_check.parent_pk_column }} IS NULL) :: INT AS "FK violations on Column {{ fk_check.column_name }}"
            {% endfor %}

            {# Single-snapshot Violations: Checks if there is more than one distinct value in the snapshot column,
            indicating an issue in a single-snapshot expectation. #}
            {% if snapshot_column %}
            , (COUNT(DISTINCT src.{{ snapshot_column }}) - 1) :: INT AS "Single-snapshot violations"
            {% endif %}

            {# Checking for empty columns - Key Fields #}
            {% for col in key_fields %}
            , COUNT_IF(ZEROIFNULL(LENGTH(src.{{ col }})) = 0) :: INT AS "{{ col }} not populated (Key Field)"
            {% endfor %}

            {# Checking for empty columns - Optional Fields #}
            {% for col in optional_fields %}
            , COUNT_IF(ZEROIFNULL(LENGTH(src.{{ col }})) = 0) :: INT AS "{{ col }} not populated (Optional Field)"
            {% endfor %}

        FROM {{ table_to_test }} src

        {# FK Joins #}
        {% for fk_check in foreign_key_checks %}
        LEFT JOIN {{ fk_check.parent_table }} {{ fk_check.parent_table.split('.')[-1] }} ON src.{{ fk_check.column_name }} = TRIM({{ fk_check.parent_table.split('.')[-1] }}.{{ fk_check.parent_pk_column }})
        {% endfor %}

        {# Add where condition if where_condition parameter is set in the configuration #}
        {%- if config.get('where_condition', none) -%}
            {# Check if ${CURR_DAY} exists in the where condition if it does replace it with business date #}
            {%- set where_condition = config.get('where_condition') -%}
            {%- if where_condition is string and '${CURR_DAY}' in where_condition -%}
                {%- set where_condition = where_condition | replace('${CURR_DAY}', business_date | string) -%}
            {%- endif -%}
            /* Filter specified from the config parameter where_condition */
            WHERE {{ where_condition }}
        {%- endif -%}
    )
    ,
    {# Unpivot the results from the 'checks' CTE to transform column-wise test counts into row-wise format. #}
    unpiv AS (
        SELECT * FROM checks
        UNPIVOT ("# Records" FOR "Test Condition" IN (
            "Total Records"
            {% if primary_key_columns %}
            , "PK violations"
            {% endif %}
            {% for fk_check in foreign_key_checks %}
            , "FK violations on Column {{ fk_check.column_name }}"
            {% endfor %}
            {% if snapshot_column %}
            , "Single-snapshot violations"
            {% endif %}
            {% for col in key_fields %}
            , "{{ col }} not populated (Key Field)"
            {% endfor %}
            {% for col in optional_fields %}
            , "{{ col }} not populated (Optional Field)"
            {% endfor %}
        ))
    )
    {# Final SELECT statement to format the unpivoted results into the standard integrity lookup table structure. #}
    SELECT
        '{{ sub_area_name }}'                   AS SUB_AREA_NAME,
        '{{ table_name_to_test }}'              AS TABLE_NAME,
        '{{ business_date }}'                   AS LOAD_DT,
        {% if var('transaction_date', None) %} -- Allow transaction date to be passed as a var if available
        '{{ var("transaction_date") }}'         AS TXN_DT,
        {% else %}
        NULL AS TXN_DT,
        {% endif %}
        "Test Condition"                        AS TEST_DESC,
        CASE WHEN "Test Condition" = 'Total Records' THEN ''
            ELSE
                "# Records"::VARCHAR 
        END                                     AS INTEGRITY_CHK_VIOLATION_ROW_CNT,
        CASE WHEN "Test Condition" = 'Total Records' THEN "# Records" :: INT
            ELSE MAX("# Records" :: INT) OVER ()
        END                                     AS ROW_CNT,
        CASE
            WHEN "Test Condition" = 'Total Records' THEN ''
            WHEN "# Records" > 0 THEN 'Fail'
            ELSE 'Pass'
        END                                     AS PASS_FAIL,
        -- severity logic
        CASE 
            WHEN "Test Condition" LIKE '%PK violations%' THEN '{{ severity_config.get("PK violations", "Critical") }}'
            WHEN "Test Condition" LIKE '%FK violations%' THEN '{{ severity_config.get("FK violations", "Critical") }}'
            WHEN "Test Condition" LIKE '%Single-snapshot%' THEN '{{ severity_config.get("Single-snapshot violations", "Warning") }}'
            WHEN "Test Condition" LIKE '%(Key Field)%' THEN '{{ severity_config.get("Key Field", "Warning") }}'
            WHEN "Test Condition" LIKE '%(Optional Field)%' THEN '{{ severity_config.get("Optional Field", "Notice") }}'
            ELSE '{{ severity_config.get("Total Records", "Notice") }}'
        END                                     AS SEVERITY,
        CURRENT_TIMESTAMP                       AS RCD_INS_TS,
        CURRENT_TIMESTAMP                       AS RCD_UPD_TS
    FROM unpiv
    {# Exclude 'Total Records' from the final output as it's a reference, not a violation. #}
    --Replacing WHERE with QUALIFY since where first filters the rows with 'Total Records' value first
    --and calculates the ROW_CNT
    QUALIFY "Test Condition" <> 'Total Records'
    ORDER BY
        CASE
            WHEN PASS_FAIL = 'Fail' THEN 1
            ELSE 2
        END, 
        CASE SEVERITY
            WHEN 'Critical' THEN 1
            WHEN 'Warning' THEN 2
            WHEN 'Notice' THEN 3
            ELSE 4
        END,
        TEST_DESC
    {% endif %}

{%- endtest -%}


{% test run_custom_sql_checks(model, custom_sql_tests=[]) %}
{#
    OVERVIEW:
    This macro runs a list of user-defined SQL integrity checks on a model/source defined in a YAML file.
    Each custom query must return a single integer representing the count of failing records.
    It formats the results into the standard structure for the data integrity lookup table.

    INPUTS:
    - model (dbt model object): Table to test.
    - custom_sql_tests (list of structs): Each entry must have `name` and `sql` keys.

    OUTPUTS:
    - Returns a result set showing the result of each test, formatted for the integrity lookup table.
#}

    {%- set sub_area_name = config.get('sub_area_name') -%}
    {%- set table_name_to_test = config.get('table_name_to_test',model.identifier) -%}
    {%- set business_date = robling_product.get_business_date() -%}

    {# Extract the names of the custom tests provided for setting in the config dictionary. #}
    {%- set custom_test_names = [] -%}
    {%- for test_item in custom_sql_tests -%}
    {%- do custom_test_names.append(test_item.name) -%}
    {%- endfor -%}

    {%- do config.set('generated_test_descs', custom_test_names) -%}

    {# Create the relation (full namespace of the table) to be tested. #}
    {%- if table_name_to_test is not none -%}
        {%- set table_to_test  = api.Relation.create(
                database=database,  
                schema=model.schema,
                identifier=table_name_to_test
            ) -%}
    {%- else -%}
        {%- set table_to_test  = model -%}
    {%- endif -%}
    -- First, get the total row count of the model/table being tested.
    WITH total_rows AS (
        {%- set tot_rows_sql -%} 
        SELECT COUNT(*) as ROW_CNT FROM {{ table_to_test }} 
        {%- endset -%}
        {%- if config.get('where_condition', none) -%}
            {%- set where_condition = config.get('where_condition') -%}
            {%- if where_condition is string and '${CURR_DAY}' in where_condition -%}
                {%- set where_condition = where_condition | replace('${CURR_DAY}', business_date | string) -%}
            {%- endif -%}
            {# appending where clause in total row count sql #}
            {%- set tot_rows_sql = tot_rows_sql
                            ~ "
                            /* Filter specified from the config parameter where_condition */
                            " 
                            ~ " WHERE "~where_condition -%}
        {%- endif -%}
        {{- tot_rows_sql -}}
    ),
    -- Then, create a CTE for each custom SQL test
    -- that was provided in the YAML configuration.
    {% for test_item in custom_sql_tests %}
    test_{{ loop.index }} AS (
        -- Apply where condition to each custom test if specified
            {%- set test_sql = test_item.sql -%}
            {%- if config.get('where_condition', none) -%}
                {%- set where_condition = config.get('where_condition') -%}
                {%- if where_condition is string and '${CURR_DAY}' in where_condition -%}
                    {%- set where_condition = where_condition | replace('${CURR_DAY}', business_date | string) -%}
                {%- endif -%}
                {%- if 'WHERE' in test_sql.upper() -%}
                    {# appending where clause in test sql with AND operator #}
                    {%- set test_sql = test_sql 
                    ~ "
                    /* Filter specified from the config parameter where_condition */
                    "  
                    ~ " AND " ~ where_condition -%}
                {%- else -%}
                    {# appending where clause in test sql without AND operator #}
                    {%- set test_sql = test_sql 
                    ~ "
                    /* Filter specified from the config parameter where_condition */
                    " 
                    ~ " WHERE "~where_condition -%}
                {%- endif -%}
            {%- endif -%}
        -- The user-provided SQL must return a single row with a single integer value,
        -- representing the count of records violating the test condition.
        SELECT
            '{{ test_item.name }}' AS TEST_DESC,
            '{{ test_item.custom_severity | default("Warning") }}' AS SEVERITY,
            ( {{ test_sql }} )     AS INTEGRITY_CHK_VIOLATION_ROW_CNT
    ),
    {%- endfor -%}

    -- union all the individual test results together.
    unioned_results AS (
        {% for test_item in custom_sql_tests %}
        SELECT TEST_DESC
        ,INTEGRITY_CHK_VIOLATION_ROW_CNT 
        ,SEVERITY
        FROM test_{{ loop.index }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )

    -- Format the results into the final structure for the integrity lookup table.
    SELECT
        '{{ sub_area_name }}'                   AS SUB_AREA_NAME,
        '{{ table_name_to_test }}'              AS TABLE_NAME,
        '{{ business_date }}'                   AS LOAD_DT,
        {% if var('transaction_date', None) %}
        '{{ var("transaction_date") }}'         AS TXN_DT,
        {% else %}
        NULL AS TXN_DT,
        {% endif %}
        ur.TEST_DESC                            AS TEST_DESC,
        ur.INTEGRITY_CHK_VIOLATION_ROW_CNT      AS INTEGRITY_CHK_VIOLATION_ROW_CNT,
        tr.ROW_CNT                              AS ROW_CNT,
        CASE
            WHEN ur.INTEGRITY_CHK_VIOLATION_ROW_CNT > 0 THEN 'Fail'
            ELSE 'Pass'
        END                                     AS PASS_FAIL,
        ur.SEVERITY                             AS SEVERITY,
        CURRENT_TIMESTAMP                       AS RCD_INS_TS,
        CURRENT_TIMESTAMP                       AS RCD_UPD_TS
    FROM unioned_results ur, total_rows tr
    ORDER BY
        CASE
            WHEN PASS_FAIL = 'Fail' THEN 1
            ELSE 2
        END, TEST_DESC

{% endtest %}

{% test check_inventory_compression(model) %}
{#
    OVERVIEW:
    This test runs a detailed query to check if the inventory records are correctly compressed.
    The compression criteria for each itm-loc pair in inventory are:
        1. No unnecessary splits: A new record should only be created if the underlying data has changed.
        2. No gaps or overlaps: The end date of one record must be exactly one day before the start date of the next.
        3. End dates must be valid: EFF_END_DT must not be earlier than EFF_START_DT.
        4. The most recent record should have an end date matching the current month's end date.
    It formats the results into the standard structure for the data integrity lookup table.

    INPUTS:
    - model (dbt model object): The inventory ILD table to be tested (e.g., DWH_F_INV_ILD_B).

    OUTPUTS:
    - Returns a result set showing the count of each type of compression error, formatted for insertion into the integrity lookup table.
#}

    {#- Get the configuration values from the config dictionary -#}
    {%- set sub_area_name = config.get('sub_area_name') -%}
    {%- set table_name_to_test = config.get('table_name_to_test', model.identifier) -%}
    {%- set custom_severity = config.get('custom_severity',"Critical") -%}
    {%- set business_date = robling_product.get_business_date() -%}

    {# Create the relation (full namespace of the table) to be tested. #}
    {%- if table_name_to_test is not none -%}
        {%- set table_to_test  = api.Relation.create(
                database=database,
                schema=model.schema,
                identifier=table_name_to_test
            ) -%}
    {%- else -%}
        {%- set table_to_test  = model -%}
    {%- endif -%}

    {# Set fixed test descriptions for compression checks #}
    {%- set generated_test_descriptions = [
        'INV Comprsn Error: Data is identical to previous period',
        'INV Comprsn Error: Gap or Overlap between prior record',
        'INV Comprsn Error: END_DT != NEXT START_DT -1',
        'INV Comprsn Error: Active record not closed at mth end',
        'INV Comprsn Error: End date is greater than current eff start date'
    ] -%}

    {%- do config.set('generated_test_descs', generated_test_descriptions) -%}


    {# Retrieve the list of columns from the target table to get the hash comparison column list #}
    {%- set temp_column_list = get_columns_in_relation(table_to_test) | map(attribute='name') | list -%}
    {# Excluding columns from hash list #}
    {%- set columns_to_exclude = [
        'EFF_START_DT','EFF_END_DT','RCD_INS_TS','RCD_UPD_TS'
    ] -%}
    {# Generate the final list of columns for hash check #}
    {% set hash_columns = temp_column_list | reject('in', columns_to_exclude) | list | join(", ")%}
    {# CTE to find all compression violations #}
    WITH ALL_COMPRESSION_VIOLATIONS AS (
        SELECT
            INV.ITM_ID,
            INV.LOC_ID,
            INV.EFF_START_DT,
            INV.EFF_END_DT,

            -- USING HASH FUNCTION FOR GENERATING HASH VALUE OF THE CURRENT ROW
            -- EXCLUDING EFF_START_DT, EFF_END_DT AND MAINTENANCE COLUMN
            -- SO THAT WE COMPARE THE CURRENT ROW VS PREVIOUS ROW FOR CHANGES
            HASH(
                {{ hash_columns }}
            ) AS RECORD_HASH,

            -- USE THE LAG/LEAD FUNCTIONS TO GET THE HASH and DATES OF THE PREVIOUS/NEXT RECORD FOR THE SAME ITEM-LOC.
            LAG(EFF_END_DT, 1) OVER (PARTITION BY ITM_ID, LOC_ID ORDER BY EFF_START_DT, EFF_END_DT) AS PREV_END_DT,
            LEAD(EFF_START_DT) OVER (PARTITION BY ITM_ID, LOC_ID ORDER BY EFF_START_DT, EFF_END_DT) AS NEXT_START_DT,
            LAG(RECORD_HASH, 1) OVER (PARTITION BY ITM_ID, LOC_ID ORDER BY EFF_START_DT, EFF_END_DT) AS PREV_RECORD_HASH,
            TIM.MTH_START_DT,
            TIM.MTH_END_DT,

            -- This CASE statement identifies and describes the specific failure mode for a given row.
            CASE
                -- FAILURE CASE 1: UNNECESSARY SPLIT
                -- THIS HAPPENS IF THE DATA (HASH) IS THE SAME AS THE PREVIOUS RECORD,
                -- AND IT'S NOT A LEGITIMATE COMPRESSION (REOPENED IN EVERY MONTH START DATE
                -- AND CLOSED IN PREVIOUS MONTH END DATE)
                WHEN RECORD_HASH = PREV_RECORD_HASH AND TIM.MTH_START_DT <> INV.EFF_START_DT AND EFF_START_DT <= EFF_END_DT
                THEN 'INV Comprsn Error: Data is identical to previous period'

                -- FAILURE CASE 2: GAP OR OVERLAP
                WHEN PREV_END_DT IS NOT NULL AND (PREV_END_DT + 1) <> EFF_START_DT
                THEN 'INV Comprsn Error: Gap or Overlap between prior record'

                -- FAILURE CASE 3: INCORRECT END DATE
                WHEN NEXT_START_DT IS NOT NULL AND (NEXT_START_DT - 1) <> EFF_END_DT
                THEN 'INV Comprsn Error: END_DT != NEXT START_DT - 1'

                -- FAILURE CASE 4: INVALID DATE RANGE
                WHEN EFF_START_DT > EFF_END_DT
                THEN 'INV Comprsn Error: End date is before start date'

                -- FAILURE CASE 5: ACTIVE RECORD NOT CLOSED AT MONTH END
                WHEN NEXT_START_DT IS NULL AND EFF_END_DT <> MTH_END_DT
                THEN 'INV Comprsn Error: Active record not closed at mth end'

                ELSE NULL
            END AS TEST_DESC

        FROM {{ table_to_test }} INV
        JOIN DW_DWH.DWH_D_TIM_DAY_LU TIM ON INV.EFF_START_DT = TIM.DAY_KEY
        -- Filtering on 1 month only since we reopen the inventory snapshot at every month start date
        WHERE TIM.MTH_KEY = (SELECT CURR_MTH_KEY FROM DW_DWH.DWH_D_CURR_TIM_LU )
        -- THE FIRST RECORD FOR ANY ITEM-LOC IS NOT A VIOLATION, SO WE EXCLUDE IT.
        QUALIFY PREV_END_DT IS NOT NULL
        AND TEST_DESC IS NOT NULL
    ),

    -- CTE to count the number of rows for each distinct violation type.
    VIOLATION_COUNTS AS (
        SELECT
            TEST_DESC,
            COUNT(*) AS VIOLATION_ROWS
        FROM ALL_COMPRESSION_VIOLATIONS
        GROUP BY TEST_DESC
    ),

    -- CTE to get the total number of rows that were scanned for potential violations.
    TOTAL_COUNTS AS (
        SELECT COUNT(*) AS ROW_CNT FROM {{ table_to_test }} INV
        JOIN DW_DWH.DWH_D_TIM_DAY_LU TIM ON INV.EFF_START_DT = TIM.DAY_KEY
        -- Filtering on 1 month only since we reopen the inventory snapshot at every month start date
        WHERE TIM.MTH_KEY = (SELECT CURR_MTH_KEY FROM DW_DWH.DWH_D_CURR_TIM_LU )
    )

    -- Final SELECT to format the results into the integrity check table structure.
    SELECT
        '{{ sub_area_name }}'                   AS SUB_AREA_NAME,
        '{{ table_name_to_test }}'              AS TABLE_NAME,
        '{{ business_date }}'                   AS LOAD_DT,
        NULL                                    AS TXN_DT,
        VC.TEST_DESC                            AS TEST_DESC,
        VC.VIOLATION_ROWS                       AS INTEGRITY_CHK_VIOLATION_ROW_CNT,
        tc.ROW_CNT                              AS ROW_CNT,
        CASE
            WHEN VC.VIOLATION_ROWS > 0 THEN 'Fail'
            ELSE 'Pass'
        END                                     AS PASS_FAIL,
        '{{ custom_severity }}'                 AS SEVERITY,
        CURRENT_TIMESTAMP                       AS RCD_INS_TS,
        CURRENT_TIMESTAMP                       AS RCD_UPD_TS
    FROM VIOLATION_COUNTS VC, TOTAL_COUNTS tc
    ORDER BY
        CASE
            WHEN PASS_FAIL = 'Fail' THEN 1
            ELSE 2
        END, TEST_DESC

{% endtest %}