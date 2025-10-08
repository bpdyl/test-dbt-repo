{% macro insert_missing_into_temp(model_ref) %}
    {# 
        OVERVIEW:
        This macro inserts data missing from source into temp for cases where extraction does not provide data if OH=0. 
        The code only looks at those records which were not received in landing today and had OH <> 0 from previous batch to make it 0 in current batch.
        INPUTS:
        - model_ref (dbt.node): Reference to the dbt model
        
        OUTPUTS:
        - None
    #}
    {% if execute %}
    {% set source_relation = adapter.Relation.create(
        database=model_ref.database,
        schema='DW_TMP',
        identifier='TMP_F_INV_ILD_B',
        type = 'table') 
    %}
    {% set target_relation = adapter.Relation.create(
        database=model_ref.database,
        schema='DW_DWH',
        identifier='DWH_F_INV_IL_B',
        type = 'table') 
    %}
    {# Identify columns present in source but missing in target in case of new flex fields using dbt's native macro, excluding EFF_START_DT since it's absent in TMP table #}
    {% set source_cols_not_in_target = check_for_schema_changes(source_relation, target_relation)['source_not_in_target']|rejectattr('column','eq','EFF_START_DT')|list %}
    {# Ensure target table schema matches source by adding new columns in the curr table. This ensures same columns in TMP and Curr table durint this dml operation #}
    {% do alter_relation_add_remove_columns(target_relation, source_cols_not_in_target, none) %}

    {# Retrieve the list of columns from the temp table to get the column insertion list #}
    {% set temp_column_list = get_columns_in_relation(source_relation) | map(attribute='name') | list %}
    {# Excluding the columns that will be redefined again in the insert statement to avoid duplicate column issues #}
    {% set columns_to_exclude = [
        'EFF_START_DT', 'F_OH_QTY', 'F_OH_CST_LCL', 'F_OH_RTL_LCL',
        'F_IT_QTY', 'F_IT_CST_LCL', 'F_IT_RTL_LCL', 'F_OH_CST',
        'F_OH_RTL', 'F_IT_CST', 'F_IT_RTL'
    ] %}
    {# Generate the final list of columns for insertion by removing excluded columns from the temp table's column list #}
    {% set final_column_list = temp_column_list | reject('in', columns_to_exclude) | list %}

    {% set insert_sql %}
        INSERT INTO DW_TMP.TMP_F_INV_ILD_B (
            {{ final_column_list | join('\n            ,') }}
            ,EFF_START_DT
            ,F_OH_QTY
            ,F_OH_CST_LCL
            ,F_OH_RTL_LCL
            ,F_IT_QTY
            ,F_IT_CST_LCL
            ,F_IT_RTL_LCL
            ,F_OH_CST
            ,F_OH_RTL
            ,F_IT_CST
            ,F_IT_RTL
        )
        SELECT
            {{ final_column_list | join('\n            ,') }}
            ,TO_DATE('{{ robling_product.get_business_date() }}')   AS EFF_START_DT
            ,0                                                      AS F_OH_QTY
            ,0                                                      AS F_OH_CST_LCL
            ,0                                                      AS F_OH_RTL_LCL
            ,0                                                      AS F_IT_QTY
            ,0                                                      AS F_IT_CST_LCL
            ,0                                                      AS F_IT_RTL_LCL
            ,0                                                      AS F_OH_CST
            ,0                                                      AS F_OH_RTL
            ,0                                                      AS F_IT_CST
            ,0                                                      AS F_IT_RTL
        FROM {{ source('INV_SRC_TMP','DWH_F_INV_IL_B') }}                                           /* Using source instead of ref() to prevent cyclic dependencies*/
        WHERE (NVL(F_OH_QTY,0) <> 0 OR NVL(F_OH_CST_LCL,0) <> 0 OR NVL(F_OH_RTL_LCL,0) <> 0
            OR NVL(F_IT_QTY,0) <> 0 OR NVL(F_IT_CST_LCL,0) <> 0 OR NVL(F_IT_RTL_LCL,0) <> 0)        
        AND (SELECT PARAM_VALUE FROM DW_DWH.DWH_C_PARAM WHERE PARAM_NAME = 'INV_LOAD') <> 'DELTA'   /*AA20250401: Not having full data in landing in case of delta extraction makes sense. Hence this code doesn't do any insert if inventory load is a DELTA*/
        AND (ITM_ID, LOC_ID) NOT IN (SELECT ITM_ID, LOC_ID FROM {{ ref('TMP_F_INV_ILD_B') }})       /*AA20250401: This is used to find out records which were missing in extracted data from source*/
    {% endset %}

    {% do run_query(insert_sql) %}
    {% endif %}
{% endmacro %}

{% macro close_target_using_curr_table() %}
    {# 
        OVERVIEW:
        This macro closes the records in the target inventory table using the currrent snapshot.         
        INPUTS:
        None
        
        OUTPUTS:
        - None
    #}
    {% set update_sql %}
        UPDATE DW_DWH.DWH_F_INV_ILD_B tgt
        SET EFF_END_DT=src.LAST_EFF_START_DT - 1
        , RCD_UPD_TS=CURRENT_TIMESTAMP()
        FROM
        (SELECT SRC.*, DT.MTH_END_DT, DT.MTH_START_DT
            FROM {{ ref('DWH_F_INV_IL_B') }} SRC
            JOIN DW_DWH.DWH_D_TIM_DAY_LU DT
            ON SRC.LAST_EFF_START_DT = DT.DAY_KEY
            -- filtering on records that came in the current inventory snapshot
            WHERE SRC.LAST_EFF_START_DT = (SELECT DISTINCT EFF_START_DT FROM DW_TMP.TMP_F_INV_ILD_B) 
        ) SRC
        WHERE SRC.ITM_ID = TGT.ITM_ID AND SRC.LOC_ID = TGT.LOC_ID
            -- Expire current records
            AND TGT.EFF_END_DT = SRC.MTH_END_DT 
            -- Don't expire if the incoming snapshot is on the first date of the month
            AND TGT.EFF_END_DT <>  SRC.MTH_START_DT 
    {% endset %}
    {% do run_query(update_sql) %}
{% endmacro %}

{% macro update_staging_recon() %}
    {# 
        OVERVIEW:
        This macro updates staging recon to consider data from DWH for delta cases. This will pickup records from DWH table which didn't arrive from source
        INPUTS:
        None
        
        OUTPUTS:
        - None
    #}
    {% set curr_date = robling_product.get_business_date() %}
    {% set merge_sql %}
        MERGE INTO DW_DWH.DWH_F_RECON_LD TGT
        USING
        (
        SELECT  
            LOC_ID                      AS LOC_ID
            ,LCL_CNCY_CDE               AS LCL_CNCY_CDE
            ,SUM(F_OH_QTY)              AS F_OH_QTY
            ,SUM(F_OH_RTL_LCL)          AS F_OH_RTL_LCL
            ,SUM(F_OH_CST_LCL)          AS F_OH_CST_LCL
        FROM {{ ref('DWH_F_INV_IL_B') }} SRC
        WHERE (ITM_ID, LOC_ID) NOT IN (SELECT ITM_ID, LOC_ID FROM {{ ref('V_STG_F_INV_ILD_B') }})
        AND (SELECT TO_CHAR(PARAM_VALUE) FROM DW_DWH.DWH_C_PARAM WHERE PARAM_NAME = 'INV_LOAD') = 'DELTA'
        /*20240407 AA: This needs to work for delta condition only. The code will pickup records from DWH table which didn't arrive from source. Group by added as recon table is in location level*/
        GROUP BY LOC_ID, LCL_CNCY_CDE
        ) SRC
        ON TGT.LOC_ID = SRC.LOC_ID
        WHEN MATCHED /* 20240407 AA: Only look at current day record for Staging view of inventory*/
            AND TGT.LOAD_DT = '{{ robling_product.get_business_date() }}'
            AND TGT.FACT_TYP = 'Inventory On-Hand'
            AND TGT.SRC_SYSTEM = 'STG_V'
            AND TGT.SRC_TABLE = 'V_STG_F_INV_ILD_B'
        THEN
        UPDATE SET /* 20240407 AA: Adding the values received from DWH into pre-existing values in staging view */
            TGT.FACT_QTY = TGT.FACT_QTY + SRC.F_OH_QTY
            ,TGT.FACT_RTL_LCL = TGT.FACT_RTL_LCL + SRC.F_OH_CST_LCL
            ,TGT.FACT_CST_LCL = TGT.FACT_CST_LCL + SRC.F_OH_RTL_LCL
        WHEN NOT MATCHED
        THEN /* 20240407 AA: Sometimes data for an entire location might be missing, so in cases like this we would need to add new records instead of updating existing one*/
        INSERT (
            FACT_TYP
            ,SRC_SYSTEM
            ,SRC_TABLE
            ,LOAD_DT
            ,TXN_DT
            ,LOC_ID
            ,LCL_CNCY_CDE
            ,FACT_QTY
            ,FACT_RTL_LCL
            ,FACT_CST_LCL
            ,ATTR_1_NAME
            ,ATTR_1_VALUE
            ,RCD_INS_TS
            ,RCD_UPD_TS
        )
        VALUES
        (
            'Inventory On-Hand'
            ,'STG_V'
            ,'V_STG_F_INV_ILD_B'
            ,'{{ curr_date }}'
            ,'{{ curr_date }}'
            ,LOC_ID
            ,LCL_CNCY_CDE
            ,F_OH_QTY
            ,F_OH_RTL_LCL
            ,F_OH_CST_LCL
            ,NULL
            ,NULL
            ,CURRENT_TIMESTAMP()
            ,CURRENT_TIMESTAMP()
        )
    {% endset %}
    {% do run_query(merge_sql) %}
{% endmacro %}

{% macro generate_merge_statement_inv(target_relation, key_columns) %}
    {# 
        OVERVIEW:
        Generates a SQL merge statement for merging data from Temp to Curr Inv table.
        
        INPUTS:
        - target_relation (relation): Fully qualified target table (db.schema.table).
        - key_columns (list): List of columns used for the matching condition (Primary keys).
        
        OUTPUTS:
        - Returns a SQL merge statement string.
    #}

    {% set temp_relation = adapter.Relation.create(
        database=target_relation.database,
        schema='DW_TMP',
        identifier='TMP_F_INV_ILD_B',
        type = 'table') 
    %}

    {# Retrieve the list of columns from the temp table to get the initial hash column list #}
    {% set temp_column_list = get_columns_in_relation(temp_relation) | map(attribute='name') | list %}
    {# Excluding columns from hash list #}
    {% set columns_to_exclude = [
        'ITM_ID', 'LOC_ID', 'EFF_START_DT', 'ITM_KEY','LOC_KEY'
    ] %}
    {# Generate the final list of columns for hash check #}
    {% set hash_columns = temp_column_list | reject('in', columns_to_exclude) | list %}
    
    {% set update_columns = hash_columns + ['LAST_EFF_START_DT'] %}
    {% set insert_columns = key_columns + hash_columns + ['ITM_KEY','LOC_KEY','LAST_EFF_START_DT'] %}
    
    MERGE INTO {{ target_relation }} AS TGT
    USING (
        {{ sql }}
    ) AS SRC
    ON (
        {%- for key in key_columns -%}
            tgt.{{ key }} = src.{{ key }}{{ " and " if not loop.last else "" }}
        {%- endfor -%}
    )
    WHEN MATCHED AND (
        {{ robling_product.generate_hash_expression('TGT', hash_columns) }} <> {{ robling_product.generate_hash_expression('SRC', hash_columns) }}
    )
    THEN UPDATE SET 
        {% for col in update_columns %}
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
