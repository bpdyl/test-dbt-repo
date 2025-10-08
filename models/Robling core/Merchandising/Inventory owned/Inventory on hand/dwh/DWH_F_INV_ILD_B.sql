{% if execute %}
{% set mth_dates = run_query("
    -- To find the start date and end date and current date of current month
    SELECT DISTINCT MTH_START_DT, MTH_END_DT, DAY_KEY FROM DW_DWH.DWH_D_TIM_DAY_LU WHERE DAY_KEY = (SELECT DISTINCT EFF_START_DT FROM DW_TMP.TMP_F_INV_ILD_B)") %}
{% set mth_start_dt = mth_dates.columns[0].values()[0] %}
{% set mth_end_dt = mth_dates.columns[1].values()[0] %}
{% set curr_dt = mth_dates.columns[2].values()[0] %}
{% endif %}
{{ config(
    materialized='incremental',
    alias='DWH_F_INV_ILD_B',
    schema='DW_DWH',
    unique_key=['ITM_ID','LOC_ID','EFF_START_DT'],
    on_schema_change='append_new_columns',
    tags = ['f_inv_ild_ld'],
    incremental_strategy='append',
    pre_hook = '{% if is_incremental() %} {{ close_target_using_curr_table() }} {% endif %}',
    post_hook = ["{{ log_dml_audit(this,ref('DWH_F_INV_IL_B'),'INSERT') }}"]
) }}
{# 
    Inserts data into Target table using Curr table. If the current business date matches the month_start date then all records
    in Curr table are inserted into Target table, Else only the changed records are inserted into the Target table using Curr table. 
#}
{% if mth_strt_dt|string == curr_dt|string %}
    -- Insert all records from curr table on the first day of the month
    SELECT SRC.* EXCLUDE(LAST_EFF_START_DT, RCD_INS_TS, RCD_UPD_TS)     -- Excluding columns not required and those that are redefined in the select statement to avoid duplicate column issues. 
        ,TO_DATE('{{ mth_strt_dt }}')               AS EFF_START_DT
        ,TO_DATE('{{ mth_end_dt }}')                AS EFF_END_DT
        ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ           AS RCD_INS_TS
        ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ           AS RCD_UPD_TS
    FROM {{ ref('DWH_F_INV_IL_B') }} src
{% else %}
    -- In regular dates other than month start dates, insert values existing in Temp table
    SELECT
        src.* EXCLUDE(LAST_EFF_START_DT, RCD_INS_TS, RCD_UPD_TS)        -- Excluding LAST_EFF_START_DT (mapped to EFF_START_DT) and columns that are redefined in the select statement to avoid duplicate column issues. 
        ,LAST_EFF_START_DT                          AS EFF_START_DT
        ,TO_DATE('{{ mth_end_dt }}')                AS EFF_END_DT
        ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ           AS RCD_INS_TS
        ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ           AS RCD_UPD_TS
    FROM {{ ref('DWH_F_INV_IL_B') }} src
    WHERE LAST_EFF_START_DT = (SELECT DISTINCT EFF_START_DT FROM DW_TMP.TMP_F_INV_ILD_B) 
{% endif %}

