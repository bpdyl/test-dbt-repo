{% set key_columns = ['FACT_CDE','ITM_ID','LOC_ID','MEAS_DT'] %}
{% set hash_columns = [
    'CHN_KEY', 'CHN_ID', 'CHNL_KEY', 'CHNL_ID','LOC_KEY',
    'LOC_TYP_CDE', 'POSTAL_CDE', 'STATE_PROVINCE_CDE', 'COUNTRY_CDE',
    'DIV_KEY', 'DIV_ID', 'ITM_KEY','LCL_CNCY_CDE',
    'ITMLOC_STTS_CDE','F_FACT_CST','F_FACT_RTL', 'F_FACT_QTY1',
    'F_FACT_QTY2', 'F_FACT_QTY3', 'F_FACT_QTY4'
] %}

{{
    config(
        materialized='custom_merge',
        unique_key=key_columns,
        key_columns=key_columns,
        hash_columns=hash_columns,
        insert_columns=key_columns + hash_columns,
        schema='DM_MERCH',
        target_table = 'DM_F_MEAS_FACT_ILD_B',
        tags = ['dm_f_rig_inv_immnt_stkout_meas_fact_ild'],
        fact_cde='RIG_INV_IMMNT_STKOUT',
        pre_hook=["{{ start_script('dm_f_rig_inv_immnt_stkout_meas_fact_ild','RUNNING','NONE') }}"],
        post_hook = ["{{ log_script_success(this) }}"]
    )
}}
SELECT
    'RIG_INV_IMMNT_STKOUT'                                     AS FACT_CDE
    ,SRC.DAY_KEY                                               AS MEAS_DT
    ,{{ get_key_with_fallback_value('LOC.CHN_KEY') }}          AS CHN_KEY
    ,LOC.CHN_ID                                                AS CHN_ID
    ,{{ get_key_with_fallback_value('LOC.CHNL_KEY') }}         AS CHNL_KEY
    ,LOC.CHNL_ID                                               AS CHNL_ID
    ,SRC.LOC_KEY                                               AS LOC_KEY
    ,SRC.LOC_ID                                                AS LOC_ID
    ,LOC.LOC_TYP_CDE                                           AS LOC_TYP_CDE
    ,LOC.LOC_POSTAL_CDE                                        AS POSTAL_CDE
    ,LOC.LOC_STATE_PROVINCE_CDE                                AS STATE_PROVINCE_CDE
    ,LOC.LOC_COUNTRY_CDE                                       AS COUNTRY_CDE
    ,{{ get_key_with_fallback_value('ITM.DIV_KEY') }}          AS DIV_KEY
    ,ITM.DIV_ID                                                AS DIV_ID
    ,SRC.ITM_KEY                                               AS ITM_KEY
    ,SRC.ITM_ID                                                AS ITM_ID
    ,'{{ var("PRIMARY_CNCY_CDE") }}'                           AS LCL_CNCY_CDE 
    ,SRC.ITMLOC_STTS_CDE                                       AS ITMLOC_STTS_CDE 
    ,SRC.F_RIG_FCST_CST_LCL                                    AS F_FACT_CST  
    ,SRC.F_RIG_FCST_RTL_LCL                                    AS F_FACT_RTL  
    ,SRC.F_RIG_FCST_QTY                                        AS F_FACT_QTY1 
    ,SRC.F_IMMNT_STKOUT_COUNT                                  AS F_FACT_QTY2 
    ,SRC.F_IMMNT_STKOUT_THRESHOLD                              AS F_FACT_QTY3 
    ,SRC.F_NUM_DAYS_OF_SUPPLY                                  AS F_FACT_QTY4 
    ,CURRENT_TIMESTAMP                                         AS RCD_INS_TS                                
    ,CURRENT_TIMESTAMP                                         AS RCD_UPD_TS
FROM {{ ref('TMP_RIG_F_INV_IMMNT_STKOUT_ILD_B') }} SRC
LEFT JOIN {{ ref('V_DWH_D_ORG_LOC_LU') }} LOC ON SRC.LOC_ID = LOC.LOC_ID
LEFT JOIN {{ ref('V_DWH_D_PRD_ITM_LU') }} ITM ON SRC.ITM_ID = ITM.ITM_ID
-- ordering by MEAS_DT, LOC_KEY for performance through Snowflake partitioning
ORDER BY 
    MEAS_DT
    ,SRC.LOC_KEY