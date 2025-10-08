{% set key_columns = ['FACT_CDE', 'LOC_ID', 'ITM_ID', 'MEAS_DT'] %}
{% set hash_columns = [
     'POST_DT'
    ,'CLOSED_DT'
    ,'ITM_KEY'
    ,'LOC_KEY'
    ,'CHN_KEY'
    ,'CHN_ID'
    ,'CHNL_KEY'
    ,'CHNL_ID'
    ,'LOC_TYP_CDE'
    ,'POSTAL_CDE'
    ,'STATE_PROVINCE_CDE'
    ,'COUNTRY_CDE'
    ,'DIV_KEY'
    ,'DIV_ID'
    ,'ITMLOC_STTS_CDE'
    ,'LCL_CNCY_CDE'
    ,'F_FACT_QTY'
    ,'F_FACT_CST'
    ,'F_FACT_RTL'
    ,'F_FACT_QTY1'
    ,'F_FACT_AMT1'
    ,'F_FACT_AMT2'
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
        tags = ['dm_f_inv_meas_fact_ild'],
        pre_hook=["{{ start_script('dm_f_inv_meas_fact_ild','RUNNING','NONE') }}"],
        post_hook = ["{{ load_recon_data('Inventory On-Hand',recon_config_macro='mac_f_inv_recon_script_sql', recon_step=2) }}"
                    ,"{{ log_script_success(this) }}"]
    )
}}
SELECT   
    'OH'                                                        AS FACT_CDE
    ,SRC.EFF_START_DT                                           AS POST_DT
    ,SRC.EFF_START_DT                                           AS MEAS_DT
    ,SRC.EFF_END_DT                                             AS CLOSED_DT
    ,{{ get_key_with_fallback_value('LOC.CHN_KEY') }}           AS CHN_KEY
    ,LOC.CHN_ID                                                 AS CHN_ID
    ,{{ get_key_with_fallback_value('LOC.CHNL_KEY') }}          AS CHNL_KEY
    ,LOC.CHNL_ID                                                AS CHNL_ID
    ,SRC.LOC_KEY                                                AS LOC_KEY
    ,SRC.LOC_ID                                                 AS LOC_ID
    ,LOC.LOC_TYP_CDE                                            AS LOC_TYP_CDE
    ,LOC.LOC_POSTAL_CDE                                         AS POSTAL_CDE
    ,LOC.LOC_STATE_PROVINCE_CDE                                 AS STATE_PROVINCE_CDE
    ,LOC.LOC_COUNTRY_CDE                                        AS COUNTRY_CDE
    ,{{ get_key_with_fallback_value('ITM.DIV_KEY') }}           AS DIV_KEY
    ,ITM.DIV_ID                                                 AS DIV_ID
    ,SRC.ITM_KEY                                                AS ITM_KEY
    ,SRC.ITM_ID                                                 AS ITM_ID
    ,SRC.ITMLOC_STTS_CDE                                        AS ITMLOC_STTS_CDE
    ,SRC.LCL_CNCY_CDE                                           AS LCL_CNCY_CDE
    ,SRC.F_OH_QTY                                               AS F_FACT_QTY
    ,SRC.F_OH_CST_LCL                                           AS F_FACT_CST
    ,SRC.F_OH_RTL_LCL                                           AS F_FACT_RTL
    ,SRC.F_IT_QTY                                               AS F_FACT_QTY1
    ,SRC.F_IT_CST_LCL                                           AS F_FACT_AMT1
    ,SRC.F_IT_RTL_LCL                                           AS F_FACT_AMT2
    ,CURRENT_TIMESTAMP                                          AS RCD_INS_TS
    ,CURRENT_TIMESTAMP                                          AS RCD_UPD_TS
FROM {{ ref('V_DWH_F_INV_ILD_B') }} SRC
LEFT JOIN {{ ref('V_DWH_D_ORG_LOC_LU') }} LOC ON SRC.LOC_ID = LOC.LOC_ID
LEFT JOIN {{ ref('V_DWH_D_PRD_ITM_LU') }} ITM ON SRC.ITM_ID = ITM.ITM_ID
/* only select records with EFF_END_DT >= (CURR_DAY - 1) to avoid joining the entire table. so we pick up:
1) Open records  (EFF_END_DT = MTH_END_DT).
2) Records that may have closed on current business date 
*/
WHERE SRC.EFF_END_DT >=TO_DATE('{{ robling_product.get_business_date() }}')-1
AND (SRC.F_OH_QTY <> 0
    OR SRC.F_OH_CST_LCL <> 0
    OR SRC.F_OH_RTL_LCL <> 0
    OR SRC.F_IT_QTY <> 0
    OR SRC.F_IT_CST_LCL <> 0
    OR SRC.F_IT_RTL_LCL <> 0
    )
-- ordering by EFF_START_DT, LOC_KEY for performance through Snowflake partitioning
ORDER BY SRC.EFF_START_DT
        ,SRC.LOC_KEY