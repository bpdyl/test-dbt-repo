{{
    config(
        materialized='delete_insert_into_dm',
        schema='DM_MERCH',
        target_table = 'DM_F_MEAS_FACT_ILD_B',
        tags = ['dm_f_rig_inv_elgbl_meas_fact_ild'],
        fact_cde='RIG_INV_ELGBL',
        pre_hook=["{{ start_script('dm_f_rig_inv_elgbl_meas_fact_ild','RUNNING','NONE') }}"],
        post_hook = ["{{ log_script_success(this) }}"]
    )
}}
SELECT
    'RIG_INV_ELGBL'                                            AS FACT_CDE
    ,TO_DATE('{{ robling_product.get_business_date() }}')      AS MEAS_DT
    ,TO_DATE('{{ robling_product.get_business_date() }}')      AS POST_DT
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
    ,SRC.ELGBL_EFF_START_DT                                    AS ATTR_DT_COL2
    ,SRC.ELGBL_EFF_END_DT                                      AS ATTR_DT_COL3
    ,CURRENT_TIMESTAMP()                                       AS RCD_INS_TS                                
    ,CURRENT_TIMESTAMP()                                       AS RCD_UPD_TS
FROM {{ ref('TMP_RIG_F_INV_ELGBL_IL_B') }} SRC
LEFT JOIN {{ ref('V_DWH_D_ORG_LOC_LU') }} LOC ON SRC.LOC_ID = LOC.LOC_ID
LEFT JOIN {{ ref('V_DWH_D_PRD_ITM_LU') }} ITM ON SRC.ITM_ID = ITM.ITM_ID
-- ordering by POST_DT, LOC_KEY for performance through Snowflake partitioning
ORDER BY 
    POST_DT
    ,SRC.LOC_KEY