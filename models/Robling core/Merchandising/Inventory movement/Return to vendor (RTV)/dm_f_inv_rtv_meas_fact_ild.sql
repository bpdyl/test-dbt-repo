{{
    config(
        materialized='delete_insert_into_dm',
        schema='DM_MERCH',
        target_table = 'DM_F_MEAS_FACT_ILD_B',
        tags = ['dm_f_inv_rtv_meas_fact_ild'],
        fact_cde = 'RTV',
        delete_condition = "POST_DT IN (SELECT DISTINCT POST_DT FROM DW_TMP.TMP_F_INV_RTV_SUP_ILD_B)",
        pre_hook=["{{ start_script('dm_f_inv_rtv_meas_fact_ild','RUNNING','NONE') }}"],
        post_hook = ["{{ load_recon_data('RTV',recon_config_macro='mac_f_rtv_recon_script_sql', recon_step=2) }}"
                    ,"{{ log_script_success(this) }}"]
    )
}}
SELECT   
    'RTV'                                                   AS FACT_CDE
    ,SRC.POST_DT                                            AS POST_DT
    ,SRC.TXN_DT                                             AS MEAS_DT
    ,{{ get_key_with_fallback_value('LOC.CHN_KEY') }}       AS CHN_KEY
    ,LOC.CHN_ID                                             AS CHN_ID
    ,{{ get_key_with_fallback_value('LOC.CHNL_KEY') }}      AS CHNL_KEY
    ,LOC.CHNL_ID                                            AS CHNL_ID
    ,SRC.LOC_KEY                                            AS LOC_KEY
    ,SRC.LOC_ID                                             AS LOC_ID
    ,LOC.LOC_TYP_CDE                                        AS LOC_TYP_CDE
    ,LOC.LOC_POSTAL_CDE                                     AS POSTAL_CDE
    ,LOC.LOC_STATE_PROVINCE_CDE                             AS STATE_PROVINCE_CDE
    ,LOC.LOC_COUNTRY_CDE                                    AS COUNTRY_CDE
    ,{{ get_key_with_fallback_value('ITM.DIV_KEY') }}       AS DIV_KEY
    ,ITM.DIV_ID                                             AS DIV_ID
    ,SRC.ITM_KEY                                            AS ITM_KEY
    ,SRC.ITM_ID                                             AS ITM_ID
    ,SRC.SUP_KEY                                            AS SUP_KEY
    ,SRC.SUP_ID                                             AS SUP_ID
    ,SRC.RTV_RSN                                            AS RSN_ID
    ,SRC.ITMLOC_STTS_CDE                                    AS ITMLOC_STTS_CDE
    ,SRC.F_RTV_QTY                                          AS F_FACT_QTY
    ,SRC.F_RTV_CST_LCL                                      AS F_FACT_CST
    ,SRC.F_RTV_RTL_LCL                                      AS F_FACT_RTL
    ,SRC.LCL_CNCY_CDE                                       AS LCL_CNCY_CDE
    ,CURRENT_TIMESTAMP                                      AS RCD_INS_TS
    ,CURRENT_TIMESTAMP                                      AS RCD_UPD_TS
FROM {{ source('RTV_SRC_DM','V_DWH_F_INV_RTV_SUP_ILD_B') }} SRC
LEFT JOIN {{ ref('V_DWH_D_ORG_LOC_LU') }} LOC ON SRC.LOC_ID = LOC.LOC_ID
LEFT JOIN {{ ref('V_DWH_D_PRD_ITM_LU') }} ITM ON SRC.ITM_ID = ITM.ITM_ID
WHERE (
        SRC.F_RTV_QTY <> 0
        OR SRC.F_RTV_CST <> 0
        OR SRC.F_RTV_RTL <> 0
    )
-- ordering by TXN_DT, LOC_KEY for performance through Snowflake partitioning
ORDER BY SRC.TXN_DT
        ,SRC.LOC_KEY
