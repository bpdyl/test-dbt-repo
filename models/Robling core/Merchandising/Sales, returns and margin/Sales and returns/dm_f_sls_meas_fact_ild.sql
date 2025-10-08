{% set key_columns = ['FACT_CDE','TXN_ID', 'VERSION_ID', 'POST_DT', 'ATTR_VARCHAR_COL12'] %}
{% set hash_columns = [
    'MEAS_DT', 'ORD_DOC_CREATED_DT', 'CLOSED_DT', 'CANCLD_DT', 'RTRN_DT',
    'MIN_KEY', 'CHN_KEY', 'CHN_ID', 'CHNL_KEY', 'CHNL_ID',
    'LOC_KEY', 'LOC_ID', 'DMND_LOC_KEY', 'DMND_LOC_ID',
    'LOC_TYP_CDE', 'POSTAL_CDE', 'STATE_PROVINCE_CDE', 'COUNTRY_CDE',
    'DIV_KEY', 'DIV_ID', 'ITM_KEY', 'ITM_ID', 'SUP_KEY', 'SUP_ID',
    'EMP_ID', 'ITMLOC_STTS_CDE', 'RTL_TYP_CDE', 'RTRN_FLG', 'RSN_ID',
    'CO_ID', 'DO_ID', 'LCL_CNCY_CDE', 'ATTR_DT_COL1',
    'F_FACT_QTY', 'F_FACT_CST', 'F_FACT_RTL', 'F_FACT_AMT1', 'F_FACT_AMT2', 'F_FACT_AMT3',
    'ATTR_VARCHAR_COL13'
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
        tags = ['dm_f_sls_meas_fact_ild'],
        pre_hook=["{{ start_script('dm_f_sls_meas_fact_ild','RUNNING','NONE') }}"],
        post_hook = ["{{ load_recon_data('Sales',recon_config_macro='mac_f_sls_recon_script_sql', recon_step=2) }}"
                    ,"{{ log_script_success(this) }}"]
    )
}}
SELECT   
    'SLS'                                                   AS FACT_CDE
    ,TXN_ITM.VERSION_ID                                     AS VERSION_ID
    ,TXN_ITM.POST_DT                                        AS POST_DT
    ,TXN_ITM.TXN_DT                                         AS MEAS_DT
    ,CO.CO_ORD_DT                                           AS ORD_DOC_CREATED_DT
    ,CO_ITM.INVOICE_DT                                      AS CLOSED_DT
    ,CO_ITM.CANCLD_DT                                       AS CANCLD_DT
    ,CO_ITM.RTRN_DT                                         AS RTRN_DT
    ,TXN_ITM.TXN_MIN_KEY                                    AS MIN_KEY
    ,{{ get_key_with_fallback_value('LOC.CHN_KEY') }}       AS CHN_KEY
    ,LOC.CHN_ID                                             AS CHN_ID
    ,{{ get_key_with_fallback_value('LOC.CHNL_KEY') }}      AS CHNL_KEY
    ,LOC.CHNL_ID                                            AS CHNL_ID
    ,TXN_ITM.LOC_KEY                                        AS LOC_KEY
    ,TXN_ITM.LOC_ID                                         AS LOC_ID
    ,COALESCE(TXN_ITM.DMND_LOC_KEY, CO.DMND_LOC_KEY)        AS DMND_LOC_KEY
    ,COALESCE(TXN_ITM.DMND_LOC_ID, CO.DMND_LOC_ID)          AS DMND_LOC_ID
    ,LOC.LOC_TYP_CDE                                        AS LOC_TYP_CDE
    ,LOC.LOC_POSTAL_CDE                                     AS POSTAL_CDE
    ,LOC.LOC_STATE_PROVINCE_CDE                             AS STATE_PROVINCE_CDE
    ,LOC.LOC_COUNTRY_CDE                                    AS COUNTRY_CDE
    ,{{ get_key_with_fallback_value('ITM.DIV_KEY') }}       AS DIV_KEY
    ,ITM.DIV_ID                                             AS DIV_ID
    ,TXN_ITM.ITM_KEY                                        AS ITM_KEY
    ,TXN_ITM.ITM_ID                                         AS ITM_ID
    ,{{ get_key_with_fallback_value('ITM.SUP_KEY') }}       AS SUP_KEY
    ,ITM.SUP_ID                                             AS SUP_ID
    ,HDR.POS_SALESPERSON_ID                                 AS EMP_ID
    ,TXN_ITM.ITMLOC_STTS_CDE                                AS ITMLOC_STTS_CDE
    ,TXN_ITM.RTL_TYP_CDE                                    AS RTL_TYP_CDE
    ,TXN_ITM.RTRN_FLG                                       AS RTRN_FLG
    ,TXN_ITM.RTRN_RSN                                       AS RSN_ID
    ,TXN_ITM.CO_ID                                          AS CO_ID
    ,TXN_ITM.DO_ID                                          AS DO_ID
    ,TXN_ITM.TXN_ID                                         AS TXN_ID
    ,TXN_ITM.LCL_CNCY_CDE                                   AS LCL_CNCY_CDE
    ,CO_ITM.BACK_ORD_DT                                     AS ATTR_DT_COL1
    ,TXN_ITM.F_SLS_QTY                                      AS F_FACT_QTY
    ,TXN_ITM.F_SLS_CST_LCL                                  AS F_FACT_CST
    ,TXN_ITM.F_SLS_RTL_LCL                                  AS F_FACT_RTL
    ,TXN_ITM.F_TOT_DSC_AMT_LCL                              AS F_FACT_AMT1
    ,TXN_ITM.F_EMP_DSC_AMT_LCL                              AS F_FACT_AMT2
    ,TXN_ITM.F_SLS_TAX_AMT_LCL                              AS F_FACT_AMT3
    ,TXN_ITM.TXN_LN_ID                                      AS ATTR_VARCHAR_COL12
    ,TXN_ITM.IS_CURRENT                                     AS ATTR_VARCHAR_COL13
    ,CURRENT_TIMESTAMP                                      AS RCD_INS_TS
    ,CURRENT_TIMESTAMP                                      AS RCD_UPD_TS
FROM {{ source('SLS_SRC_DM','V_DWH_F_SLS_TXN_LN_ITM_B') }} TXN_ITM
LEFT JOIN {{ source('SLS_SRC_DM','V_DWH_F_SLS_TXN_ATTR_LU') }} HDR 
ON TXN_ITM.TXN_ID = HDR.TXN_ID AND TXN_ITM.POST_DT = HDR.POST_DT
LEFT JOIN {{ ref('V_DWH_D_ORG_LOC_LU') }} LOC ON TXN_ITM.LOC_ID = LOC.LOC_ID
LEFT JOIN {{ ref('V_DWH_D_PRD_ITM_LU') }} ITM ON TXN_ITM.ITM_ID = ITM.ITM_ID
LEFT JOIN DW_DWH_V.V_DWH_F_ECOMM_CO_HDR_B CO ON TXN_ITM.CO_ID = CO.CO_ID
LEFT JOIN DW_DWH_V.V_DWH_F_ECOMM_CO_LN_ITM_B CO_ITM ON TXN_ITM.CO_ID = CO_ITM.CO_ID AND TXN_ITM.CO_LN_ID = CO_ITM.CO_LN_ID
WHERE (
        F_SLS_QTY <> 0
        OR F_SLS_CST_LCL <> 0
        OR F_SLS_RTL_LCL <> 0
    )
-- ordering by TXN_DT, LOC_KEY for performance through Snowflake partitioning
ORDER BY TXN_ITM.TXN_DT
        ,TXN_ITM.LOC_KEY
