{% set key_columns = ['FACT_CDE', 'DO_ID', 'ATTR_VARCHAR_COL15'] %}
{% set hash_columns = [
    'POST_DT','MEAS_DT','ORD_DOC_CREATED_DT','INV_DOC_CREATED_DT','CLOSED_DT','CANCLD_DT','RTRN_DT'
    ,'MIN_KEY','CHN_KEY','CHN_ID','CHNL_KEY','CHNL_ID','LOC_KEY','LOC_ID','DMND_LOC_KEY'
    ,'DMND_LOC_ID','FULFILL_LOC_KEY','FULFILL_LOC_ID','LOC_TYP_CDE','POSTAL_CDE','STATE_PROVINCE_CDE','COUNTRY_CDE','DIV_KEY'
    ,'DIV_ID','ITM_KEY','ITM_ID','ITMLOC_STTS_CDE','CO_ID','STTS_CDE','ORD_DOC_LN_STTS_CDE'
    ,'LCL_CNCY_CDE','F_FACT_QTY','F_FACT_CST','F_FACT_RTL','F_FACT_QTY1','F_FACT_AMT3','ATTR_VARCHAR_COL14', 'ATTR_VARCHAR_COL16','ATTR_VARCHAR_COL17'
] %}

{{
    config(
        materialized='custom_merge',
        unique_key=key_columns,
        key_columns=key_columns,
        hash_columns=hash_columns,
        insert_columns=key_columns + hash_columns,
        target_table = 'DM_F_MEAS_FACT_ILD_B',
        schema='DM_MERCH',
        tags = ['dm_f_co_fulmnt_meas_fact_ild'],
        pre_hook=["{{ start_script('dm_f_co_fulmnt_meas_fact_ild','RUNNING','NONE') }}"],
        post_hook = ["{{ load_recon_data('Fulfillment',recon_config_macro='mac_f_do_recon_script_sql', recon_step=2) }}"
                    ,"{{ log_script_success(this) }}"]
    )
}}
SELECT
    'CO_FULFILLED'                                                      AS FACT_CDE
    ,TO_DATE('{{ robling_product.get_business_date() }}')               AS POST_DT
    ,DO_HDR.DO_INVOICE_DT                                               AS MEAS_DT
    ,CO_LN.CO_ORD_DT                                                    AS ORD_DOC_CREATED_DT
    ,SRC.DO_CREATED_DT                                                  AS INV_DOC_CREATED_DT
    ,DO_HDR.DO_INVOICE_DT                                               AS CLOSED_DT
    ,CO_LN.CANCLD_DT                                                    AS CANCLD_DT
    ,CO_LN.RTRN_DT                                                      AS RTRN_DT
    ,COALESCE(SRC.DO_CREATED_MIN_KEY, 12359)                            AS MIN_KEY
    ,{{ get_key_with_fallback_value('LOC.CHN_KEY') }}                   AS CHN_KEY
    ,LOC.CHN_ID                                                         AS CHN_ID
    ,{{ get_key_with_fallback_value('LOC.CHNL_KEY') }}                  AS CHNL_KEY
    ,LOC.CHNL_ID                                                        AS CHNL_ID
    ,SRC.FULMNT_LOC_KEY                                                 AS LOC_KEY
    ,SRC.FULMNT_LOC_ID                                                  AS LOC_ID
    ,SRC.DMND_LOC_KEY                                                   AS DMND_LOC_KEY
    ,SRC.DMND_LOC_ID                                                    AS DMND_LOC_ID
    ,SRC.FULMNT_LOC_KEY                                                 AS FULFILL_LOC_KEY
    ,SRC.FULMNT_LOC_ID                                                  AS FULFILL_LOC_ID
    ,LOC.LOC_TYP_CDE                                                    AS LOC_TYP_CDE
    ,CO_LN.DLVRY_POSTAL_CDE                                             AS POSTAL_CDE
    ,CO_LN.DLVRY_STATE                                                  AS STATE_PROVINCE_CDE
    ,CO_LN.DLVRY_COUNTRY_CDE                                            AS COUNTRY_CDE
    ,{{ get_key_with_fallback_value('ITM.DIV_KEY') }}                   AS DIV_KEY
    ,ITM.DIV_ID                                                         AS DIV_ID
    ,SRC.ITM_KEY                                                        AS ITM_KEY
    ,SRC.ITM_ID                                                         AS ITM_ID
    ,SRC.ITMLOC_STTS_CDE                                                AS ITMLOC_STTS_CDE    
    ,SRC.CO_ID                                                          AS CO_ID
    ,SRC.DO_ID                                                          AS DO_ID
    ,DO_HDR.DO_HDR_STTS                                                 AS STTS_CDE
    ,CO_LN.CO_LN_ITM_STTS                                               AS ORD_DOC_LN_STTS_CDE
    ,DO_HDR.DO_HDR_STTS                                                 AS INV_DOC_LN_STTS_CDE
    ,SRC.LCL_CNCY_CDE                                                   AS LCL_CNCY_CDE
    ,SRC.F_FULFILL_QTY                                                  AS F_FACT_QTY
    ,(SRC.F_FULFILL_QTY * CO_LN.F_CO_UNIT_CST_LCL)                      AS F_FACT_CST
    ,(SRC.F_FULFILL_QTY * SRC.F_CO_PAID_UNIT_RTL_LCL)                   AS F_FACT_RTL
    ,SRC.F_DO_CURR_QTY                                                  AS F_FACT_QTY1
    ,SRC.F_DO_TAX_AMT_LCL                                               AS F_FACT_AMT3
    ,SRC.CO_LN_ID                                                       AS ATTR_VARCHAR_COL14
    ,SRC.DO_LN_ID                                                       AS ATTR_VARCHAR_COL15  --DO_LN_ID is a part of PK for the DWH table and is used as part of the merge condition as well.
    ,COUNT(DISTINCT SRC.DO_ID) OVER(PARTITION BY SRC.CO_ID)             AS ATTR_VARCHAR_COL16 -- Number of fulfillments per order 
    ,CO_FULFILL_STTS.SPLIT_SHPMNT_TYPE                                  AS ATTR_VARCHAR_COL17 -- Co Fulfillment status : Perfectly Splitted, Partially Fulfilled, Mismatched
FROM {{ source('DO_DWH_V','V_DWH_F_ECOMM_DO_LN_ITM_B') }} SRC
INNER JOIN {{ ref('V_DWH_F_ECOMM_DO_HDR_B') }} DO_HDR ON  SRC.DO_ID = DO_HDR.DO_ID
INNER JOIN {{ ref('V_DWH_F_ECOMM_CO_LN_ITM_B') }} CO_LN ON  SRC.CO_LN_ID = CO_LN.CO_LN_ID 
INNER JOIN {{ ref('V_CFG_F_ECOMM_SPLIT_SHPMNT_TYP') }} CO_FULFILL_STTS ON SRC.CO_LN_ID = CO_FULFILL_STTS.CO_LN_ID
LEFT JOIN {{ ref('V_DWH_D_ORG_LOC_LU') }} LOC ON SRC.FULMNT_LOC_ID = LOC.LOC_ID
LEFT JOIN {{ ref('V_DWH_D_PRD_ITM_LU') }} ITM ON SRC.ITM_ID = ITM.ITM_ID
WHERE (
        SRC.F_FULFILL_QTY <> 0
    )
    -- Exclude deleted records
   AND SRC.IS_DELETED = '0'
   AND DO_HDR.IS_DELETED = '0'
   AND CO_LN.IS_DELETED = '0'
-- ordering by DO_INVOICE_DT, FULMNT_LOC_KEY for performance through Snowflake partitioning
ORDER BY DO_HDR.DO_INVOICE_DT
        ,SRC.FULMNT_LOC_KEY 
