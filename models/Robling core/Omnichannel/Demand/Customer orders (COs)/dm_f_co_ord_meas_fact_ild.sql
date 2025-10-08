{% set key_columns = ['FACT_CDE', 'CO_ID', 'ATTR_VARCHAR_COL14', 'ATTR_VARCHAR_COL15'] %}
{% set hash_columns = [
    'POST_DT', 'MEAS_DT', 'ORD_DOC_CREATED_DT','INV_DOC_CREATED_DT', 'CLOSED_DT'
    , 'CANCLD_DT', 'RTRN_DT', 'MIN_KEY', 'CHN_KEY', 'CHN_ID', 'CHNL_KEY', 'CHNL_ID', 'LOC_KEY'
    , 'LOC_ID', 'DMND_LOC_KEY', 'DMND_LOC_ID', 'FULFILL_LOC_KEY','FULFILL_LOC_ID', 'DO_ID','LOC_TYP_CDE', 'POSTAL_CDE', 'STATE_PROVINCE_CDE'
    , 'COUNTRY_CDE', 'DIV_KEY', 'DIV_ID', 'ITM_KEY', 'ITM_ID', 'ITMLOC_STTS_CDE', 'STTS_CDE'
    , 'ORD_DOC_LN_STTS_CDE', 'LCL_CNCY_CDE', 'F_FACT_QTY', 'F_FACT_CST', 'F_FACT_RTL', 'F_FACT_QTY1', 'F_FACT_QTY2', 'F_FACT_AMT1'
    , 'F_FACT_AMT3', 'ATTR_DT_COL1','ATTR_VARCHAR_COL16','ATTR_VARCHAR_COL17'
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
        tags = ['dm_f_co_ord_meas_fact_ild'],
        pre_hook=["{{ start_script('dm_f_co_ord_meas_fact_ild','RUNNING','NONE') }}"],
        post_hook = ["{{ load_recon_data('Customer Order',recon_config_macro='mac_f_co_recon_script_sql', recon_step=2) }}"
                    ,"{{ log_script_success(this) }}"]
    )
}}

{% if check_if_model_exists('V_DWH_F_ECOMM_DO_LN_ITM_B') %}

SELECT   
    'CO_ORD'                                                                                            AS FACT_CDE
    ,TO_DATE('{{ robling_product.get_business_date() }}')                                               AS POST_DT
    ,SRC.CO_ORD_DT                                                                                      AS MEAS_DT
    ,SRC.CO_ORD_DT                                                                                      AS ORD_DOC_CREATED_DT
    ,DO_HDR_LN.DO_CREATED_DT                                                                            AS INV_DOC_CREATED_DT 
    ,DO_HDR_LN.DO_INVOICE_DT                                                                            AS CLOSED_DT
    ,SRC.CANCLD_DT                                                                                      AS CANCLD_DT
    ,SRC.RTRN_DT                                                                                        AS RTRN_DT
    ,COALESCE(SRC.CO_ORD_MIN_KEY, 12359)                                                                AS MIN_KEY
    ,{{ get_key_with_fallback_value('DMND_LOC.CHN_KEY') }}                                              AS CHN_KEY
    ,DMND_LOC.CHN_ID                                                                                    AS CHN_ID  
    ,{{ get_key_with_fallback_value('DMND_LOC.CHNL_KEY') }}                                             AS CHNL_KEY
    ,DMND_LOC.CHNL_ID                                                                                   AS CHNL_ID
    ,SRC.DMND_LOC_KEY                                                                                   AS LOC_KEY
    ,SRC.DMND_LOC_ID                                                                                    AS LOC_ID
    ,SRC.DMND_LOC_KEY                                                                                   AS DMND_LOC_KEY
    ,SRC.DMND_LOC_ID                                                                                    AS DMND_LOC_ID
    ,{{ get_key_with_fallback_value('DO_HDR_LN.FULMNT_LOC_KEY') }}                                      AS FULFILL_LOC_KEY
    ,DO_HDR_LN.FULMNT_LOC_ID                                                                            AS FULFILL_LOC_ID
    ,DMND_LOC.LOC_TYP_CDE                                                                               AS LOC_TYP_CDE
    ,SRC.DLVRY_POSTAL_CDE                                                                               AS POSTAL_CDE
    ,SRC.DLVRY_STATE                                                                                    AS STATE_PROVINCE_CDE
    ,SRC.DLVRY_COUNTRY_CDE                                                                              AS COUNTRY_CDE
    ,{{ get_key_with_fallback_value('ITM.DIV_KEY') }}                                                   AS DIV_KEY
    ,ITM.DIV_ID                                                                                         AS DIV_ID
    ,SRC.ITM_KEY                                                                                        AS ITM_KEY
    ,SRC.ITM_ID                                                                                         AS ITM_ID
    ,SRC.ITMLOC_STTS_CDE                                                                                AS ITMLOC_STTS_CDE    
    ,SRC.CO_ID                                                                                          AS CO_ID
    ,DO_HDR_LN.DO_ID                                                                                    AS DO_ID 
    ,SRC.CO_LN_ITM_STTS                                                                                 AS STTS_CDE
    ,SRC.CO_LN_ITM_STTS                                                                                 AS ORD_DOC_LN_STTS_CDE
    ,SRC.LCL_CNCY_CDE                                                                                   AS LCL_CNCY_CDE
    ,COALESCE(DO_HDR_LN.F_DO_CURR_QTY , SRC.F_CO_ORD_QTY)                                               AS F_FACT_QTY
    ,COALESCE(DO_HDR_LN.F_DO_CURR_QTY * SRC.F_CO_UNIT_CST_LCL, SRC.F_CO_ORD_CST_LCL)                    AS F_FACT_CST
    ,COALESCE(DO_HDR_LN.F_DO_CURR_QTY * SRC.F_CO_PAID_UNIT_RTL_LCL ,SRC.F_CO_ORD_RTL_LCL)               AS F_FACT_RTL
    ,COALESCE(DO_HDR_LN.F_DO_CURR_QTY, SRC.F_CO_ALLOCATED_QTY)                                          AS F_FACT_QTY1 -- CO Allocated Qty
    ,COALESCE(DO_HDR_LN.F_FULFILL_QTY, SRC.F_CO_FULFILL_QTY)                                            AS F_FACT_QTY2 -- Fulfilled Qty for this line item 
    ,(SRC.F_CO_DSC_AMT_LCL) * 
        COALESCE(DO_HDR_LN.F_DO_CURR_QTY, SRC.F_CO_ORD_QTY) /  NULLIFZERO(SRC.F_CO_ORD_QTY)             AS F_FACT_AMT1  -- CO Discount
    ,SRC.F_CO_TAX_AMT_LCL * 
        COALESCE(DO_HDR_LN.F_DO_CURR_QTY, SRC.F_CO_ORD_QTY) /  NULLIFZERO(SRC.F_CO_ORD_QTY)             AS F_FACT_AMT3  -- CO Tax amt
    ,SRC.BACK_ORD_DT                                                                                    AS ATTR_DT_COL1 -- Back ordered Date
    ,SRC.CO_LN_ID                                                                                       AS ATTR_VARCHAR_COL14  --CO_LN_ID is a part of PK for the DWH table and is used as part of the merge condition as well.
    ,COALESCE(DO_HDR_LN.DO_LN_ID,'-1')                                                                  AS ATTR_VARCHAR_COL15 -- After splitting the CO_LN into DO level, DO_LN_ID is required as part of pk for merging
    ,COUNT(DISTINCT DO_HDR_LN.DO_ID) OVER(PARTITION BY DO_HDR_LN.CO_ID)                                 AS ATTR_VARCHAR_COL16 -- Number of fulfillments per order 
    ,CO_FULFILL_STTS.SPLIT_SHPMNT_TYPE                                                                  AS ATTR_VARCHAR_COL17 -- Co Fulfillment Split shipment type : Perfectly Splitted, Partially Fulfilled, Mismatched
    ,CURRENT_TIMESTAMP                                                                                  AS RCD_INS_TS
    ,CURRENT_TIMESTAMP                                                                                  AS RCD_UPD_TS
FROM {{ source('CO_SRC_DM','V_DWH_F_ECOMM_CO_LN_ITM_B') }} SRC 
LEFT OUTER JOIN {{ ref('TMP_F_CO_LN_MERGE_DO_LN_MTX') }} MTX 
        ON SRC.CO_LN_ID = MTX.CO_LN_ID
LEFT OUTER JOIN
        {{ ref('V_CFG_F_ECOMM_DO_HDR_LN_FILTERED') }} DO_HDR_LN ON MTX.CO_LN_ID = DO_HDR_LN.CO_LN_ID
LEFT OUTER JOIN 
        {{ ref('V_CFG_F_ECOMM_SPLIT_SHPMNT_TYP') }} CO_FULFILL_STTS ON SRC.CO_LN_ID = CO_FULFILL_STTS.CO_LN_ID
LEFT JOIN {{ ref('V_DWH_D_ORG_LOC_LU') }} DMND_LOC ON SRC.DMND_LOC_ID = DMND_LOC.LOC_ID
LEFT JOIN {{ ref('V_DWH_D_PRD_ITM_LU') }} ITM ON SRC.ITM_ID = ITM.ITM_ID
WHERE (
        SRC.F_CO_ORD_QTY <> 0
        OR SRC.F_CO_ORD_CST_LCL <> 0
        OR SRC.F_CO_ORD_RTL_LCL <> 0
    )
UNION ALL 
SELECT   
    'CO_ORD'                                                                                            AS FACT_CDE
    ,TO_DATE('{{ robling_product.get_business_date() }}')                                               AS POST_DT
    ,SRC.CO_ORD_DT                                                                                      AS MEAS_DT
    ,SRC.CO_ORD_DT                                                                                      AS ORD_DOC_CREATED_DT
    ,NULL                                                                                               AS INV_DOC_CREATED_DT 
    ,NULL                                                                                               AS CLOSED_DT
    ,SRC.CANCLD_DT                                                                                      AS CANCLD_DT
    ,SRC.RTRN_DT                                                                                        AS RTRN_DT
    ,COALESCE(SRC.CO_ORD_MIN_KEY, 12359)                                                                AS MIN_KEY
    ,{{ get_key_with_fallback_value('DMND_LOC.CHN_KEY') }}                                              AS CHN_KEY
    ,DMND_LOC.CHN_ID                                                                                    AS CHN_ID  
    ,{{ get_key_with_fallback_value('DMND_LOC.CHNL_KEY') }}                                             AS CHNL_KEY
    ,DMND_LOC.CHNL_ID                                                                                   AS CHNL_ID
    ,SRC.DMND_LOC_KEY                                                                                   AS LOC_KEY
    ,SRC.DMND_LOC_ID                                                                                    AS LOC_ID
    ,SRC.DMND_LOC_KEY                                                                                   AS DMND_LOC_KEY
    ,SRC.DMND_LOC_ID                                                                                    AS DMND_LOC_ID
    ,{{ get_key_with_fallback_value('NULL') }}                                                          AS FULFILL_LOC_KEY
    ,'-1'                                                                                               AS FULFILL_LOC_ID
    ,DMND_LOC.LOC_TYP_CDE                                                                               AS LOC_TYP_CDE
    ,SRC.DLVRY_POSTAL_CDE                                                                               AS POSTAL_CDE
    ,SRC.DLVRY_STATE                                                                                    AS STATE_PROVINCE_CDE
    ,SRC.DLVRY_COUNTRY_CDE                                                                              AS COUNTRY_CDE
    ,{{ get_key_with_fallback_value('ITM.DIV_KEY') }}                                                   AS DIV_KEY
    ,ITM.DIV_ID                                                                                         AS DIV_ID
    ,SRC.ITM_KEY                                                                                        AS ITM_KEY
    ,SRC.ITM_ID                                                                                         AS ITM_ID
    ,SRC.ITMLOC_STTS_CDE                                                                                AS ITMLOC_STTS_CDE    
    ,SRC.CO_ID                                                                                          AS CO_ID
    ,NULL                                                                                               AS DO_ID 
    ,SRC.CO_LN_ITM_STTS                                                                                 AS STTS_CDE
    ,SRC.CO_LN_ITM_STTS                                                                                 AS ORD_DOC_LN_STTS_CDE
    ,SRC.LCL_CNCY_CDE                                                                                   AS LCL_CNCY_CDE
    ,SRC.F_CO_ORD_QTY - COALESCE(SRC.F_CO_ALLOCATED_QTY,0)                                              AS F_FACT_QTY -- CO Open Quantity
    ,(SRC.F_CO_ORD_QTY - COALESCE(SRC.F_CO_ALLOCATED_QTY, 0)) * SRC.F_CO_UNIT_CST_LCL                   AS F_FACT_CST -- CO Open Cost
    ,(SRC.F_CO_ORD_QTY - COALESCE(SRC.F_CO_ALLOCATED_QTY, 0)) * SRC.F_CO_PAID_UNIT_RTL_LCL              AS F_FACT_RTL -- CO Open Retail 
    ,NULL                                                                                               AS F_FACT_QTY1 
    ,NULL                                                                                               AS F_FACT_QTY2
    ,(SRC.F_CO_DSC_AMT_LCL) /  NULLIFZERO(SRC.F_CO_ORD_QTY) * 
            (SRC.F_CO_ORD_QTY - COALESCE(SRC.F_CO_ALLOCATED_QTY, 0) - COALESCE(SRC.F_CO_CANCLD_QTY, 0)) AS F_FACT_AMT1  -- CO Open Discount
    ,(SRC.F_CO_TAX_AMT_LCL) /  NULLIFZERO(SRC.F_CO_ORD_QTY) * 
            (SRC.F_CO_ORD_QTY - COALESCE(SRC.F_CO_ALLOCATED_QTY, 0) - COALESCE(SRC.F_CO_CANCLD_QTY, 0)) AS F_FACT_AMT3  -- CO Open Tax amt
    ,SRC.BACK_ORD_DT                                                                                    AS ATTR_DT_COL1 -- Back ordered Date
    ,SRC.CO_LN_ID                                                                                       AS ATTR_VARCHAR_COL14  --CO_LN_ID is a part of PK for the DWH table and is used as part of the merge condition as well.
    ,'-1'                                                                                               AS ATTR_VARCHAR_COL15 -- Mapped default value as -1 when inserting unallocated lines , mapping NULL would cause failure during merge
    ,NULL                                                                                               AS ATTR_VARCHAR_COL16
    ,CO_FULFILL_STTS.SPLIT_SHPMNT_TYPE                                                                  AS ATTR_VARCHAR_COL17 -- Co Fulfillment Split shipment type : Perfectly Splitted, Partially Fulfilled, Mismatched
    ,CURRENT_TIMESTAMP                                                                                  AS RCD_INS_TS
    ,CURRENT_TIMESTAMP                                                                                  AS RCD_UPD_TS
-- ordering by CO_ORD_DT, DMND_LOC_KEY for performance through Snowflake partitioning
FROM {{ ref('V_DWH_F_ECOMM_CO_LN_ITM_B') }} SRC 
INNER JOIN {{ ref('TMP_F_CO_LN_MERGE_DO_LN_MTX') }} MTX
        ON SRC.CO_LN_ID = MTX.CO_LN_ID 
LEFT OUTER JOIN {{ ref('V_CFG_F_ECOMM_SPLIT_SHPMNT_TYP') }} CO_FULFILL_STTS ON SRC.CO_LN_ID = CO_FULFILL_STTS.CO_LN_ID
LEFT OUTER JOIN {{ ref('V_DWH_D_ORG_LOC_LU') }} DMND_LOC ON SRC.DMND_LOC_ID = DMND_LOC.LOC_ID 
LEFT OUTER JOIN {{ ref('V_DWH_D_PRD_ITM_LU') }} ITM ON SRC.ITM_ID = ITM.ITM_ID 
INNER JOIN {{ ref('V_DWH_F_ECOMM_CO_HDR_B') }} CO_HDR ON SRC.CO_ID = CO_HDR.CO_ID 
    WHERE ( 
        SRC.F_CO_ORD_QTY - COALESCE(SRC.F_CO_ALLOCATED_QTY,0) <> 0
        AND SRC.IS_DELETED = '0'
    )
ORDER BY MEAS_DT
        ,DMND_LOC_KEY 

{% else %}

SELECT   
    'CO_ORD'                                                            AS FACT_CDE
    ,TO_DATE('{{ robling_product.get_business_date() }}')               AS POST_DT
    ,SRC.CO_ORD_DT                                                      AS MEAS_DT
    ,SRC.CO_ORD_DT                                                      AS ORD_DOC_CREATED_DT
    ,NULL                                                               AS INV_DOC_CREATED_DT
    ,SRC.INVOICE_DT                                                     AS CLOSED_DT
    ,SRC.CANCLD_DT                                                      AS CANCLD_DT
    ,SRC.RTRN_DT                                                        AS RTRN_DT
    ,COALESCE(SRC.CO_ORD_MIN_KEY, 12359)                                AS MIN_KEY
    ,{{ get_key_with_fallback_value('DMND_LOC.CHN_KEY') }}              AS CHN_KEY
    ,DMND_LOC.CHN_ID                                                    AS CHN_ID  
    ,{{ get_key_with_fallback_value('DMND_LOC.CHNL_KEY') }}             AS CHNL_KEY
    ,DMND_LOC.CHNL_ID                                                   AS CHNL_ID
    ,SRC.DMND_LOC_KEY                                                   AS LOC_KEY
    ,SRC.DMND_LOC_ID                                                    AS LOC_ID
    ,SRC.DMND_LOC_KEY                                                   AS DMND_LOC_KEY
    ,SRC.DMND_LOC_ID                                                    AS DMND_LOC_ID
    ,{{ get_key_with_fallback_value('NULL') }}                          AS FULFILL_LOC_KEY
    ,'-1'                                                               AS FULFILL_LOC_ID
    ,DMND_LOC.LOC_TYP_CDE                                               AS LOC_TYP_CDE
    ,SRC.DLVRY_POSTAL_CDE                                               AS POSTAL_CDE
    ,SRC.DLVRY_STATE                                                    AS STATE_PROVINCE_CDE
    ,SRC.DLVRY_COUNTRY_CDE                                              AS COUNTRY_CDE
    ,{{ get_key_with_fallback_value('ITM.DIV_KEY') }}                   AS DIV_KEY
    ,ITM.DIV_ID                                                         AS DIV_ID
    ,SRC.ITM_KEY                                                        AS ITM_KEY
    ,SRC.ITM_ID                                                         AS ITM_ID
    ,SRC.ITMLOC_STTS_CDE                                                AS ITMLOC_STTS_CDE    
    ,SRC.CO_ID                                                          AS CO_ID
    ,NULL                                                               AS DO_ID
    ,SRC.CO_LN_ITM_STTS                                                 AS STTS_CDE
    ,SRC.CO_LN_ITM_STTS                                                 AS ORD_DOC_LN_STTS_CDE
    ,SRC.LCL_CNCY_CDE                                                   AS LCL_CNCY_CDE
    ,SRC.F_CO_ORD_QTY                                                   AS F_FACT_QTY
    ,SRC.F_CO_ORD_CST_LCL                                               AS F_FACT_CST
    ,SRC.F_CO_ORD_RTL_LCL                                               AS F_FACT_RTL
    ,SRC.F_CO_DSC_AMT_LCL                                               AS F_FACT_AMT1
    ,SRC.F_CO_TAX_AMT_LCL                                               AS F_FACT_AMT3
    ,SRC.BACK_ORD_DT                                                    AS ATTR_DT_COL1
    ,SRC.CO_LN_ID                                                       AS ATTR_VARCHAR_COL14  --CO_LN_ID is a part of PK for the DWH table and is used as part of the merge condition as well.
    ,NULL                                                               AS ATTR_VARCHAR_COL15  {# Mapping NULL in attr varchar col 15 column so that whenever fulfillment subject area is not implemented it doesn't break the code#}
    ,NULL                                                               AS ATTR_VARCHAR_COL16 {# Mapping NULL in attr varchar col 16 column so that whenever fulfillment subject area is not implemented it doesn't break the code#}
    ,NULL                                                               AS ATTR_VARCHAR_COL17 -- Co Fulfillment Split shipment type : Perfectly Splitted, Partially Fulfilled, Mismatched
    ,CURRENT_TIMESTAMP                                                  AS RCD_INS_TS
    ,CURRENT_TIMESTAMP                                                  AS RCD_UPD_TS
FROM {{ source('CO_SRC_DM','V_DWH_F_ECOMM_CO_LN_ITM_B') }} SRC
LEFT JOIN {{ ref('V_DWH_D_ORG_LOC_LU') }} DMND_LOC ON SRC.DMND_LOC_ID = DMND_LOC.LOC_ID
LEFT JOIN {{ ref('V_DWH_D_PRD_ITM_LU') }} ITM ON SRC.ITM_ID = ITM.ITM_ID
WHERE (
        SRC.F_CO_ORD_QTY <> 0
        OR SRC.F_CO_ORD_CST_LCL <> 0
        OR SRC.F_CO_ORD_RTL_LCL <> 0
    )
-- ordering by CO_ORD_DT, DMND_LOC_KEY for performance through Snowflake partitioning
ORDER BY SRC.CO_ORD_DT
        ,SRC.DMND_LOC_KEY 
{% endif %}