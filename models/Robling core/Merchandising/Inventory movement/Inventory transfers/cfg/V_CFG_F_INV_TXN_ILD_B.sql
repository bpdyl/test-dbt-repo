/*
  The purpose of this view to map different types of inventory transaction type to its respective fact codes
  For example:
    i.e txn_typ 400 -> INV_TSF_OUT
        txn_typ 401 -> INV_TSF_IN
  This also makes this view configurable based on the values seeded in the DW_CFG.CFG_INV_TXN_LU table.
*/
{{ config(
    materialized='view',
    alias='V_CFG_F_INV_TXN_ILD_B',
    schema='DW_CFG',
    tags=['dm_f_inv_txn_meas_fact_ild']
) }}
SELECT
    CFG.FACT_CDE                                                AS FACT_CDE
    ,INV_TXN.POST_DT                                            AS POST_DT
    ,INV_TXN.TXN_DT                                             AS MEAS_DT
    ,NULL                                                       AS ORD_DOC_CREATED_DT
    ,NULL                                                       AS ORD_DOC_DUE_DT
    ,NULL                                                       AS INV_DOC_CREATED_DT
    ,NULL                                                       AS INV_DOC_DUE_DT
    ,INV_TXN.TXN_DT                                             AS SHIPPED_DT
    ,NULL                                                       AS RCPT_DT
    ,NULL                                                       AS CLOSED_DT
    ,NULL                                                       AS CANCLD_DT
    ,NULL                                                       AS RTRN_DT
    ,NULL                                                       AS MIN_KEY
    ,LOC.CHN_KEY                                                AS CHN_KEY
    ,LOC.CHN_ID                                                 AS CHN_ID
    ,LOC.CHNL_KEY                                               AS CHNL_KEY
    ,LOC.CHNL_ID                                                AS CHNL_ID
    ,INV_TXN.LOC_KEY                                            AS LOC_KEY
    ,INV_TXN.LOC_ID                                             AS LOC_ID
    ,NULL                                                       AS DMND_LOC_KEY
    ,NULL                                                       AS DMND_LOC_ID
    ,NULL                                                       AS FULFILL_LOC_KEY
    ,NULL                                                       AS FULFILL_LOC_ID
    ,DECODE(CFG.FROM_LOC,'LOC',INV_TXN.LOC_KEY
                        ,'LOC_2',INV_TXN.LOC_2_KEY)             AS INV_FROM_LOC_KEY
    ,DECODE(CFG.FROM_LOC,'LOC',INV_TXN.LOC_ID
                        ,'LOC_2',INV_TXN.LOC_2_ID)              AS INV_FROM_LOC_ID
    ,DECODE(CFG.TO_LOC,'LOC',INV_TXN.LOC_KEY
                      ,'LOC_2',INV_TXN.LOC_2_KEY)               AS INV_TO_LOC_KEY
    ,DECODE(CFG.TO_LOC,'LOC',INV_TXN.LOC_ID
                      ,'LOC_2',INV_TXN.LOC_2_ID)                AS INV_TO_LOC_ID
    ,LOC.LOC_TYP_CDE                                            AS LOC_TYP_CDE
    ,LOC.LOC_POSTAL_CDE                                         AS POSTAL_CDE
    ,LOC.LOC_STATE_PROVINCE_CDE                                 AS STATE_PROVINCE_CDE
    ,LOC.LOC_COUNTRY_CDE                                        AS COUNTRY_CDE
    ,COALESCE(ITM.DIV_KEY,'-1')                                 AS DIV_KEY
    ,ITM.DIV_ID                                                 AS DIV_ID
    ,INV_TXN.ITM_KEY                                            AS ITM_KEY
    ,INV_TXN.ITM_ID                                             AS ITM_ID
    ,NULL                                                       AS PACK_ITM_KEY
    ,NULL                                                       AS PACK_ITM_ID
    ,INV_TXN.SUP_KEY                                            AS SUP_KEY
    ,INV_TXN.SUP_ID                                             AS SUP_ID
    ,NULL                                                       AS EMP_KEY
    ,NULL                                                       AS EMP_ID
    ,NULL                                                       AS CUS_PROFILE_KEY
    ,NULL                                                       AS CUS_PROFILE_ID
    ,NULL                                                       AS PRM_KEY
    ,NULL                                                       AS PRM_ID
    ,INV_TXN.ITMLOC_STTS_CDE                                    AS ITMLOC_STTS_CDE
    ,NULL                                                       AS RTL_TYP_CDE
    ,NULL                                                       AS RTRN_FLG
    ,NULL                                                       AS RSN_ID
    ,NULL                                                       AS CO_ID
    ,NULL                                                       AS DO_ID
    ,NULL                                                       AS VERSION_ID
    ,INV_TXN.INV_TXN_DOC_ID                                     AS TXN_ID
    ,NULL                                                       AS PO_ID
    ,NULL                                                       AS ASN_ID
    ,NULL                                                       AS STTS_CDE
    ,NULL                                                       AS ORD_DOC_LN_STTS_CDE
    ,NULL                                                       AS INV_DOC_LN_STTS_CDE
    ,INV_TXN.LCL_CNCY_CDE                                       AS LCL_CNCY_CDE
    ,INV_TXN.F_INV_TXN_QTY                                      AS F_FACT_QTY
    ,INV_TXN.F_INV_TXN_CST_LCL                                  AS F_FACT_CST
    ,INV_TXN.F_INV_TXN_RTL_LCL                                  AS F_FACT_RTL
    ,NULL                                                       AS F_FACT_QTY1
    ,NULL                                                       AS F_FACT_QTY2
    ,NULL                                                       AS F_FACT_QTY3
    ,NULL                                                       AS F_FACT_QTY4
    ,NULL                                                       AS F_FACT_QTY5
    ,NULL                                                       AS F_FACT_QTY6
    ,NULL                                                       AS F_FACT_QTY7
    ,NULL                                                       AS F_FACT_QTY8
    ,NULL                                                       AS F_FACT_QTY9
    ,NULL                                                       AS F_FACT_QTY10
    ,NULL                                                       AS F_FACT_AMT1
    ,NULL                                                       AS F_FACT_AMT2
    ,NULL                                                       AS F_FACT_AMT3
    ,NULL                                                       AS F_FACT_AMT4
    ,NULL                                                       AS F_FACT_AMT5
    ,NULL                                                       AS F_FACT_AMT6
    ,NULL                                                       AS F_FACT_AMT7
    ,NULL                                                       AS F_FACT_AMT8
    ,NULL                                                       AS F_FACT_AMT9
    ,NULL                                                       AS F_FACT_AMT10
    ,NULL                                                       AS ATTR_DT_COL1
    ,NULL                                                       AS ATTR_DT_COL2
    ,NULL                                                       AS ATTR_DT_COL3
    ,NULL                                                       AS ATTR_DT_COL4
    ,NULL                                                       AS ATTR_DT_COL5
    ,NULL                                                       AS ATTR_DT_COL6
    ,NULL                                                       AS ATTR_DT_COL7
    ,NULL                                                       AS ATTR_DT_COL8
    ,NULL                                                       AS ATTR_DT_COL9
    ,NULL                                                       AS ATTR_DT_COL10
    ,NULL                                                       AS ATTR_NUM_COL1
    ,NULL                                                       AS ATTR_NUM_COL2
    ,NULL                                                       AS ATTR_NUM_COL3
    ,NULL                                                       AS ATTR_NUM_COL4
    ,NULL                                                       AS ATTR_NUM_COL5
    ,NULL                                                       AS ATTR_NUM_COL6
    ,NULL                                                       AS ATTR_NUM_COL7
    ,NULL                                                       AS ATTR_NUM_COL8
    ,NULL                                                       AS ATTR_NUM_COL9
    ,NULL                                                       AS ATTR_NUM_COL10
    ,NULL                                                       AS ATTR_VARCHAR_COL1
    ,NULL                                                       AS ATTR_VARCHAR_COL2
    ,NULL                                                       AS ATTR_VARCHAR_COL3
    ,NULL                                                       AS ATTR_VARCHAR_COL4
    ,NULL                                                       AS ATTR_VARCHAR_COL5
    ,NULL                                                       AS ATTR_VARCHAR_COL6
    ,NULL                                                       AS ATTR_VARCHAR_COL7
    ,NULL                                                       AS ATTR_VARCHAR_COL8
    ,NULL                                                       AS ATTR_VARCHAR_COL9
    ,NULL                                                       AS ATTR_VARCHAR_COL10
    ,NULL                                                       AS ATTR_VARCHAR_COL11
    ,NULL                                                       AS ATTR_VARCHAR_COL12
    ,NULL                                                       AS ATTR_VARCHAR_COL13
    ,INV_TXN.ROW_ID                                             AS ATTR_VARCHAR_COL14
    ,NULL                                                       AS ATTR_VARCHAR_COL15
    ,NULL                                                       AS ATTR_VARCHAR_COL16
    ,NULL                                                       AS ATTR_VARCHAR_COL17
    ,NULL                                                       AS ATTR_VARCHAR_COL18
    ,NULL                                                       AS ATTR_VARCHAR_COL19
    ,NULL                                                       AS ATTR_VARCHAR_COL20
    ,NULL                                                       AS ATTR_VARCHAR_COL21
    ,NULL                                                       AS ATTR_VARCHAR_COL22
    ,NULL                                                       AS ATTR_VARCHAR_COL23
    ,NULL                                                       AS ATTR_VARCHAR_COL24
    ,NULL                                                       AS ATTR_VARCHAR_COL25
    ,NULL                                                       AS ATTR_VARCHAR_COL26
    ,NULL                                                       AS ATTR_VARCHAR_COL28
    ,NULL                                                       AS ATTR_VARCHAR_COL29
    ,NULL                                                       AS ATTR_VARCHAR_COL27
    ,NULL                                                       AS ATTR_VARCHAR_COL30
FROM {{ ref('V_DWH_F_INV_TXN_ILD_B') }} INV_TXN
-- The following join is used to map a inventory transaction type to its corresponding fact_cde as well as the from and to locations
INNER JOIN {{ source("INV_TXN_CFG_LU","CFG_INV_TXN_LU") }} CFG
    ON INV_TXN.INV_TXN_TYP = CFG.INV_TXN_TYP
LEFT OUTER JOIN {{ ref("V_DWH_D_PRD_ITM_LU") }} ITM
    ON INV_TXN.ITM_ID = ITM.ITM_ID
LEFT OUTER JOIN {{ ref("V_DWH_D_ORG_LOC_LU") }} LOC
    ON INV_TXN.LOC_ID = LOC.LOC_ID
