{# Note: following macro is called to switch between source 
and source_chg and it is applicable for robling product only.
It ensures that first load in daily batch is done using _LND schema 
and second load in daily batch is done using _LND_CHG schema#}
{% set curr_day = robling_product.get_business_date() | string | trim %}
{% set src_name = select_stg_source(
    base_source_name = 'SALES_AND_RETURNS_SRC',
    curr_day = curr_day,
    switch_date = '2023-12-28'
) %}

{{ config(
    materialized='view',
    alias='V_STG_F_SLS_TXN_LN_ITM_B',
    schema='DW_STG_V',
    unique_key =  ['TXN_ID','TXN_LN_ID','POST_DT','VERSION_ID'],
    tags=['f_sls_txn_ln_itm_ld']
) }}
-- depends_on: {{ ref('V_RECON_F_SLS_TXN_LN_ITM_B') }}
SELECT
         SLS.TXN_ID                                                     AS TXN_ID
        ,SLS.TXN_LN_ID                                                  AS TXN_LN_ID
        ,TO_DATE(SELECT PARAM_VALUE FROM DW_DWH.DWH_C_PARAM 
                WHERE PARAM_NAME = 'CURR_DAY')                          AS POST_DT
        ,SLS.REVISION_NO                                                AS VERSION_ID
        ,SLS.TXN_CREATE_TS                                              AS TXN_TS
        ,SLS.LOC_ID                                                     AS LOC_ID
        ,SLS.DMND_LOC_ID                                                AS DMND_LOC_ID
        ,SLS.ITM_ID                                                     AS ITM_ID
        ,SLS.RTRN_FLG                                                   AS RTRN_FLG
        ,SLS.REGISTER_ID                                                AS REGISTER_ID
        ,SLS.POS_TXN_ID                                                 AS POS_TXN_NUM
        ,SLS.CO_ID                                                      AS CO_ID
        ,SLS.CO_LN_ID                                                   AS CO_LN_ID
        ,RSN.RSN_DESC                                                   AS RTRN_RSN
        ,SLS.ITMLOC_STTS_CDE                                            AS ITMLOC_STTS_CDE
        ,SLS.RTL_TYP_CDE                                                AS RTL_TYP_CDE
        ,SLS.SELLING_UOM                                                AS SELLING_UOM
        ,SUM(DECODE(SLS.RTRN_FLG, 1, -1, 1) * SLS.F_SLS_QTY)            AS F_SLS_QTY
        ,SUM(DECODE(SLS.RTRN_FLG, 1, -1, 1) * SLS.F_SLS_RTL_LCL)        AS F_SLS_RTL_LCL
        ,SLS.F_UNIT_RTL_LCL                                             AS F_UNIT_RTL_LCL
        ,SUM(SLS.F_TOT_DSC_AMT_LCL)                                     AS F_TOT_DSC_AMT_LCL
        ,SUM(SLS.F_EMP_DSC_AMT_LCL)                                     AS F_EMP_DSC_AMT_LCL
        ,SUM(DECODE(SLS.RTRN_FLG, 1, -1, 1) * SLS.F_SLS_VAT_AMT_LCL)    AS F_SLS_TAX_AMT_LCL
        ,SLS.LCL_CNCY_CDE                                               AS LCL_CNCY_CDE
        , 1                                                             AS FLEX_FIELD_1
        , '2'                                                           AS FLEX_FIELD_2
    FROM {{ source(src_name,'LND_F_SLS_TXN_LN_ITM_B') }} SLS
    LEFT JOIN {{ source(src_name,'LND_D_RSN_LU') }} RSN
    ON SLS.RSN_ID = RSN.RSN_ID 
        AND RSN.RSN_CLS_ID = 'SRR'
    WHERE SLS.TXN_DT_KEY BETWEEN '2022-01-30' AND  '{{ curr_day }}'
    GROUP BY ALL
