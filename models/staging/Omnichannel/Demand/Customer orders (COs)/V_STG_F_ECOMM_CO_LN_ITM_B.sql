{# Note: following macro is called to switch between source 
and source_chg and it is applicable for robling product only.
It ensures that first load in daily batch is done using _LND schema 
and second load in daily batch is done using _LND_CHG schema#}
{% set curr_day = robling_product.get_business_date() | string | trim %}
{% set src_name = select_stg_source(
    base_source_name = 'CUSTOMER_ORDERS_SRC',
    curr_day = curr_day,
    switch_date = '2023-12-28'
) %}

{{ config(
    materialized='view',
    alias='V_STG_F_ECOMM_CO_LN_ITM_B',
    schema='DW_STG_V',
    unique_key = ['CO_LN_ID', 'CO_ID'],
    tags=['f_ecomm_co_ln_itm_ld']
) }}
-- depends_on: {{ ref('V_RECON_F_ECOMM_CO_LN_ITM_B') }}
SELECT
     TRIM(CO_LN.CO_LN_OMS_ID)                       AS CO_LN_ID
    ,TRIM(CO_LN.CO_ID)                              AS CO_ID
    ,TRIM(CO_LN.CUS_ORD_LN_ID)                      AS CO_LN_NUM
    ,TRIM(CO_LN.LOC_ID)                             AS DMND_LOC_ID
    ,TRIM(CO_LN.ITM_ID)                             AS ITM_ID
    ,TRIM(CO_LN.CO_LN_ITM_STTS)                     AS CO_LN_ITM_STTS
    ,NULL                                           AS BOPIS_FLG
    ,CO_LN.RTRN_FLG                                 AS RTRN_FLG
    ,CO_LN.ORD_CREATED_TS                           AS CO_ORD_TS
    ,CO_LN.F_ORD_QTY                                AS F_CO_ORD_QTY
    ,CO_LN.DO_CREATED_DT::TIMESTAMP_NTZ             AS DO_CREATED_TS
    ,CO_LN.F_ALLOCATED_QTY                          AS F_CO_ALLOCATED_QTY
    ,CO_LN.BACK_ORD_DT::TIMESTAMP_NTZ               AS BACK_ORD_TS
    ,CO_LN.F_BACK_ORD_QTY                           AS F_CO_BACK_ORD_QTY
    ,CO_LN.INVOICE_DT::TIMESTAMP_NTZ                AS INVOICE_TS
    ,NULL                                           AS F_CO_FULFILL_QTY
    ,CO_LN.CAN_DT::TIMESTAMP_NTZ                    AS CANCLD_TS
    ,CO_LN.CANCLD_RSN                               AS CANCLD_RSN
    ,CO_LN.F_CAN_QTY                                AS F_CO_CANCLD_QTY
    ,NULL                                           AS RTRN_TS
    ,CO_LN.RTRN_RSN                                 AS RTRN_RSN
    ,NULL                                           AS F_CO_RTRN_QTY
    ,NULL                                           AS DLVRY_TYP
    ,NULL                                           AS DLVRY_CITY
    ,CO_LN.DLVRY_STATE                              AS DLVRY_STATE
    ,CO_LN.DLVRY_ZIP_CDE                            AS DLVRY_POSTAL_CDE
    ,CO_LN.DLVRY_COUNTRY_CDE                        AS DLVRY_COUNTRY_CDE
    ,NULL                                           AS F_CO_ORIG_UNIT_RTL_LCL
    ,CO_LN.F_UNIT_RTL_LCL                           AS F_CO_PAID_UNIT_RTL_LCL
    ,CO_LN.F_ORD_AMT_LCL                            AS F_CO_ORD_RTL_LCL
    ,CO_LN.F_DSC_AMT_LCL                            AS F_CO_DSC_AMT_LCL
    ,CO_LN.F_TAX_AMT_LCL                            AS F_CO_TAX_AMT_LCL
    ,CO_LN.F_OTHER_CHRGS_LCL                        AS F_CO_LN_ADDTNL_CHRGS_LCL
    ,CO_LN.F_ORD_LN_TOT_LCL                         AS F_CO_LN_ORD_TOT_AMT_LCL
    ,CO_LN.LCL_CNCY_CDE                             AS LCL_CNCY_CDE
    ,NULL                                           AS SRC_UPD_TS
    ,CO_LN.IS_DELETED                               AS IS_DELETED
    , 1                                             AS FLEX_FIELD_1
    , '2'                                           AS FLEX_FIELD_2
FROM {{ source(src_name,'LND_F_ECOMM_CO_LN_ITM_B') }} CO_LN
WHERE TO_DATE(CO_LN.ORD_CREATED_TS) BETWEEN '2022-01-30' AND  '{{ curr_day }}'
