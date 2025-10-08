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
    alias='V_STG_F_ECOMM_CO_HDR_B',
    schema='DW_STG_V',
    unique_key = ['CO_ID'],
    tags=['f_ecomm_co_hdr_ld']
) }}

SELECT
     TRIM(CO_HDR.CUS_ORD_OMS_ID)                    AS CO_ID
    ,TRIM(CO_HDR.CUS_ORD_ID)                        AS CO_NUM
    ,TRIM(CO_HDR.CO_HDR_STTS)                       AS CO_HDR_STTS
    ,TRIM(CO_HDR.DMND_LOC_ID)                       AS DMND_LOC_ID
    ,CO_HDR.ORD_CREATED_TS                          AS CO_ORD_TS
    ,CO_HDR.F_ORD_AMT_LCL                           AS F_CO_ORD_RTL_LCL
    ,CO_HDR.F_DSC_AMT_LCL                           AS F_CO_DSC_AMT_LCL
    ,CO_HDR.F_TAX_AMT_LCL                           AS F_CO_TAX_AMT_LCL
    ,CO_HDR.F_SHIPPING_CHRGS_LCL                    AS F_CO_SHIPPING_RTL_LCL
    ,NULL                                           AS F_CO_SHIPPING_CST_LCL
    ,CO_HDR.F_ADDTNL_CHRGS_LCL                      AS F_CO_HDR_ADDTNL_CHRGS_LCL
    ,NULL                                           AS F_CO_LN_ADDTNL_CHRGS_LCL
    ,CO_HDR.F_ORD_TOT_LCL                           AS F_CO_ORD_TOT_AMT_LCL
    ,TRIM(CO_HDR.LCL_CNCY_CDE)                      AS LCL_CNCY_CDE
    ,CO_HDR.SRC_UPD_TS                              AS SRC_UPD_TS
    ,TO_VARCHAR(CO_HDR.IS_DELETED)                  AS IS_DELETED
FROM {{source (src_name,'LND_F_ECOMM_CO_HDR_B')}} CO_HDR
WHERE TO_DATE(CO_HDR.ORD_CREATED_TS) BETWEEN '2022-01-30' AND  '{{ curr_day }}'
