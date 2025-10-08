{# Note: following macro is called to switch between source 
and source_chg and it is applicable for robling product only.
It ensures that first load in daily batch is done using _LND schema 
and second load in daily batch is done using _LND_CHG schema#}
{% set curr_day = robling_product.get_business_date() | string | trim %}
{% set src_name = select_stg_source(
    base_source_name = 'CUSTOMER_FULFILLMENT_SRC',
    curr_day = curr_day,
    switch_date = '2023-12-28'
) %}

{{ config(
    materialized='view',
    alias='V_STG_F_ECOMM_DO_HDR_B',
    schema='DW_STG_V',
    unique_key = ['DO_ID'],
    tags=['f_ecomm_do_hdr_ld']
) }}

SELECT
     TRIM(SRC.DO_ID)                             AS DO_ID
    ,TRIM(SRC.DO_NUM)                            AS DO_NUM
    ,TRIM(SRC.CO_ID)                             AS CO_ID
    ,TRIM(SRC.FULMNT_LOC_ID)                     AS FULMNT_LOC_ID
    ,TRIM(SRC.INV_SRC_LOC_ID)                    AS INV_SRC_LOC_ID
    ,TRIM(SRC.DO_HDR_STTS)                       AS DO_HDR_STTS
    ,SRC.DO_CREATED_TS                           AS DO_CREATED_TS
    ,SRC.DO_INVOICE_TS                           AS DO_INVOICE_TS        
    ,TRIM(SRC.DO_INVOICE_ID)                     AS DO_INVOICE_ID        
    ,TRIM(SRC.FULFILL_TYP)                       AS FULFILL_TYP          
    ,SRC.DO_BOPIS_FLG                            AS DO_BOPIS_FLG         
    ,SRC.F_DO_DUTY_AMT_LCL                       AS F_DO_DUTY_AMT_LCL
    ,SRC.F_DO_SHIPPING_CST_LCL                   AS F_DO_SHIPPING_CST_LCL
    ,TRIM(SRC.LCL_CNCY_CDE)                      AS LCL_CNCY_CDE
    ,SRC.SRC_UPD_TS                              AS SRC_UPD_TS
    ,TRIM(SRC.IS_DELETED)                        AS IS_DELETED
FROM {{source (src_name,'LND_F_ECOMM_DO_HDR_B')}} SRC
WHERE TO_DATE(SRC.DO_CREATED_TS) BETWEEN '2022-01-30' AND  '{{ curr_day }}'
