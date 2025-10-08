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
    alias='V_STG_F_ECOMM_DO_LN_ITM_B',
    schema='DW_STG_V',
    unique_key = ['DO_LN_ID'],
    tags=['f_ecomm_do_ln_itm_ld']
) }}
-- depends_on: {{ ref('V_RECON_F_ECOMM_DO_LN_ITM_B') }}
SELECT
     TRIM(SRC.DO_ID)                           AS DO_ID
    ,TRIM(SRC.DO_LN_ID)                        AS DO_LN_ID
    ,TRIM(SRC.DO_LN_NUM)                       AS DO_LN_NUM
    ,TRIM(SRC.CO_ID)                           AS CO_ID
    ,TRIM(SRC.CO_LN_ID)                        AS CO_LN_ID
    ,TRIM(SRC.ITM_ID)                          AS ITM_ID
    ,TRIM(SRC.DO_LN_STTS)                      AS DO_LN_STTS
    ,SRC.F_DO_ORIG_QTY                         AS F_DO_ORIG_QTY
    ,SRC.F_DO_CURR_QTY                         AS F_DO_CURR_QTY
    ,SRC.F_DO_CANCLD_QTY                       AS F_DO_CANCLD_QTY
    ,SRC.F_FULFILL_QTY                         AS F_FULFILL_QTY
    ,SRC.F_DO_TAX_AMT_LCL                      AS F_DO_TAX_AMT_LCL
    ,SRC.F_DO_LN_ORD_TOT_AMT_LCL               AS F_DO_LN_ORD_TOT_AMT_LCL
    ,TRIM(SRC.LCL_CNCY_CDE)                    AS LCL_CNCY_CDE
    ,SRC.SRC_UPD_TS                            AS SRC_UPD_TS
    ,TRIM(SRC.IS_DELETED)                      AS IS_DELETED
    , 1                                        AS FLEX_FIELD_1
    , '2'                                      AS FLEX_FIELD_2
FROM {{ source(src_name,'LND_F_ECOMM_DO_LN_ITM_B') }} SRC
LEFT OUTER JOIN {{ source(src_name,'LND_F_ECOMM_DO_HDR_B') }} DO_HDR ON SRC.DO_ID = DO_HDR.DO_ID 
WHERE TO_DATE(DO_HDR.DO_CREATED_TS) BETWEEN '2022-01-30' AND  '{{ curr_day }}'
