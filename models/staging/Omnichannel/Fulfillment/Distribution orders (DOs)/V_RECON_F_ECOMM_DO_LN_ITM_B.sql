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
    alias='V_RECON_F_ECOMM_DO_LN_ITM_B',
    schema='DW_STG_V',
    tags=['f_ecomm_do_ln_itm_ld']
) }}
/* Recon views should be simple with only the necessary columns that are used in the reconciliation table load */
SELECT
    TRIM(SRC.DO_ID)                               AS DO_ID
    ,TRIM(SRC.CO_LN_ID)                           AS CO_LN_ID
    ,SRC.F_FULFILL_QTY                            AS F_FULFILL_QTY
    ,TRIM(SRC.LCL_CNCY_CDE)                       AS LCL_CNCY_CDE
    ,TRIM(SRC.IS_DELETED)                         AS IS_DELETED
FROM {{ source(src_name,'LND_F_ECOMM_DO_LN_ITM_B') }} SRC
LEFT OUTER JOIN {{ source(src_name,'LND_F_ECOMM_DO_HDR_B') }} DO_HDR ON SRC.DO_ID = DO_HDR.DO_ID 
WHERE TO_DATE(DO_HDR.DO_CREATED_TS) BETWEEN '2022-01-30' AND  '{{ curr_day }}'

