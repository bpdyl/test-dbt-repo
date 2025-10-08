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
    alias='V_RECON_F_ECOMM_CO_LN_ITM_B',
    schema='DW_STG_V',
    tags=['f_ecomm_co_ln_itm_ld']
) }}
/* Recon views should be simple with only the necessary columns that are used in the reconciliation table load */
SELECT
    TRIM(CO_LN.LOC_ID)                             AS DMND_LOC_ID
    ,CO_LN.ORD_CREATED_TS                           AS CO_ORD_TS
    ,CO_LN.F_ORD_QTY                                AS F_CO_ORD_QTY
    ,CO_LN.F_ORD_AMT_LCL                            AS F_CO_ORD_RTL_LCL
    ,CO_LN.LCL_CNCY_CDE                             AS LCL_CNCY_CDE
FROM {{ source(src_name,'LND_F_ECOMM_CO_LN_ITM_B') }} CO_LN
WHERE TO_DATE(CO_LN.ORD_CREATED_TS) BETWEEN '2022-01-30' AND  '{{ curr_day }}'
