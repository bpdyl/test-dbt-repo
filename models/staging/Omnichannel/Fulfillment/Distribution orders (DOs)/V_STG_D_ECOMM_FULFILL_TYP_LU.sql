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
    alias='V_STG_D_ECOMM_FULFILL_TYP_LU',
    schema='DW_STG_V',
    unique_key = ['FULFILL_TYP'],
    tags=['d_ecomm_fulfill_typ_ld']
) }}

SELECT
     TRIM(FULFILL_TYP.FULFILL_TYP)                AS FULFILL_TYP
    ,TRIM(FULFILL_TYP.FULFILL_TYP_DESC)           AS FULFILL_TYP_DESC
    ,TRIM(FULFILL_TYP.FULFILL_METHOD)             AS FULFILL_METHOD
    ,TRIM(FULFILL_TYP.FULFILL_METHOD_DESC)        AS FULFILL_METHOD_DESC
FROM {{source (src_name,'LND_D_ECOMM_FULFILL_TYP_LU')}} FULFILL_TYP
