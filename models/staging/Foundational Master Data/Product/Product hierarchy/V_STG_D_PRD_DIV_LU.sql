{# Note: following macro is called to switch between source 
and source_chg and it is applicable for robling product only.
It ensures that first load in daily batch is done using _LND schema 
and second load in daily batch is done using _LND_CHG schema#}
{% set curr_day = robling_product.get_business_date() | string | trim %}
{% set src_name = select_stg_source(
    base_source_name = 'PRD_HIER_STG_SRC',
    curr_day = curr_day,
    switch_date = '2023-12-28'
) %}

{{ config(
    materialized='view',
    alias='V_STG_D_PRD_DIV_LU',
    schema='DW_STG_V',
    tags=['d_prd_div_ld']
) }}
SELECT
     TRIM(DIV.DIV_ID)           AS DIV_ID
    ,TRIM(DIV.DIV_ID)           AS DIV_NUM
    ,TRIM(DIV.DIV_DESC)         AS DIV_DESC
FROM {{ source(src_name, 'LND_D_PRD_DIV_LU') }} DIV
