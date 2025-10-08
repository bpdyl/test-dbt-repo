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
    alias='V_STG_D_PRD_COLOR_LU',
    schema='DW_STG_V',
    tags=['d_prd_color_ld']
) }}
SELECT
     TRIM(COLOR.COLOR_ID)       AS COLOR_ID
    ,TRIM(COLOR.COLOR_ID)       AS COLOR_NUM
    ,TRIM(COLOR.COLOR_DESC)     AS COLOR_DESC
FROM {{ source(src_name, 'LND_D_PRD_COLOR_LU') }} COLOR
