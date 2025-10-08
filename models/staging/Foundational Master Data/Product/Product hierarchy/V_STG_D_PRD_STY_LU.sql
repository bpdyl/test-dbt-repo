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
    alias='V_STG_D_PRD_STY_LU',
    schema='DW_STG_V',
    tags=['d_prd_sty_ld']
) }}

SELECT
     TRIM(STY.STY_ID)                                                   AS STY_ID
    ,TRIM(STY.STY_ID)                                                   AS STY_NUM
    ,TRIM(STY.STY_DESC)                                                 AS STY_DESC
    ,TRIM(STY.DPT_ID)||'-'||TRIM(STY.CLS_ID)||'-'||TRIM(STY.SBC_ID)     AS SBC_ID
    ,TRIM(STY.SUP_ID)                                                   AS SUP_ID
    ,STY.FIRST_RCVD_DT                                                  AS FIRST_RCVD_DT
    ,STY.LAST_RCVD_DT                                                   AS LAST_RCVD_DT
FROM {{ source(src_name, 'LND_D_PRD_STY_LU') }} STY
