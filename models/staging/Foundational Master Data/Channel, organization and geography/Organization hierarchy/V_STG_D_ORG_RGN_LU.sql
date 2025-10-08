{# Note: following macro is called to switch between source 
and source_chg and it is applicable for robling product only.
It ensures that first load in daily batch is done using _LND schema 
and second load in daily batch is done using _LND_CHG schema#}
{% set curr_day = robling_product.get_business_date() | string | trim %}
{% set src_name = select_stg_source(
    base_source_name = 'ORG_HIER_STG_SRC',
    curr_day = curr_day,
    switch_date = '2023-12-28'
) %}

{{ config(
    materialized='view',
    alias='V_STG_D_ORG_RGN_LU',
    schema='DW_STG_V',
    tags=['d_org_rgn_ld']
) }}

SELECT
     TRIM(RGN.RGN_ID)                                             AS RGN_ID
    ,TRIM(RGN.RGN_ID)                                             AS RGN_NUM
    ,TRIM(RGN.RGN_DESC)                                           AS RGN_DESC
    ,TRIM(RGN.ARA_ID)                                             AS ARA_ID
    ,TRIM(RGN.RGN_MGR_FIRST_NAME || ' ' || RGN.RGN_MGR_LAST_NAME) AS RGN_MGR_NAME
FROM {{ source(src_name,'LND_D_ORG_RGN_LU') }} RGN 