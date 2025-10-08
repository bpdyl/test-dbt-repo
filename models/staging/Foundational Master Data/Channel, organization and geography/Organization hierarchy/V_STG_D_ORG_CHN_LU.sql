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
    alias='V_STG_D_ORG_CHN_LU',
    schema='DW_STG_V',
    tags=['d_org_chn_ld']
) }}

SELECT
     TRIM(CHN.CHN_ID)                                              AS CHN_ID
    ,TRIM(CHN.CHN_ID)                                              AS CHN_NUM
    ,TRIM(CHN.CHN_DESC)                                            AS CHN_DESC
    ,TRIM(CHN.CHN_MGR_FIRST_NAME || ' ' || CHN.CHN_MGR_LAST_NAME)  AS CHN_MGR_NAME
FROM {{ source(src_name,'LND_D_ORG_CHN_LU') }} CHN
