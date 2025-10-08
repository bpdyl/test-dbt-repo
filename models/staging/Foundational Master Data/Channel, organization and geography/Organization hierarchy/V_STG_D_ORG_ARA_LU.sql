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
    alias='V_STG_D_ORG_ARA_LU',
    schema='DW_STG_V',
    tags=['d_org_ara_ld']
) }}

SELECT
     TRIM(ARA.ARA_ID)                                             AS ARA_ID
    ,TRIM(ARA.ARA_ID)                                             AS ARA_NUM
    ,TRIM(ARA.ARA_DESC)                                           AS ARA_DESC
    ,TRIM(ARA.CHN_ID)                                             AS CHN_ID
    ,TRIM(ARA.ARA_MGR_FIRST_NAME || ' ' || ARA.ARA_MGR_LAST_NAME) AS ARA_MGR_NAME
FROM {{ source(src_name,'LND_D_ORG_ARA_LU') }} ARA