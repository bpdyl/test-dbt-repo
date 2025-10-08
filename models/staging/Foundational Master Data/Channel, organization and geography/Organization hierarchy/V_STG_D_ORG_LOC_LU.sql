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
    alias='V_STG_D_ORG_LOC_LU',
    schema='DW_STG_V',
    tags=['d_org_loc_ld']
) }}

SELECT
     TRIM(LOC.LOC_ID)                           AS LOC_ID
    ,TRIM(LOC.LOC_ID)                           AS LOC_NUM
    ,TRIM(LOC.LOC_DESC)                         AS LOC_DESC
    ,TRIM(LOC.DST_ID)                           AS DST_ID
    ,TRIM(LOC.CHNL_ID)                          AS CHNL_ID
    ,NULL                                       AS LOC_MGR_NAME
    ,TRIM(LOC.CNCY_CDE)                         AS LOC_CNCY_CDE
    ,TRIM(LOC.LOC_TYP_CDE)                      AS LOC_TYP_CDE
    ,TRIM(LOC.LOC_TYP_CDE_DESC)                 AS LOC_TYP_CDE_DESC
    ,TRIM(LOC.LOC_POSTAL_CDE)                   AS LOC_POSTAL_CDE
    ,TRIM(LOC.LOC_STATE_CDE)                    AS LOC_STATE_PROVINCE_CDE
    ,TRIM(LOC.LOC_COUNTRY_CDE)                  AS LOC_COUNTRY_CDE
    ,TRIM(LOC.LOC_FMT_CDE)                      AS LOC_FMT_CDE
    ,LOC.LOC_DEFAULT_WH_NUM                     AS LOC_DEFAULT_WH_NUM
    ,LOC.LOC_START_DT                           AS LOC_OPEN_DT
    ,LOC.LOC_END_DT                             AS LOC_CLOSE_DT
    ,1                                          AS FLEX_FIELD_1
    ,'2'                                        AS FLEX_FIELD_2
FROM {{ source(src_name,'LND_D_ORG_LOC_LU') }} LOC 