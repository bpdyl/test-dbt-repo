
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
    alias='V_STG_D_PRD_ITM_LU',
    schema='DW_STG_V',
    tags=['d_prd_itm_ld']
) }}
SELECT
     TRIM(ITM.ITM_ID)           AS ITM_ID
    ,TRIM(ITM.ITM_ID)           AS ITM_NUM
    ,TRIM(ITM.ITM_DESC)         AS ITM_DESC
    ,TRIM(ITM.STY_ID)           AS STY_ID
    ,TRIM(ITM.COLOR_ID)         AS COLOR_ID
    ,TRIM(ITM.SIZE_ID)          AS SIZE_ID
    ,TRIM(ITM.UPC_ID)           AS UPC_ID
    ,TRIM(ITM.SUP_PART_ID)      AS SUP_PART_NUM
    ,TRIM(ITM.MER_IND)          AS MER_IND
    ,TRIM(ITM.PACK_IND)         AS PACK_IND
    ,TRIM(ITM.SIMPLE_PACK_IND)  AS SIMPLE_PACK_IND
    ,TRIM(ITM.STND_UOM_CDE)     AS STND_UOM_CDE
    ,ITM.FIRST_RCVD_DT          AS FIRST_RCVD_DT
    ,ITM.LAST_RCVD_DT           AS LAST_RCVD_DT
    ,ITM.FIRST_SOLD_DT          AS FIRST_SOLD_DT
    ,ITM.LAST_SOLD_DT           AS LAST_SOLD_DT
FROM {{ source(src_name, 'LND_D_PRD_ITM_LU') }} ITM
