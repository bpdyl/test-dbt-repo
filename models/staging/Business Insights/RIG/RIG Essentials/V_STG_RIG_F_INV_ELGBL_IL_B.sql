{# Note: following macro is called to switch between source
and source_chg and it is applicable for robling product only.
It ensures that first load in daily batch is done using _LND schema
and second load in daily batch is done using _LND_CHG schema#}
{% set curr_day = robling_product.get_business_date() | string | trim %}
{% set src_name = select_stg_source(
    base_source_name = 'ELIGIBILITY_STG_SRC',
    curr_day = curr_day,
    switch_date = '2023-12-28'
) %}

/*
Default Logic Used:
    - Eligibility start date (ELGBL_EFF_START_DT) is the earliest inventory start date for the item at the location.
    - Eligibility end date (ELGBL_EFF_END_DT) is set to 28 days after the last sales transaction date (if any) for the item at the location.
    - Only considers locations that are not eCommerce channels and not warehouses.
*/

{{ config(
    materialized='view',
    alias='V_STG_RIG_F_INV_ELGBL_IL_B',
    schema='DW_STG_V',
    unique_key = ['LOC_ID','ITM_ID'],
    tags=['f_rig_inv_elgbl_il_b']
) }}

SELECT
     SRC.ITM_ID                                                         AS ITM_ID
    ,SRC.LOC_ID                                                         AS LOC_ID
    ,SRC.FIRST_ITM_LOC_OH_DT                                            AS ELGBL_EFF_START_DT   -- Start eligibility based on inventory
    ,DATEADD(DAY, 28, SRC.LAST_ITM_LOC_SLS_DT)                          AS ELGBL_EFF_END_DT     -- Eligibility end date set to 28 days after the last sales transaction date
FROM {{ ref('V_CFG_RIG_F_INV_RECAP_IL_B') }} SRC
LEFT JOIN {{ ref('V_DWH_D_ORG_LOC_LU') }} LOC
    ON SRC.LOC_ID = LOC.LOC_ID
-- filtering out eCommerce and warehouse locations from the eligibility view
AND COALESCE(LOC.CHNL_DESC, '0') <> 'eCommerce'
AND COALESCE(LOC.LOC_TYP_CDE, '0') <> 'W'