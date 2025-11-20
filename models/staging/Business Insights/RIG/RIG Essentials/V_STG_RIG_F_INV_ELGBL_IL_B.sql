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
    - Eligibility end date (ELGBL_EFF_END_DT) is set to 28 days after the last sales transaction date (if any) for the item at the location, but if the item has markdowns (MD), the eligibility ends on the first markdown date, ensuring that MD items are excluded from eligibility.
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
     SRC.ITM_ID                                                          AS ITM_ID
    ,SRC.LOC_ID                                                          AS LOC_ID
    ,SRC.FIRST_ITM_LOC_OH_DT                                             AS ELGBL_EFF_START_DT   -- Start eligibility based on inventory
    -- Eligibility end date set to 28 days after the last sales transaction date
    -- But if the item has markdowns (MD), the eligibility should end on the day before the first markdown date.
    -- The FIRST_ITM_MD_DT - 1 ensures MD items are excluded from eligibility and the first markdown date is also not considered as an eligible date.
    ,LEAST(DATEADD(DAY, 28, SRC.LAST_ITM_LOC_SLS_DT), SRC.FIRST_ITM_MD_DT-1) AS ELGBL_EFF_END_DT     
FROM {{ ref('V_RIG_F_INV_RECAP_IL_B') }} SRC
LEFT JOIN {{ ref('V_DWH_D_ORG_LOC_LU') }} LOC
    ON SRC.LOC_ID = LOC.LOC_ID
-- filtering out eCommerce and warehouse locations from the eligibility view
WHERE 
    COALESCE(LOC.CHNL_DESC, '0') <> 'eCommerce'
    AND COALESCE(LOC.LOC_TYP_CDE, '0') <> 'W'
    
