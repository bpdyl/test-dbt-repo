/*
Purpose of this view:
    This view provides a recap of inventory and sales activity at the item-location and style-color-location levels.
Use cases:
    - FIRST_CC_LOC_OH_DT / LAST_CC_LOC_OH_DT: Earliest and latest on-hand dates for a style-color at a location.
    - FIRST_CC_LOC_SLS_DT / LAST_CC_LOC_SLS_DT: Earliest and latest sales dates for a style-color at a location.
    - FIRST_CC_LOC_MD_DT: Earliest date of markdown inventory for a style-color at a location.
    - FIRST_ITM_LOC_OH_DT / LAST_ITM_LOC_OH_DT: Earliest and latest on-hand dates for an item at a location.
    - FIRST_ITM_LOC_SLS_DT / LAST_ITM_LOC_SLS_DT: Earliest and latest sales dates for an item at a location.
    - FIRST_ITM_MD_DT: Earliest date of markdown inventory for an item at a location.
*/

{{ config(
    materialized='view',
    alias='V_CFG_RIG_F_INV_RECAP_IL_B',
    schema='DW_CFG',
    tags=['f_rig_inv_recap_il_b']
) }}

SELECT
     DM.ITM_ID                                                                                                      AS ITM_ID
    ,{{ get_coalesced_surrogate_key('ITM.ITM_KEY','DM.ITM_ID') }}                                                   AS ITM_KEY
    ,DM.LOC_ID                                                                                                      AS LOC_ID
    ,{{ get_coalesced_surrogate_key('LOC.LOC_KEY','DM.LOC_ID') }}                                                   AS LOC_KEY
    ,MIN(CASE WHEN DM.FACT_CDE = 'OH' AND DM.F_FACT_QTY > 0 THEN DM.MEAS_DT END)
        OVER (PARTITION BY ITM.STY_ID, ITM.COLOR_ID, DM.LOC_ID)                                                     AS FIRST_CC_LOC_OH_DT
    ,MAX(CASE WHEN DM.FACT_CDE = 'OH' AND DM.F_FACT_QTY > 0 THEN DM.MEAS_DT END)
        OVER (PARTITION BY ITM.STY_ID, ITM.COLOR_ID, DM.LOC_ID)                                                     AS LAST_CC_LOC_OH_DT
    ,MIN(CASE WHEN DM.FACT_CDE = 'SLS' AND DM.F_FACT_QTY > 0 THEN DM.MEAS_DT END)
        OVER (PARTITION BY ITM.STY_ID, ITM.COLOR_ID, DM.LOC_ID)                                                     AS FIRST_CC_LOC_SLS_DT
    ,MAX(CASE WHEN DM.FACT_CDE = 'SLS' AND DM.F_FACT_QTY > 0 THEN DM.MEAS_DT END)
        OVER (PARTITION BY ITM.STY_ID, ITM.COLOR_ID, DM.LOC_ID)                                                     AS LAST_CC_LOC_SLS_DT
    ,MIN(CASE WHEN DM.FACT_CDE = 'OH' AND DM.F_FACT_QTY > 0 AND DM.ITMLOC_STTS_CDE <> 'R' THEN DM.MEAS_DT END)
        OVER (PARTITION BY ITM.STY_ID, ITM.COLOR_ID, DM.LOC_ID)                                                     AS FIRST_CC_LOC_MD_DT
    ,MIN(CASE WHEN DM.FACT_CDE = 'OH' AND DM.F_FACT_QTY > 0 THEN DM.MEAS_DT END)
        OVER (PARTITION BY DM.ITM_ID, DM.LOC_ID)                                                                    AS FIRST_ITM_LOC_OH_DT
    ,MAX(CASE WHEN DM.FACT_CDE = 'OH' AND DM.F_FACT_QTY > 0 THEN DM.MEAS_DT END)
        OVER (PARTITION BY DM.ITM_ID, DM.LOC_ID)                                                                    AS LAST_ITM_LOC_OH_DT
    ,MIN(CASE WHEN DM.FACT_CDE = 'SLS' AND DM.F_FACT_QTY > 0 THEN DM.MEAS_DT END)
        OVER (PARTITION BY DM.ITM_ID, DM.LOC_ID)                                                                    AS FIRST_ITM_LOC_SLS_DT
    ,MAX(CASE WHEN DM.FACT_CDE = 'SLS' AND DM.F_FACT_QTY > 0 THEN DM.MEAS_DT END)
        OVER (PARTITION BY DM.ITM_ID, DM.LOC_ID)                                                                    AS LAST_ITM_LOC_SLS_DT
    ,MIN(CASE WHEN DM.FACT_CDE = 'OH' AND DM.F_FACT_QTY > 0 AND DM.ITMLOC_STTS_CDE <> 'R' THEN DM.MEAS_DT END)
        OVER (PARTITION BY DM.ITM_ID, DM.LOC_ID)                                                                    AS FIRST_ITM_MD_DT
FROM {{ source('CFG_RIG_RECAP_SRC','DM_F_MEAS_FACT_ILD_B') }} DM
LEFT JOIN {{ ref('V_DWH_D_PRD_ITM_LU') }} ITM
    ON DM.ITM_ID = ITM.ITM_ID
LEFT JOIN {{ ref('V_DWH_D_ORG_LOC_LU') }} LOC
    ON DM.LOC_ID = LOC.LOC_ID
WHERE DM.FACT_CDE IN ('OH', 'SLS')
QUALIFY ROW_NUMBER() OVER (PARTITION BY DM.ITM_ID, DM.LOC_ID ORDER BY DM.MEAS_DT) = 1

