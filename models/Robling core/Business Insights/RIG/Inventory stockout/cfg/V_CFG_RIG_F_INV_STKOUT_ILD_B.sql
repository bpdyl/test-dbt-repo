/*
Purpose of this view:
This view identifies stockout instances at the item-location-day level based on 
eligible item-location-day combinations that do not have on-hand inventory records in the datamart.
Use cases:
    - To track when and where stockouts are occurring.
    - To link stockout days with forecasted demand.
    - To calculate metrics like stockout counts, day counts, and weekend impacts.
*/
{% if execute %}
{% set rig_load_type_query = run_query("
    -- To find the load type for RIG from the DWH_C_PARAM table
    SELECT PARAM_VALUE FROM DW_DWH.DWH_C_PARAM WHERE PARAM_NAME = 'RIG_LOAD' ") %}
{% set rig_load_type = rig_load_type_query.columns[0].values()[0] %}
{% endif %}

{{ config(
    materialized='view',
    alias='V_CFG_RIG_F_INV_STKOUT_ILD_B',
    schema='DW_CFG',
    tags=['f_rig_inv_stkout_ild_b']
) }}
SELECT
    DAY.DAY_KEY                                               AS DAY_KEY
    ,ELGBL.ITM_ID                                             AS ITM_ID
    ,ELGBL.ITM_KEY                                            AS ITM_KEY
    ,ELGBL.LOC_ID                                             AS LOC_ID
    ,ELGBL.LOC_KEY                                            AS LOC_KEY
    ,1                                                        AS F_STKOUT_COUNT                -- Marks a stockout event occurrence
    ,1                                                        AS F_STKOUT_DAY_COUNT            -- Counts this day as a stockout day
    ,CASE WHEN DAYOFWEEK(FCST.DAY_KEY) IN (0,6)
        THEN 1
        ELSE 0
    END                                                       AS F_STKOUT_WEEKEND_DAY_COUNT    -- Weekend stockout indicator
    ,FCST.F_RIG_FCST_QTY                                      AS F_RIG_FCST_QTY     
    ,FCST.F_RIG_FCST_CST_LCL                                  AS F_RIG_FCST_CST_LCL         
    ,FCST.F_RIG_FCST_RTL_LCL                                  AS F_RIG_FCST_RTL_LCL         
    ,DM.ITMLOC_STTS_CDE                                       AS ITMLOC_STTS_CDE
    ,DM.LCL_CNCY_CDE                                          AS LCL_CNCY_CDE
FROM {{ ref('V_RIG_F_INV_ELGBL_IL_B') }} ELGBL
INNER JOIN DW_DWH.DWH_D_TIM_DAY_LU DAY 
    ON DAY.DAY_KEY BETWEEN ELGBL.ELGBL_EFF_START_DT AND ELGBL.ELGBL_EFF_END_DT
LEFT JOIN {{ ref('V_RIG_F_INV_FCST_ILD_B') }} FCST
    ON FCST.ITM_ID = ELGBL.ITM_ID
    AND FCST.LOC_ID = ELGBL.LOC_ID
    AND FCST.DAY_KEY = DAY.DAY_KEY
LEFT JOIN DM_MERCH.DM_F_MEAS_FACT_ILD_B DM                                         
    ON DM.ITM_ID = ELGBL.ITM_ID
    AND DM.LOC_ID = ELGBL.LOC_ID
    AND DAY.DAY_KEY BETWEEN DM.MEAS_DT AND DM.CLOSED_DT
    AND DM.FACT_CDE = 'OH'
WHERE (DM.ITM_ID IS NULL OR DM.F_FACT_QTY <= 0)                                                                 -- means no inventory record i.e, stockout
{# 
-- FOR DAILY PROCESSING: Set the value of RIG_LOAD param to 'DAILY' 
-- FOR HISTORICAL LOADS: Set the value of RIG_LOAD param to 'FULL'
#}
{% if rig_load_type|string == 'DAILY' %}
AND DAY.DAY_KEY =  TO_DATE('{{ robling_product.get_business_date() }}')
{% else %}
AND DAY.DAY_KEY <= TO_DATE('{{ robling_product.get_business_date() }}')                -- Forecast limited to current date
{% endif %}
