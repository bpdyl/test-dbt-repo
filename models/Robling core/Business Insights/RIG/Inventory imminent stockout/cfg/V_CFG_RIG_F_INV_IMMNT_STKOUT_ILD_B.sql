/*
Purpose of this view:
This view identifies imminent stockout instances at the item-location-day level.
Definition:
An imminent stockout occurs when projected on-hand and in-transit inventory is expected to run out within a defined threshold window (e.g., 14 days).
Use cases:
    - Track items/locations likely to go out of stock soon.
    - Quantify potential future stockouts based on forecasted demand.
    - The imminent stockout threshold (in days) is defined as 14.
    - Days of supply = (On-hand + In-transit) / Forecast Quantity.
*/
{% if execute %}
{% set rig_load_type_query = run_query("
    -- To find the load type for RIG from the DWH_C_PARAM table
    SELECT PARAM_VALUE FROM DW_DWH.DWH_C_PARAM WHERE PARAM_NAME = 'RIG_LOAD' ") %}
{% set rig_load_type = rig_load_type_query.columns[0].values()[0] %}
{% endif %}

{{ config(
    materialized='view',
    alias='V_CFG_RIG_F_INV_IMMNT_STKOUT_ILD_B',
    schema='DW_CFG',
    tags=['f_rig_inv_immnt_stkout_ild_b']
) }}

SELECT
    DAY.DAY_KEY                                                             AS DAY_KEY
    ,ELGBL.ITM_ID                                                           AS ITM_ID
    ,ELGBL.ITM_KEY                                                          AS ITM_KEY
    ,ELGBL.LOC_ID                                                           AS LOC_ID
    ,ELGBL.LOC_KEY                                                          AS LOC_KEY
    ,1                                                                      AS F_IMMNT_STKOUT_COUNT          -- Marks a imminent stockout event possibility
    ,14                                                                     AS F_IMMNT_STKOUT_THRESHOLD      -- Threshold (days) for defining imminent stockout
    ,(DM.F_FACT_QTY + DM.F_FACT_QTY1) / NULLIF(FCST.F_RIG_FCST_QTY, 0)      AS F_NUM_DAYS_OF_SUPPLY          -- Estimated number of days remaining before on-hand inventory reaches zero, based on sales rate
    ,FCST.F_RIG_FCST_QTY                                                    AS F_RIG_FCST_QTY                -- Forecasted demand quantity
    ,FCST.F_RIG_FCST_CST_LCL                                                AS F_RIG_FCST_CST_LCL            -- Forecasted cost value
    ,FCST.F_RIG_FCST_RTL_LCL                                                AS F_RIG_FCST_RTL_LCL            -- Forecasted retail value
    ,DM.ITMLOC_STTS_CDE                                                     AS ITMLOC_STTS_CDE
    ,DM.LCL_CNCY_CDE                                                        AS LCL_CNCY_CDE
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
WHERE (DM.F_FACT_QTY + DM.F_FACT_QTY1) > 0                                                                     -- Only consider active and in-transit inventory
    AND (DM.F_FACT_QTY + DM.F_FACT_QTY1) / NULLIF(FCST.F_RIG_FCST_QTY, 0) < F_IMMNT_STKOUT_THRESHOLD           -- condition for immiment stockout 
    {# 
    -- FOR DAILY PROCESSING: Set the value of RIG_LOAD param to 'DAILY' 
    -- FOR HISTORICAL LOADS: Set the value of RIG_LOAD param to 'FULL'
    #}
    {% if rig_load_type|string == 'DAILY' %}
    AND DAY.DAY_KEY =  TO_DATE('{{ robling_product.get_business_date() }}')
    {% else %}
    AND DAY.DAY_KEY <= TO_DATE('{{ robling_product.get_business_date() }}')
    {% endif %}