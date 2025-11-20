/*
Purpose of this view:
    This view calculates item-loc-day level forecasts using a 28-day average of sales starting from week start date.
    - F_SLS_AVG_QTY: Average sales quantity based on sales over the previous 28 days.
    - F_SLS_AVG_CST_LCL: Average sales cost based on sales over the previous 28 days.
    - F_SLS_AVG_RTL_LCL: Average sales retail based on sales over the previous 28 days.
    Only eligible items and locations (from V_RIG_F_INV_ELGBL_IL_B) are included, and the forecast respects eligibility start and end dates.
*/

{{ config(
    materialized='view',
    alias='V_CFG_RIG_F_INV_FCST_ILD_B',
    schema='DW_CFG',
    tags=['f_rig_inv_fcst_ild_b']
) }}
/* Temporary CTE to calculate range of last 28 days for each day starting from year start date to current date */
    WITH L28_DAYS AS
    (
        SELECT
        DISTINCT
            DATEADD (DAY, -28, TIM.WK_START_DT)         AS LAG_DAY_KEY                          -- Start of 28-day historical window
            ,TIM.WK_START_DT-1                          AS LAST_WK_END_DT                       -- End of 28-day windo (day before current week start)
            ,TIM.DAY_KEY                                AS DAY_KEY                              -- day for which the average is calculated
        FROM DW_DWH_V.V_DWH_D_TIM_DAY_LU TIM 
        WHERE TIM.DAY_KEY  >= (SELECT TY_START_DT FROM DW_DWH_V.V_DWH_D_CURR_TIM_LU)
    )
    /* Query to calculate average sales for the last 4 weeks for eligible products on day level */
    SELECT
        L28D.DAY_KEY                      AS DAY_KEY
        ,DM.ITM_ID                        AS ITM_ID
        ,DM.ITM_KEY                       AS ITM_KEY
        ,DM.LOC_ID                        AS LOC_ID
        ,DM.LOC_KEY                       AS LOC_KEY
        ,SUM(DM.F_FACT_QTY) / 28          AS F_RIG_FCST_QTY                                     -- Average daily quantity over past 28 days
        ,SUM(DM.F_FACT_CST) / 28          AS F_RIG_FCST_CST_LCL                                 -- Average daily cost over past 28 days
        ,SUM(DM.F_FACT_RTL) / 28          AS F_RIG_FCST_RTL_LCL                                 -- Average daily retail over the past 28 days
        ,DM.LCL_CNCY_CDE                  AS LCL_CNCY_CDE
    FROM {{ source('CFG_RIG_SRC','DM_F_MEAS_FACT_ILD_B') }} DM
    INNER JOIN L28_DAYS L28D
        ON DM.MEAS_DT >= L28D.LAG_DAY_KEY AND
        DM.MEAS_DT <= L28D.LAST_WK_END_DT
    INNER JOIN {{ ref('V_RIG_F_INV_ELGBL_IL_B') }} ELGBL                                        -- Only include eligible items/locations
        ON DM.LOC_ID = ELGBL.LOC_ID
        AND DM.ITM_ID = ELGBL.ITM_ID
        AND L28D.DAY_KEY BETWEEN ELGBL.ELGBL_EFF_START_DT AND ELGBL.ELGBL_EFF_END_DT
    WHERE DM.FACT_CDE = 'SLS'
    GROUP BY
        L28D.DAY_KEY
        ,DM.ITM_ID
        ,DM.ITM_KEY
        ,DM.LOC_ID
        ,DM.LOC_KEY
        ,DM.LCL_CNCY_CDE