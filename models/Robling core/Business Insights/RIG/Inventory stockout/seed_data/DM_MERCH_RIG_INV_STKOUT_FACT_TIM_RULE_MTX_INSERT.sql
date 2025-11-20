/*Seeds data for calculating Stockout instances till date
RIG_INV_STKOUT (FACT_CDE) is specific to Stockouts for Inventory at Item-Location-Day level.
DAILY (MEAS_TIM_RULE_CDE) could be used across different fact code if applicable
RIG_INV_STKOUT (MEAS_CDE) is unique*/

DELETE FROM DM_MERCH.DM_D_MEAS_FACT_TIM_RULE_MTX 
WHERE MEAS_CDE = 'RIG_INV_STKOUT';

INSERT INTO DM_MERCH.DM_D_MEAS_FACT_TIM_RULE_MTX (
     FACT_CDE
    ,MEAS_TIM_RULE_CDE
    ,MEAS_CDE
    ,MEAS_COEFF
)
VALUES (
     'RIG_INV_STKOUT'
    ,'DAILY'
    ,'RIG_INV_STKOUT'
    ,1
); 