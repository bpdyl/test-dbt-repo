/*Seeds data for calculating Eligible Item-Location pairs till date
RIG_INV_ELGBL (FACT_CDE) is specific to Eligibility for Inventory at Item-Location level.
LTCD_AGG (MEAS_TIM_RULE_CDE) could be used across different fact code if applicable
RIG_INV_ELGBL (MEAS_CDE) is unique*/


DELETE FROM DM_MERCH.DM_D_MEAS_FACT_TIM_RULE_MTX 
WHERE MEAS_CDE = 'RIG_INV_ELGBL';

INSERT INTO DM_MERCH.DM_D_MEAS_FACT_TIM_RULE_MTX (
     FACT_CDE
    ,MEAS_TIM_RULE_CDE
    ,MEAS_CDE
    ,MEAS_COEFF
)
VALUES (
     'RIG_INV_ELGBL'
    ,'LTCD_AGG'
    ,'RIG_INV_ELGBL'
    ,1
); 
