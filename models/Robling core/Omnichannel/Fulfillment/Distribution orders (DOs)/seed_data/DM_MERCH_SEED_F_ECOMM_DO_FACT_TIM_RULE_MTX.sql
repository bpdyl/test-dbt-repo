/*
Summary
Purpose: Populates time rule codes which is used by looker. Values in looker get pulled from
DM_F_MEAS_FACT_ILD_B based on the filters applied in MEAS_TIM_RULE_CDE
Run Frequency: Upon install, Upon addition of new area in looker
*/

/*Seeds data for Distribution Order (CO_FULFILLED) which is used to bring daily data out for distribution order
CO_FULFILLED (FACT_CDE) is specific to distribution order
DAILY (MEAS_TIM_RULE_CDE)  be used across different fact code if applicable
CO_FULFILLED (MEAS_CDE) is unique*/
INSERT INTO DM_MERCH.DM_D_MEAS_FACT_TIM_RULE_MTX (
	 FACT_CDE
	,MEAS_TIM_RULE_CDE
	,MEAS_CDE
	,MEAS_COEFF
	)
VALUES (
	    'CO_FULFILLED'
	   ,'DAILY'
	   ,'CO_FULFILLED'
	   ,1
	   );

/*Seeds data for Distribution Order (CO_FULFILLED) which is used to bring daily data out for distribution order in last year
CO_FULFILLED (FACT_CDE) is specific to distribution order
DAILY_LY (MEAS_TIM_RULE_CDE)  be used across different fact code if applicable
CO_ORDERED_LY (MEAS_CDE) is unique*/
INSERT INTO DM_MERCH.DM_D_MEAS_FACT_TIM_RULE_MTX (
	 FACT_CDE
	,MEAS_TIM_RULE_CDE
	,MEAS_CDE
	,MEAS_COEFF
	)
VALUES (
	    'CO_FULFILLED'
	   ,'DAILY_LY'
	   ,'CO_FULFILLED_LY'
	   ,1
	   );