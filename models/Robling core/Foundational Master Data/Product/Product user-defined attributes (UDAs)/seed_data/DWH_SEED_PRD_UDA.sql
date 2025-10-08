/*
Summary
Purpose: Adds entry of scripts into DWH_C_BATCH_SCRIPTS table. Scripts will not run
if there is no entry in this table
Run Frequency: Upon install
*/

INSERT INTO DW_DWH.DWH_C_BATCH_SCRIPTS (
	SELECT 'd_prd_uda_itm_mtx_ld'
	,'This script loads Product UDA Item Matrix data to the target table DWH_D_PRD_UDA_ITM_MTX.'
	,'NA'
	,NVL((SELECT MAX(JOB_ID) FROM DW_DWH.DWH_C_BATCH_SCRIPTS),0) + 1
	);

INSERT INTO DW_DWH.DWH_C_BATCH_SCRIPTS (
	SELECT 'd_prd_uda_ld'
	,'This script loads Product UDA lookup data to the target table DWH_D_PRD_UDA_LU.'
	,'NA'
	,NVL((SELECT MAX(JOB_ID) FROM DW_DWH.DWH_C_BATCH_SCRIPTS),0) + 1
	);