/*
Summary
Purpose: Adds entry of scripts into DWH_C_BATCH_SCRIPTS table. Scripts will not run
if there is no entry in this table
Run Frequency: Upon install
*/

INSERT INTO DW_DWH.DWH_C_BATCH_SCRIPTS (
SELECT 'f_ecomm_do_hdr_ld'
,'This script loads Customer Fulfillment header DWH table.'
,'NA'
,NVL((SELECT MAX(JOB_ID) FROM DW_DWH.DWH_C_BATCH_SCRIPTS),0) + 1
);

INSERT INTO DW_DWH.DWH_C_BATCH_SCRIPTS (
SELECT 'f_ecomm_do_ln_itm_ld'
,'This script loads Customer Fulfillment line item DWH table.'
,'NA'
,NVL((SELECT MAX(JOB_ID) FROM DW_DWH.DWH_C_BATCH_SCRIPTS),0) + 1
);

INSERT INTO DW_DWH.DWH_C_BATCH_SCRIPTS (
SELECT 'd_ecomm_fulfill_typ_ld'
,'This script loads Customer Fulfillment type DWH table.'
,'NA'
,NVL((SELECT MAX(JOB_ID) FROM DW_DWH.DWH_C_BATCH_SCRIPTS),0) + 1
);

INSERT INTO DW_DWH.DWH_C_BATCH_SCRIPTS (
SELECT 'dm_f_co_fulmnt_meas_fact_ild'
,'This script loads Customer Fulfillment in table DM_F_MEAS_FACT_ILD_B using SOURCE TABLE DWH_F_ECOMM_DO_LN_ITM_B.'
,'NA'
,NVL((SELECT MAX(JOB_ID) FROM DW_DWH.DWH_C_BATCH_SCRIPTS),0) + 1
);