/*
Summary
Purpose: Adds entry of scripts into DWH_C_BATCH_SCRIPTS table. Scripts will not run
if there is no entry in this table
Run Frequency: Upon install
*/

DELETE FROM DW_DWH.DWH_C_BATCH_SCRIPTS
WHERE script_name in (
  'f_rig_inv_immnt_stkout_ild_b'
  ,'dm_f_rig_inv_immnt_stkout_meas_fact_ild'
);

INSERT INTO DW_DWH.DWH_C_BATCH_SCRIPTS (
    SELECT 'f_rig_inv_immnt_stkout_ild_b'
    ,'This script loads data for inventory potentially going out of stock to the table RIG_F_INV_IMMNT_STKOUT_ILD_B at the item/location/day level'
    ,'NA'
    ,NVL((SELECT MAX(JOB_ID) FROM DW_DWH.DWH_C_BATCH_SCRIPTS),0) + 1
    );

INSERT INTO DW_DWH.DWH_C_BATCH_SCRIPTS (
    SELECT 'dm_f_rig_inv_immnt_stkout_meas_fact_ild'
    ,'This script loads the imminent stockout table by itm/loc/day to datamart table DM_F_MEAS_FACT_ILD_B'
    ,'NA'
    ,NVL((SELECT MAX(JOB_ID) FROM DW_DWH.DWH_C_BATCH_SCRIPTS),0) + 1
    );