/*
Summary
Purpose: Adds entry of scripts into DWH_C_BATCH_SCRIPTS table. Scripts will not run
if there is no entry in this table
Run Frequency: Upon install
*/

DELETE FROM DW_DWH.DWH_C_BATCH_SCRIPTS
WHERE script_name in (
   'f_rig_inv_elgbl_il_b'
  ,'dm_f_rig_inv_elgbl_meas_fact_ild'
  ,'f_rig_inv_fcst_ild_b'
);

INSERT INTO DW_DWH.DWH_C_BATCH_SCRIPTS (
    SELECT 'f_rig_inv_elgbl_il_b'
    ,'This script loads eligible item location pairs and their eligibility date range to the table RIG_F_INV_ELGBL_IL_B'
    ,'NA'
    ,NVL((SELECT MAX(JOB_ID) FROM DW_DWH.DWH_C_BATCH_SCRIPTS),0) + 1
    );

INSERT INTO DW_DWH.DWH_C_BATCH_SCRIPTS (
    SELECT 'dm_f_rig_inv_elgbl_meas_fact_ild'
    ,'This script loads the eligibility table by itm/loc to datamart table DM_F_MEAS_FACT_ILD_B'
    ,'NA'
    ,NVL((SELECT MAX(JOB_ID) FROM DW_DWH.DWH_C_BATCH_SCRIPTS),0) + 1
    );


INSERT INTO DW_DWH.DWH_C_BATCH_SCRIPTS (
    SELECT 'f_rig_inv_fcst_ild_b'
    ,'This script loads forecasts for out of stock inventory to the table RIG_F_INV_FCST_ILD_B  at the item/location/day level'
    ,'NA'
    ,NVL((SELECT MAX(JOB_ID) FROM DW_DWH.DWH_C_BATCH_SCRIPTS),0) + 1
    );
