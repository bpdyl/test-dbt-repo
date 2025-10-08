/*
Summary
Purpose: Adds entry of scripts into DWH_C_BATCH_SCRIPTS table. Scripts will not run
if there is no entry in this table
Run Frequency: Upon install
*/

INSERT INTO DW_DWH.DWH_C_BATCH_SCRIPTS (
	SELECT 'd_prd_grp_ld'
	,'This script loads Groups'
	,'NA'
    ,NVL((SELECT MAX(JOB_ID) FROM DW_DWH.DWH_C_BATCH_SCRIPTS),0) + 1
    );

INSERT INTO DW_DWH.DWH_C_BATCH_SCRIPTS (
	SELECT 'd_prd_dpt_ld'
	,'This script loads Department '
	,'NA'
    ,NVL((SELECT MAX(JOB_ID) FROM DW_DWH.DWH_C_BATCH_SCRIPTS),0) + 1
    );

INSERT INTO DW_DWH.DWH_C_BATCH_SCRIPTS (
	SELECT 'd_prd_sbc_ld'
	,'This script loads Subclass '
	,'NA'
    ,NVL((SELECT MAX(JOB_ID) FROM DW_DWH.DWH_C_BATCH_SCRIPTS),0) + 1
    );

INSERT INTO DW_DWH.DWH_C_BATCH_SCRIPTS (
	SELECT 'd_prd_size_ld'
	,'This script loads product size'
	,'NA'
    ,NVL((SELECT MAX(JOB_ID) FROM DW_DWH.DWH_C_BATCH_SCRIPTS),0) + 1
    );


INSERT INTO DW_DWH.DWH_C_BATCH_SCRIPTS (
	SELECT 'd_prd_color_ld'
	,'Script to load Color Table'
	,'NA'
	,NVL((SELECT MAX(JOB_ID) FROM DW_DWH.DWH_C_BATCH_SCRIPTS),0) + 1
    );


INSERT INTO DW_DWH.DWH_C_BATCH_SCRIPTS (
	SELECT 'd_prd_sty_ld'
	,'This script loads Style table'
	,'NA'
	,NVL((SELECT MAX(JOB_ID) FROM DW_DWH.DWH_C_BATCH_SCRIPTS),0) + 1
    );

INSERT INTO DW_DWH.DWH_C_BATCH_SCRIPTS (
	SELECT 'd_prd_div_ld'
	,'Script to load Division Table in LEDM'
	,'NA'
	,NVL((SELECT MAX(JOB_ID) FROM DW_DWH.DWH_C_BATCH_SCRIPTS),0) + 1
    );

INSERT INTO DW_DWH.DWH_C_BATCH_SCRIPTS (
	SELECT 'd_prd_itm_ld'
	,'This script loads item table '
	,'NA'
	,NVL((SELECT MAX(JOB_ID) FROM DW_DWH.DWH_C_BATCH_SCRIPTS),0) + 1
    );

INSERT INTO DW_DWH.DWH_C_BATCH_SCRIPTS (
	SELECT 'd_prd_cls_ld'
	,'This script loads class'
	,'NA'
    ,NVL((SELECT MAX(JOB_ID) FROM DW_DWH.DWH_C_BATCH_SCRIPTS),0) + 1
    );


INSERT INTO DW_DWH.DWH_C_BATCH_SCRIPTS (
	SELECT 'd_prd_sty_img_ld'
	,'This script loads style image information'
	,'NA'
	,NVL((SELECT MAX(JOB_ID) FROM DW_DWH.DWH_C_BATCH_SCRIPTS),0) + 1
	);

INSERT INTO DW_DWH.DWH_C_BATCH_SCRIPTS (
	SELECT 'dm_f_prd_keys_update_ild'
	,'This script updates keys and columns related to product hierarchy in datamart after batch end'
	,'NA'
    ,NVL((SELECT MAX(JOB_ID) FROM DW_DWH.DWH_C_BATCH_SCRIPTS),0) + 1
    );

INSERT INTO DW_DWH.DWH_C_BATCH_SCRIPTS (
    SELECT 'd_sup_ld'
    ,'This script loads Supplier'
    ,'NA'
    ,NVL((SELECT MAX(JOB_ID) FROM DW_DWH.DWH_C_BATCH_SCRIPTS),0) + 1
    );

