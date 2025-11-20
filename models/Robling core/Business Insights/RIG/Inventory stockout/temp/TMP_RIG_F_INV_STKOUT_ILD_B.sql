{{ config(
    materialized='table',
    alias='TMP_RIG_F_INV_STKOUT_ILD_B',
    schema='DW_TMP',
    tags=['f_rig_inv_stkout_ild_b'],
    pre_hook=["{{ start_script('f_rig_inv_stkout_ild_b','RUNNING','NONE') }}"],
    post_hook=["{{ log_dml_audit(this, ref('V_CFG_RIG_F_INV_STKOUT_ILD_B'),'CREATE_TABLE_AS_SELECT') }}"]
) }}

SELECT
    SRC.* EXCLUDE(ITMLOC_STTS_CDE, LCL_CNCY_CDE)     -- Excluding columns that are redefined in the select statement to avoid duplicate column issues. 
    ,COALESCE(SRC.ITMLOC_STTS_CDE, INV.ITMLOC_STTS_CDE)     AS ITMLOC_STTS_CDE
    ,COALESCE(SRC.LCL_CNCY_CDE, INV.LCL_CNCY_CDE)           AS LCL_CNCY_CDE
FROM {{ ref('V_CFG_RIG_F_INV_STKOUT_ILD_B') }} SRC
LEFT JOIN {{ref('V_DWH_F_INV_ILD_B')}} INV
    ON SRC.ITM_ID  = INV.ITM_ID
    AND SRC.LOC_ID  = INV.LOC_ID
    AND SRC.DAY_KEY BETWEEN INV.EFF_START_DT AND INV.EFF_END_DT