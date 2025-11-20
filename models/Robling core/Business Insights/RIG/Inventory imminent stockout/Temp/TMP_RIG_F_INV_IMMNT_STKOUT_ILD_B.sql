{{ config(
    materialized='table',
    alias='TMP_RIG_F_INV_IMMNT_STKOUT_ILD_B',
    schema='DW_TMP',
    tags=['f_rig_inv_immnt_stkout_ild_b'],
    pre_hook=["{{ start_script('f_rig_inv_immnt_stkout_ild_b','RUNNING','NONE') }}"],
    post_hook=["{{ log_dml_audit(this, ref('V_CFG_RIG_F_INV_IMMNT_STKOUT_ILD_B'),'CREATE_TABLE_AS_SELECT') }}"]
) }}

SELECT
    SRC.*    
FROM {{ ref('V_CFG_RIG_F_INV_IMMNT_STKOUT_ILD_B') }} SRC
