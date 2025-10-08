{{ config(
    materialized='incremental',
    transient=false,
    alias='RIG_F_INV_ELGBL_IL_B',
    schema='DW_RIG',
    unique_key=['ITM_ID','LOC_ID'],
    merge_exclude_columns=['ITM_ID','LOC_ID','ITM_KEY','LOC_KEY','ELGBL_EFF_START_DT','RCD_INS_TS'],
    on_schema_change='append_new_columns',
    tags=['f_rig_inv_elgbl_il_b'],
    post_hook=["{{ log_dml_audit(this, ref('TMP_RIG_F_INV_ELGBL_IL_B'), 'INSERT') }}","{{ log_script_success(this) }}"]
) }}

SELECT 
    SRC.*  
    ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ       AS RCD_INS_TS     
    ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ       AS RCD_UPD_TS                       
FROM 
    {{ ref('TMP_RIG_F_INV_ELGBL_IL_B') }} SRC