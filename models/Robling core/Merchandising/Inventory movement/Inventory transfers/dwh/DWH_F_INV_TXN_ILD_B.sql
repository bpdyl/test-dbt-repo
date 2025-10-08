{{ config(
    materialized='incremental',
    transient=false,
    alias='DWH_F_INV_TXN_ILD_B',
    schema='DW_DWH',
    unique_key = ['ROW_ID'],
    merge_exclude_columns=['ROW_ID','RCD_INS_TS'],
    on_schema_change='append_new_columns',
    tags=['f_inv_txn_ild_ld'],
    post_hook=["{{ log_dml_audit(this,ref('TMP_F_INV_TXN_ILD_B'),'MERGE') }}","{{ log_script_success(this) }}"]
) }}

SELECT 
    TMP.* 
    ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ       AS RCD_INS_TS     
    ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ       AS RCD_UPD_TS                       
FROM {{ref('TMP_F_INV_TXN_ILD_B')}} TMP
