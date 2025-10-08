{{ config(
    materialized='incremental',
    transient=false,
    alias='DWH_F_SLS_TXN_ATTR_LU',
    schema='DW_DWH',
    unique_key=['TXN_ID'],
    merge_exclude_columns=['TXN_ID','RCD_INS_TS'],
    on_schema_change='append_new_columns',
    tags=['f_sls_txn_attr_ld'],
    post_hook=["{{ log_dml_audit(this,ref('TMP_F_SLS_TXN_ATTR_LU'),'MERGE') }}","{{ log_script_success(this) }}"]
) }}

SELECT 
    *  
    ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ       AS RCD_INS_TS     
    ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ       AS RCD_UPD_TS                       
FROM {{ ref('TMP_F_SLS_TXN_ATTR_LU') }}
