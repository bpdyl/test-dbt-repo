{{ config(
    materialized='incremental',
    transient=false,
    alias='DWH_F_ECOMM_DO_HDR_B',
    schema='DW_DWH',
    unique_key=['DO_ID'],
    merge_exclude_columns=['DO_ID','RCD_INS_TS'],
    on_schema_change='append_new_columns',
    tags=['f_ecomm_do_hdr_ld'],
    post_hook=["{{ log_dml_audit(this,ref('TMP_F_ECOMM_DO_HDR_B'),'MERGE') }}","{{ log_script_success(this) }}"]
) }}

SELECT 
    *  
    ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ       AS RCD_INS_TS     
    ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ       AS RCD_UPD_TS                       
FROM {{ ref('TMP_F_ECOMM_DO_HDR_B') }}
