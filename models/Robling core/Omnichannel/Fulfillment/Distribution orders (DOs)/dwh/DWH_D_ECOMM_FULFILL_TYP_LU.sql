{{ config(
    materialized='incremental',
    transient=false,
    alias='DWH_D_ECOMM_FULFILL_TYP_LU',
    schema='DW_DWH',
    unique_key=['FULFILL_TYP'],
    merge_exclude_columns=['FULFILL_TYP','RCD_INS_TS'],
    on_schema_change='append_new_columns',
    tags=['d_ecomm_fulfill_typ_ld'],
    post_hook=["{{ log_dml_audit(this,ref('TMP_D_ECOMM_FULFILL_TYP_LU'),'MERGE') }}","{{ log_script_success(this) }}"]
) }}

SELECT 
    *  
    ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ       AS RCD_INS_TS     
    ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ       AS RCD_UPD_TS                       
FROM {{ ref('TMP_D_ECOMM_FULFILL_TYP_LU') }}
