{{ config(
     materialized='incremental',
     transient=false,
     alias='DWH_F_SLS_TXN_LN_ITM_B',
     schema='DW_DWH',
     unique_key=['TXN_ID','TXN_LN_ID','VERSION_ID','POST_DT'],
     merge_exclude_columns=['TXN_ID','TXN_LN_ID','VERSION_ID','POST_DT','RCD_INS_TS'],
     on_schema_change='append_new_columns',
     tags = ['f_sls_txn_ln_itm_ld'],
     post_hook = ["{{ log_dml_audit(this,ref('TMP_F_SLS_TXN_LN_ITM_B'),'MERGE') }}"
               ,"{{ update_current_flg() }}"]
) }}

SELECT
     *
     ,CURRENT_TIMESTAMP()::TIMESTAMP_NTZ    AS RCD_INS_TS
     ,CURRENT_TIMESTAMP()::TIMESTAMP_NTZ    AS RCD_UPD_TS
FROM {{ ref('TMP_F_SLS_TXN_LN_ITM_B') }}

