{{ config(
     materialized='incremental',
     transient=false,
     alias='DWH_F_ECOMM_CO_LN_ITM_B',
     schema='DW_DWH',
     unique_key=['CO_ID','CO_LN_ID','CO_ORD_DT'],
     merge_exclude_columns=['CO_ID','CO_LN_ID','CO_ORD_DT','RCD_INS_TS'],
     on_schema_change='append_new_columns',
     tags = ['f_ecomm_co_ln_itm_ld'],
     post_hook = ["{{ log_dml_audit(this,ref('TMP_F_ECOMM_CO_LN_ITM_B'),'MERGE') }}"]
) }}

SELECT
     *
     ,CURRENT_TIMESTAMP()::TIMESTAMP_NTZ    AS RCD_INS_TS
     ,CURRENT_TIMESTAMP()::TIMESTAMP_NTZ    AS RCD_UPD_TS
FROM {{ ref('TMP_F_ECOMM_CO_LN_ITM_B') }}

