{{ config(
     materialized='delete_merge',
     transient=false,
     alias='DWH_F_INV_RTV_SUP_ILD_B',
     schema='DW_DWH',
     delete_matching_keys=['POST_DT'],
     on_schema_change='append_new_columns',
     tags = ['f_inv_rtv_sup_ild_ld'],
     post_hook = ["{{ log_dml_audit(this,ref('TMP_F_INV_RTV_SUP_ILD_B'),'DELETE') }}"
                    ,"{{ log_dml_audit(this,ref('TMP_F_INV_RTV_SUP_ILD_B'),'MERGE') }}"]
) }}

SELECT
     *
     ,CURRENT_TIMESTAMP()::TIMESTAMP_NTZ    AS RCD_INS_TS
     ,CURRENT_TIMESTAMP()::TIMESTAMP_NTZ    AS RCD_UPD_TS
FROM {{ ref('TMP_F_INV_RTV_SUP_ILD_B') }} 
