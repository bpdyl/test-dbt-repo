{{ config(
    materialized='incremental',
    transient=false,
    alias='DWH_D_PRD_STY_IMG_LU',
    schema='DW_DWH',
    unique_key=['STY_ID'],
    merge_exclude_columns=['STY_ID','STY_KEY','RCD_INS_TS'],
    on_schema_change='append_new_columns',
    tags=['d_prd_sty_img_ld'],
    post_hook = ["{{ log_dml_audit(this,ref('TMP_D_PRD_STY_IMG_LU'),'MERGE') }}"
                ,"{{ log_script_success(this) }}"]
) }}

SELECT
  *
  ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ                    AS RCD_INS_TS
  ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ                    AS RCD_UPD_TS
  ,0                                                   AS RCD_CLOSE_FLG
  ,'9999-12-31'                                        AS RCD_CLOSE_DT
FROM {{ ref('TMP_D_PRD_STY_IMG_LU') }}
