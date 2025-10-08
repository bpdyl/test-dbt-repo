{{ config(
    materialized='incremental',
    transient=false,
    alias='DWH_D_PRD_ITM_LU',
    schema='DW_DWH',
    unique_key=['ITM_ID'],
    merge_exclude_columns=['ITM_ID', 'ITM_KEY', 'RCD_INS_TS'],
    rollup_key=['STY_KEY'],
    rollup_fields=['STY_ID', 'STY_NUM', 'STY_DESC', 'SBC_KEY', 'SBC_ID', 'SBC_NUM', 'SBC_DESC', 'CLS_KEY', 'CLS_ID', 'CLS_NUM', 'CLS_DESC', 'DPT_KEY', 'DPT_ID', 'DPT_NUM', 'DPT_DESC', 'GRP_KEY', 'GRP_ID', 'GRP_NUM', 'GRP_DESC', 'DIV_KEY', 'DIV_ID', 'DIV_NUM', 'DIV_DESC', 'SUP_KEY', 'SUP_ID', 'SUP_NUM', 'SUP_DESC'],
    on_schema_change='append_new_columns',
    tags=['d_prd_itm_ld'],
    post_hook = ["{{ log_dml_audit(this,ref('TMP_D_PRD_ITM_LU'),'MERGE') }}"
                ,"{{ close_dimension_using_temp(this, ref('TMP_D_PRD_ITM_LU')) }}"
                ,"{{ update_closed_dimension_using_rollup(this, ref('V_DWH_D_PRD_STY_LU')) }}"
                ,"{{ log_script_success(this) }}"
    ]
) }}
SELECT
  *
  ,{{ dbt_utils.generate_surrogate_key(config.get('unique_key')) }}  AS ITM_KEY
  ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                  AS RCD_INS_TS
  ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                  AS RCD_UPD_TS
  ,0                                                                 AS RCD_CLOSE_FLG
  ,'9999-12-31'                                                      AS RCD_CLOSE_DT
FROM {{ ref('TMP_D_PRD_ITM_LU') }}
