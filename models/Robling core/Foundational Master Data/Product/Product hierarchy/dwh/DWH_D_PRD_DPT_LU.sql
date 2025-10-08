{{ config(
    materialized='incremental',
    transient=false,
    alias='DWH_D_PRD_DPT_LU',
    schema='DW_DWH',
    unique_key=['DPT_ID'],
    merge_exclude_columns=['DPT_ID','DPT_KEY','RCD_INS_TS'],
    rollup_key=['GRP_KEY'],
    rollup_fields=['GRP_ID', 'GRP_NUM', 'GRP_DESC', 'DIV_KEY', 'DIV_ID', 'DIV_NUM', 'DIV_DESC'],
    on_schema_change='append_new_columns',
    tags=['d_prd_dpt_ld'],
    post_hook = ["{{ log_dml_audit(this,ref('TMP_D_PRD_DPT_LU'),'MERGE') }}"
                ,"{{ close_dimension_using_temp(this, ref('TMP_D_PRD_DPT_LU')) }}"
                ,"{{ update_closed_dimension_using_rollup(this, ref('V_DWH_D_PRD_GRP_LU')) }}"
                ,"{{ log_script_success(this) }}"
    ]
) }}

SELECT 
    *  
    ,{{ dbt_utils.generate_surrogate_key(config.get('unique_key')) }} AS DPT_KEY
    ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                 AS RCD_INS_TS
    ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                 AS RCD_UPD_TS
    ,0                                                                AS RCD_CLOSE_FLG
    ,'9999-12-31'                                                     AS RCD_CLOSE_DT
FROM {{ ref('TMP_D_PRD_DPT_LU') }}
