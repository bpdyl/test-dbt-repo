{{ config(
    materialized='incremental',
    transient=false,
    alias='DWH_D_PRD_GRP_LU',
    schema='DW_DWH',
    unique_key=['GRP_ID'],
    merge_exclude_columns=['GRP_ID', 'GRP_KEY', 'RCD_INS_TS'],
    rollup_key=['DIV_KEY'],
    rollup_fields=['DIV_ID', 'DIV_NUM', 'DIV_DESC'],
    on_schema_change='append_new_columns',
    tags=['d_prd_grp_ld'],
    post_hook = ["{{ log_dml_audit(this,ref('TMP_D_PRD_GRP_LU'),'MERGE') }}"
                ,"{{ close_dimension_using_temp(this, ref('TMP_D_PRD_GRP_LU')) }}"
                ,"{{ update_closed_dimension_using_rollup(this, ref('V_DWH_D_PRD_DIV_LU')) }}"
                ,"{{ log_script_success(this) }}"
    ]
) }}

SELECT 
    *  
    ,{{ dbt_utils.generate_surrogate_key(config.get('unique_key')) }}  AS GRP_KEY
    ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                  AS RCD_INS_TS
    ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                  AS RCD_UPD_TS
    ,0                                                                 AS RCD_CLOSE_FLG
    ,'9999-12-31'                                                      AS RCD_CLOSE_DT
FROM {{ ref('TMP_D_PRD_GRP_LU') }} 
