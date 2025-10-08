{{ config(
    materialized='incremental',
    transient=false,
    alias='DWH_D_ORG_RGN_LU',
    schema='DW_DWH',
    unique_key=['RGN_ID'],
    merge_exclude_columns=['RGN_ID','RGN_KEY','RCD_INS_TS'],
    rollup_key=['ARA_KEY'],
    rollup_fields=['ARA_ID', 'ARA_NUM', 'ARA_DESC', 'CHN_KEY', 'CHN_ID', 'CHN_NUM', 'CHN_DESC'],
    on_schema_change='append_new_columns',
    tags=['d_org_rgn_ld'],
    post_hook = ["{{ log_dml_audit(this,ref('TMP_D_ORG_RGN_LU'),'MERGE') }}"
                ,"{{ close_dimension_using_temp(this, ref('TMP_D_ORG_RGN_LU'), 'RGN_KEY') }}"
                ,"{{ update_closed_dimension_using_rollup(this, ref('DWH_D_ORG_ARA_LU')) }}"
                ,"{{ log_script_success(this) }}"
    ]
) }}

SELECT 
    *  
    ,{{ dbt_utils.generate_surrogate_key(config.get('unique_key')) }} AS RGN_KEY
    ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                 AS RCD_INS_TS
    ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                 AS RCD_UPD_TS
    ,0                                                                AS RCD_CLOSE_FLG
    ,TO_DATE('9999-12-31')                                            AS RCD_CLOSE_DT
FROM {{ ref('TMP_D_ORG_RGN_LU') }} 