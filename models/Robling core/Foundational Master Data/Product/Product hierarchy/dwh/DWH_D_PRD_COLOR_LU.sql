{{ config(
    materialized='incremental',
    transient=false,
    alias='DWH_D_PRD_COLOR_LU',
    schema='DW_DWH',
    unique_key=['COLOR_ID'],
    merge_exclude_columns=['COLOR_ID','COLOR_KEY','RCD_INS_TS'],
    on_schema_change='append_new_columns',
    tags=['d_prd_color_ld'],
    post_hook = ["{{ log_dml_audit(this,ref('TMP_D_PRD_COLOR_LU'),'MERGE') }}"
                ,"{{ close_dimension_using_temp(this, ref('TMP_D_PRD_COLOR_LU')) }}"
                ,"{{ log_script_success(this) }}"]
) }}

SELECT
    SRC.*
    ,{{ dbt_utils.generate_surrogate_key(config.get('unique_key')) }} AS COLOR_KEY
    ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                 AS RCD_INS_TS
    ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                 AS RCD_UPD_TS
    ,0                                                                AS RCD_CLOSE_FLG
    ,'9999-12-31'                                                     AS RCD_CLOSE_DT
FROM {{ ref('TMP_D_PRD_COLOR_LU') }} SRC
