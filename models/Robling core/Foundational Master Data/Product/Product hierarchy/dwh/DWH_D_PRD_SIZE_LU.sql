{{ config(
    materialized='incremental',
    transient=false,
    alias='DWH_D_PRD_SIZE_LU',
    schema='DW_DWH',
    unique_key=['SIZE_ID'],
    merge_exclude_columns=['SIZE_ID','SIZE_KEY','RCD_INS_TS'],
    on_schema_change='append_new_columns',
    tags=['d_prd_size_ld'],
    post_hook = ["{{ log_dml_audit(this,ref('TMP_D_PRD_SIZE_LU'),'MERGE') }}"
                ,"{{ close_dimension_using_temp(this, ref('TMP_D_PRD_SIZE_LU'), 'SIZE_KEY') }}"
                ,"{{ log_script_success(this) }}"]
) }}

SELECT
    SRC.*
    ,{{ dbt_utils.generate_surrogate_key(config.get('unique_key')) }} AS SIZE_KEY
    ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                 AS RCD_INS_TS
    ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                 AS RCD_UPD_TS
    ,0                                                                AS RCD_CLOSE_FLG
    ,'9999-12-31'                                                     AS RCD_CLOSE_DT
FROM {{ ref('TMP_D_PRD_SIZE_LU') }} SRC
