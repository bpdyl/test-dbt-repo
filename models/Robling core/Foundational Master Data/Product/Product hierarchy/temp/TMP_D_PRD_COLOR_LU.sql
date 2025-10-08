{{ config(
    materialized='table',
    alias='TMP_D_PRD_COLOR_LU',
    schema='DW_TMP',
    tags=['d_prd_color_ld'],
    pre_hook=["{{ start_script('d_prd_color_ld','RUNNING','NONE') }}"],
    post_hook=["{{ log_dml_audit(this, ref('V_STG_D_PRD_COLOR_LU'),'CREATE_TABLE_AS_SELECT') }}"]
) }}

SELECT
    SRC.*
FROM {{ ref('V_STG_D_PRD_COLOR_LU') }} SRC
