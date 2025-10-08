{{ config(
    materialized='table',
    alias='TMP_D_PRD_DIV_LU',
    schema='DW_TMP',
    tags=['d_prd_div_ld'],
    pre_hook=["{{ start_script('d_prd_div_ld','RUNNING','NONE') }}"],
    post_hook=["{{ log_dml_audit(this, ref('V_STG_D_PRD_DIV_LU'),'CREATE_TABLE_AS_SELECT') }}"]
) }}

SELECT
    SRC.*    
FROM {{ ref('V_STG_D_PRD_DIV_LU') }} SRC
