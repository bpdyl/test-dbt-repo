{{ config(
    materialized='table',
    alias='TMP_D_PRD_UDA_LU',
    schema='DW_TMP',
    on_schema_change = 'append_new_columns',
    tags=['d_prd_uda_ld'],
    pre_hook=["{{ start_script('d_prd_uda_ld','RUNNING','NONE') }}"],
    post_hook=["{{ log_dml_audit(this, ref('V_STG_D_PRD_UDA_LU'),'CREATE_TABLE_AS_SELECT') }}"]
) }}

SELECT
    SRC.*
FROM {{ ref('V_STG_D_PRD_UDA_LU') }} SRC

