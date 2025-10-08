{{ config(
    materialized='table',
    alias='TMP_D_ORG_CHNL_LU',
    schema='DW_TMP',
    on_schema_change = 'append_new_columns',
    tags=['d_org_chnl_ld'],
    pre_hook=["{{ start_script('d_org_chnl_ld','RUNNING','NONE') }}"],
    post_hook=["{{ log_dml_audit(this,ref('V_STG_D_ORG_CHNL_LU'),'CREATE_TABLE_AS_SELECT') }}"]
) }}

SELECT
    SRC.*
FROM {{ ref('V_STG_D_ORG_CHNL_LU') }} SRC 