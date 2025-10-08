{{ config(
    materialized='table',
    alias='TMP_D_PRD_GRP_LU',
    schema='DW_TMP',
    on_schema_change = 'append_new_columns',
    tags=['d_prd_grp_ld'],
    pre_hook=["{{ start_script('d_prd_grp_ld','RUNNING','NONE') }}"],
    post_hook=["{{ log_dml_audit(this, ref('V_STG_D_PRD_GRP_LU'),'CREATE_TABLE_AS_SELECT') }}"]
) }}

SELECT
    SRC.*
    ,COALESCE(_ROLLUP.DIV_KEY, '-1')               AS DIV_KEY
    ,_ROLLUP.DIV_NUM                               AS DIV_NUM
    ,_ROLLUP.DIV_DESC                              AS DIV_DESC
FROM {{ ref('V_STG_D_PRD_GRP_LU') }} SRC
LEFT OUTER JOIN {{ ref('V_DWH_D_PRD_DIV_LU') }} _ROLLUP
  ON _ROLLUP.DIV_ID = SRC.DIV_ID
