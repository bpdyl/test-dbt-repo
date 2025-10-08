{{ config(
    materialized='table',
    alias='TMP_D_PRD_CLS_LU',
    schema='DW_TMP',
    tags=['d_prd_cls_ld'],
    pre_hook=["{{ start_script('d_prd_cls_ld','RUNNING','NONE') }}"],
    post_hook=["{{ log_dml_audit(this, ref('V_STG_D_PRD_CLS_LU'),'CREATE_TABLE_AS_SELECT') }}"]
) }}

SELECT
  SRC.*
  ,COALESCE(_ROLLUP.DPT_KEY, '-1')           AS DPT_KEY
  ,_ROLLUP.DPT_NUM                           AS DPT_NUM
  ,_ROLLUP.DPT_DESC                          AS DPT_DESC
  ,COALESCE(_ROLLUP.GRP_KEY, '-1')           AS GRP_KEY
  ,_ROLLUP.GRP_ID                            AS GRP_ID
  ,_ROLLUP.GRP_DESC                          AS GRP_DESC
  ,_ROLLUP.GRP_NUM                           AS GRP_NUM
  ,COALESCE(_ROLLUP.DIV_KEY, '-1')           AS DIV_KEY
  ,_ROLLUP.DIV_ID                            AS DIV_ID
  ,_ROLLUP.DIV_NUM                           AS DIV_NUM
  ,_ROLLUP.DIV_DESC                          AS DIV_DESC
FROM {{ ref('V_STG_D_PRD_CLS_LU') }} SRC
LEFT OUTER JOIN {{ ref('V_DWH_D_PRD_DPT_LU') }} _ROLLUP
  ON SRC.DPT_ID = _ROLLUP.DPT_ID
