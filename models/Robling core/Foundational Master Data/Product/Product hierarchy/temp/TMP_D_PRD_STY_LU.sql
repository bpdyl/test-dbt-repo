{{ config(
    materialized='table',
    alias='TMP_D_PRD_STY_LU',
    schema='DW_TMP',
    on_schema_change = 'append_new_columns',
    tags=['d_prd_sty_ld'],
    pre_hook=["{{ start_script('d_prd_sty_ld','RUNNING','NONE') }}"],
    post_hook=["{{ log_dml_audit(this, ref('V_STG_D_PRD_STY_LU'),'CREATE_TABLE_AS_SELECT') }}"]
) }}

SELECT
    SRC.*
    ,COALESCE(_ROLLUP.SBC_KEY, '-1')               AS SBC_KEY
    ,_ROLLUP.SBC_NUM                               AS SBC_NUM
    ,_ROLLUP.SBC_DESC                              AS SBC_DESC
    ,COALESCE(_ROLLUP.CLS_KEY, '-1')               AS CLS_KEY
    ,_ROLLUP.CLS_ID                                AS CLS_ID
    ,_ROLLUP.CLS_NUM                               AS CLS_NUM
    ,_ROLLUP.CLS_DESC                              AS CLS_DESC
    ,COALESCE(_ROLLUP.DPT_KEY, '-1')               AS DPT_KEY
    ,_ROLLUP.DPT_ID                                AS DPT_ID
    ,_ROLLUP.DPT_NUM                               AS DPT_NUM
    ,_ROLLUP.DPT_DESC                              AS DPT_DESC
    ,COALESCE(_ROLLUP.GRP_KEY, '-1')               AS GRP_KEY
    ,_ROLLUP.GRP_ID                                AS GRP_ID
    ,_ROLLUP.GRP_NUM                               AS GRP_NUM
    ,_ROLLUP.GRP_DESC                              AS GRP_DESC
    ,COALESCE(_ROLLUP.DIV_KEY, '-1')               AS DIV_KEY
    ,_ROLLUP.DIV_ID                                AS DIV_ID
    ,_ROLLUP.DIV_NUM                               AS DIV_NUM
    ,_ROLLUP.DIV_DESC                              AS DIV_DESC
    ,SUP.SUP_KEY                                   AS SUP_KEY
    ,SUP.SUP_NUM                                   AS SUP_NUM
    ,SUP.SUP_DESC                                  AS SUP_DESC
FROM {{ ref('V_STG_D_PRD_STY_LU') }} SRC
LEFT OUTER JOIN {{ ref('V_DWH_D_PRD_SBC_LU') }} _ROLLUP
  ON _ROLLUP.SBC_ID = SRC.SBC_ID
LEFT OUTER JOIN {{ ref('V_DWH_D_SUP_LU') }} SUP
  ON SUP.SUP_ID = SRC.SUP_ID
