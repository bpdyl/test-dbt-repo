{{ config(
    materialized='table',
    alias='TMP_D_PRD_SBC_LU',
    schema='DW_TMP',
    on_schema_change = 'append_new_columns',
    tags=['d_prd_sbc_ld'],
    pre_hook=["{{ start_script('d_prd_sbc_ld','RUNNING','NONE') }}"],
    post_hook=["{{ log_dml_audit(this, ref('V_STG_D_PRD_SBC_LU'),'CREATE_TABLE_AS_SELECT') }}"]
) }}

SELECT
    SRC.*
    , COALESCE(_ROLLUP.CLS_KEY, '-1')   AS CLS_KEY
    , _ROLLUP.CLS_NUM                   AS CLS_NUM
    , _ROLLUP.CLS_DESC                  AS CLS_DESC
    , COALESCE(_ROLLUP.DPT_KEY, '-1')   AS DPT_KEY
    , _ROLLUP.DPT_ID                    AS DPT_ID
    , _ROLLUP.DPT_NUM                   AS DPT_NUM
    , _ROLLUP.DPT_DESC                  AS DPT_DESC
    , COALESCE(_ROLLUP.GRP_KEY, '-1')   AS GRP_KEY
    , _ROLLUP.GRP_ID                    AS GRP_ID
    , _ROLLUP.GRP_NUM                   AS GRP_NUM
    , _ROLLUP.GRP_DESC                  AS GRP_DESC
    , COALESCE(_ROLLUP.DIV_KEY, '-1')   AS DIV_KEY
    , _ROLLUP.DIV_ID                    AS DIV_ID
    , _ROLLUP.DIV_NUM                   AS DIV_NUM
    , _ROLLUP.DIV_DESC                  AS DIV_DESC
FROM {{ ref('V_STG_D_PRD_SBC_LU') }} SRC
LEFT OUTER JOIN {{ ref('V_DWH_D_PRD_CLS_LU') }} _ROLLUP
  ON _ROLLUP.CLS_ID = SRC.CLS_ID
