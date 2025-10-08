{{ config(
    materialized='table',
    alias='TMP_D_PRD_DPT_LU',
    schema='DW_TMP',
    tags=['d_prd_dpt_ld'],
    pre_hook=["{{ start_script('d_prd_dpt_ld','RUNNING','NONE') }}"],
    post_hook=["{{ log_dml_audit(this, ref('V_STG_D_PRD_DPT_LU'),'CREATE_TABLE_AS_SELECT') }}"]
) }}

SELECT
    SRC.*
    , COALESCE(_ROLLUP.GRP_KEY, '-1')               AS GRP_KEY
    , _ROLLUP.GRP_NUM                               AS GRP_NUM
	  , _ROLLUP.GRP_DESC                              AS GRP_DESC
	  , COALESCE(_ROLLUP.DIV_KEY, '-1')               AS DIV_KEY
	  , _ROLLUP.DIV_ID                                AS DIV_ID
	  , _ROLLUP.DIV_NUM                               AS DIV_NUM
	  , _ROLLUP.DIV_DESC                              AS DIV_DESC
FROM {{ ref('V_STG_D_PRD_DPT_LU') }} SRC
LEFT OUTER JOIN {{ ref('V_DWH_D_PRD_GRP_LU') }} _ROLLUP
  ON _ROLLUP.GRP_ID = SRC.GRP_ID
