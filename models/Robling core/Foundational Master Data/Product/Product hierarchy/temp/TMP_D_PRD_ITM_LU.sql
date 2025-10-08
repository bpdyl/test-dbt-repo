{{ config(
    materialized='table',
    alias='TMP_D_PRD_ITM_LU',
    schema='DW_TMP',
    on_schema_change = 'append_new_columns',
    tags=['d_prd_itm_ld'],
    pre_hook=["{{ start_script('d_prd_itm_ld','RUNNING','NONE') }}"],
    post_hook=["{{ log_dml_audit(this, ref('V_STG_D_PRD_ITM_LU'),'CREATE_TABLE_AS_SELECT') }}"]
) }}

SELECT
    SRC.*
   ,COALESCE(_ROLLUP.STY_KEY, '-1')    AS STY_KEY
   ,_ROLLUP.STY_NUM                    AS STY_NUM
   ,_ROLLUP.STY_DESC                   AS STY_DESC
   ,COALESCE(_ROLLUP.SBC_KEY,'-1')     AS SBC_KEY
   ,_ROLLUP.SBC_ID                     AS SBC_ID
   ,_ROLLUP.SBC_NUM                    AS SBC_NUM
   ,_ROLLUP.SBC_DESC                   AS SBC_DESC
   ,COALESCE(_ROLLUP.CLS_KEY,'-1')     AS CLS_KEY
   ,_ROLLUP.CLS_ID                     AS CLS_ID
   ,_ROLLUP.CLS_NUM                    AS CLS_NUM
   ,_ROLLUP.CLS_DESC                   AS CLS_DESC
   ,COALESCE(_ROLLUP.DPT_KEY,'-1')     AS DPT_KEY
   ,_ROLLUP.DPT_ID                     AS DPT_ID
   ,_ROLLUP.DPT_NUM                    AS DPT_NUM
   ,_ROLLUP.DPT_DESC                   AS DPT_DESC
   ,COALESCE(_ROLLUP.GRP_KEY,'-1')     AS GRP_KEY
   ,_ROLLUP.GRP_ID                     AS GRP_ID
   ,_ROLLUP.GRP_NUM                    AS GRP_NUM
   ,_ROLLUP.GRP_DESC                   AS GRP_DESC
   ,COALESCE(_ROLLUP.DIV_KEY,'-1')     AS DIV_KEY
   ,_ROLLUP.DIV_ID                     AS DIV_ID
   ,_ROLLUP.DIV_NUM                    AS DIV_NUM
   ,_ROLLUP.DIV_DESC                   AS DIV_DESC
   ,_ROLLUP.SUP_KEY                    AS SUP_KEY
   ,_ROLLUP.SUP_ID                     AS SUP_ID
   ,_ROLLUP.SUP_NUM                    AS SUP_NUM
   ,_ROLLUP.SUP_DESC                   AS SUP_DESC
   ,COALESCE(COLOR_KEY,'-1')           AS COLOR_KEY
   ,COLOR.COLOR_NUM                    AS COLOR_NUM
   ,COLOR.COLOR_DESC                   AS COLOR_DESC
   ,COALESCE(SIZE_KEY,'-1')            AS SIZE_KEY
   ,SIZE.SIZE_NUM                      AS SIZE_NUM
   ,SIZE.SIZE_DESC                     AS SIZE_DESC
   ,SIZE.SIZE_SORT_NUM                 AS SIZE_SORT_NUM
FROM {{ ref('V_STG_D_PRD_ITM_LU') }} SRC
LEFT OUTER JOIN {{ ref('V_DWH_D_PRD_STY_LU') }} _ROLLUP
    ON _ROLLUP.STY_ID = SRC.STY_ID
LEFT OUTER JOIN {{ ref('V_DWH_D_PRD_COLOR_LU') }}  COLOR
    ON SRC.COLOR_ID=COLOR.COLOR_ID
LEFT OUTER JOIN {{ ref('V_DWH_D_PRD_SIZE_LU') }}  SIZE
    ON SRC.SIZE_ID=SIZE.SIZE_ID

