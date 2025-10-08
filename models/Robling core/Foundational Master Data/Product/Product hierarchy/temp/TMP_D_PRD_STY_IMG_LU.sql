{{ config(
    materialized='table',
    alias='TMP_D_PRD_STY_IMG_LU',
    schema='DW_TMP',
    tags=['d_prd_sty_img_ld'],
    pre_hook=["{{ start_script('d_prd_sty_img_ld','RUNNING','NONE') }}"],
    post_hook=["{{ log_dml_audit(this, ref('V_STG_D_PRD_STY_IMG_LU'),'CREATE_TABLE_AS_SELECT') }}"]
) }}

SELECT 
  SRC.*
  ,COALESCE(_ROLLUP.STY_KEY, '-1')          AS STY_KEY
FROM {{ ref('V_STG_D_PRD_STY_IMG_LU') }} SRC
LEFT OUTER JOIN {{ ref('V_DWH_D_PRD_STY_LU') }} _ROLLUP
  ON _ROLLUP.STY_ID = SRC.STY_ID
