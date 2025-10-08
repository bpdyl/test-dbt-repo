{{ config(
    materialized='table',
    alias='TMP_D_ORG_LOC_LU',
    schema='DW_TMP',
    on_schema_change = 'append_new_columns',
    tags=['d_org_loc_ld'],
    pre_hook=["{{ start_script('d_org_loc_ld','RUNNING','NONE') }}"],
    post_hook=["{{ log_dml_audit(this,ref('V_STG_D_ORG_LOC_LU'),'CREATE_TABLE_AS_SELECT') }}"]
) }}

SELECT
    SRC.*
    ,COALESCE(_ROLLUP.DST_KEY, '-1')        AS DST_KEY
    ,_ROLLUP.DST_NUM                      AS DST_NUM
    ,_ROLLUP.DST_DESC                     AS DST_DESC
    ,COALESCE(_ROLLUP.RGN_KEY, '-1')        AS RGN_KEY
    ,_ROLLUP.RGN_ID                       AS RGN_ID
    ,_ROLLUP.RGN_NUM                      AS RGN_NUM
    ,_ROLLUP.RGN_DESC                     AS RGN_DESC
    ,COALESCE(_ROLLUP.ARA_KEY, '-1')      AS ARA_KEY
    ,_ROLLUP.ARA_ID                       AS ARA_ID
    ,_ROLLUP.ARA_NUM                      AS ARA_NUM
    ,_ROLLUP.ARA_DESC                     AS ARA_DESC
    ,COALESCE(_ROLLUP.CHN_KEY, '-1')      AS CHN_KEY
    ,_ROLLUP.CHN_ID                       AS CHN_ID
    ,_ROLLUP.CHN_NUM                      AS CHN_NUM
    ,_ROLLUP.CHN_DESC                     AS CHN_DESC
    ,COALESCE(CHNL.CHNL_KEY, '-1')        AS CHNL_KEY
    ,CHNL.CHNL_NUM                        AS CHNL_NUM
    ,CHNL.CHNL_DESC                       AS CHNL_DESC
FROM {{ ref('V_STG_D_ORG_LOC_LU') }} SRC
LEFT OUTER JOIN {{ ref('V_DWH_D_ORG_DST_LU') }} _ROLLUP
    ON SRC.DST_ID = _ROLLUP.DST_ID
LEFT OUTER JOIN {{ ref('V_DWH_D_ORG_CHNL_LU') }} CHNL
    ON SRC.CHNL_ID = CHNL.CHNL_ID 