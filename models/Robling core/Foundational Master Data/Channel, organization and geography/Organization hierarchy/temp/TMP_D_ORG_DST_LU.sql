{{ config(
    materialized='table',
    alias='TMP_D_ORG_DST_LU',
    schema='DW_TMP',
    on_schema_change = 'append_new_columns',
    tags=['d_org_dst_ld'],
    pre_hook=["{{ start_script('d_org_dst_ld','RUNNING','NONE') }}"],
    post_hook=["{{ log_dml_audit(this,ref('V_STG_D_ORG_DST_LU'),'CREATE_TABLE_AS_SELECT') }}"]
) }}

SELECT
    SRC.*
    ,COALESCE(_ROLLUP.RGN_KEY, '-1')    AS RGN_KEY
    ,_ROLLUP.RGN_NUM                    AS RGN_NUM
    ,_ROLLUP.RGN_DESC                   AS RGN_DESC
    ,COALESCE(_ROLLUP.ARA_KEY, '-1')    AS ARA_KEY
    ,_ROLLUP.ARA_ID                     AS ARA_ID
    ,_ROLLUP.ARA_NUM                    AS ARA_NUM
    ,_ROLLUP.ARA_DESC                   AS ARA_DESC
    ,COALESCE(_ROLLUP.CHN_KEY, '-1')    AS CHN_KEY
    ,_ROLLUP.CHN_ID                     AS CHN_ID
    ,_ROLLUP.CHN_NUM                    AS CHN_NUM
    ,_ROLLUP.CHN_DESC                   AS CHN_DESC
FROM {{ ref('V_STG_D_ORG_DST_LU') }} SRC
LEFT OUTER JOIN {{ ref('V_DWH_D_ORG_RGN_LU') }} _ROLLUP
    ON SRC.RGN_ID = _ROLLUP.RGN_ID 