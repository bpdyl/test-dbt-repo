{{ config(
    materialized='table',
    alias='TMP_D_ORG_RGN_LU',
    schema='DW_TMP',
    on_schema_change = 'append_new_columns',
    tags=['d_org_rgn_ld'],
    pre_hook=["{{ start_script('d_org_rgn_ld','RUNNING','NONE') }}"],
    post_hook=["{{ log_dml_audit(this,ref('V_STG_D_ORG_RGN_LU'),'CREATE_TABLE_AS_SELECT') }}"]
) }}

SELECT
    SRC.*
    ,COALESCE(_ROLLUP.ARA_KEY, '-1')     AS ARA_KEY
    ,_ROLLUP.ARA_NUM                     AS ARA_NUM
    ,_ROLLUP.ARA_DESC                    AS ARA_DESC
    ,COALESCE(_ROLLUP.CHN_KEY, '-1')     AS CHN_KEY
    ,_ROLLUP.CHN_ID                      AS CHN_ID
    ,_ROLLUP.CHN_NUM                     AS CHN_NUM
    ,_ROLLUP.CHN_DESC                    AS CHN_DESC
FROM {{ ref('V_STG_D_ORG_RGN_LU') }} SRC
LEFT OUTER JOIN {{ ref('V_DWH_D_ORG_ARA_LU') }} _ROLLUP
    ON SRC.ARA_ID = _ROLLUP.ARA_ID 