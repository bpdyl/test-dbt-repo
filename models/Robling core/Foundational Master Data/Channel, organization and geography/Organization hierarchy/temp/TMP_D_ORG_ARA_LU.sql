{{ config(
    materialized='table',
    alias='TMP_D_ORG_ARA_LU',
    schema='DW_TMP',
    on_schema_change = 'append_new_columns',
    tags=['d_org_ara_ld'],
    pre_hook=["{{ start_script('d_org_ara_ld','RUNNING','NONE') }}"],
    post_hook=["{{ log_dml_audit(this,ref('V_STG_D_ORG_ARA_LU'),'CREATE_TABLE_AS_SELECT') }}"]
) }}

SELECT 
    SRC.*
    ,COALESCE(_ROLLUP.CHN_KEY, '-1')    AS CHN_KEY
    ,_ROLLUP.CHN_NUM                    AS CHN_NUM
    ,_ROLLUP.CHN_DESC                   AS CHN_DESC
FROM {{ ref('V_STG_D_ORG_ARA_LU') }} SRC
LEFT OUTER JOIN {{ ref('V_DWH_D_ORG_CHN_LU') }} _ROLLUP
    ON SRC.CHN_ID = _ROLLUP.CHN_ID 