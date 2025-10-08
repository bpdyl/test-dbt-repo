{{ config(
    materialized='table',
    alias='TMP_D_SUP_LU',
    schema='DW_TMP',
    tags=['d_sup_ld'],
    pre_hook=["{{ start_script('d_sup_ld','RUNNING','NONE') }}"],
    post_hook=["{{ log_dml_audit(this, ref('V_STG_D_SUP_LU'),'CREATE_TABLE_AS_SELECT') }}"]
) }}

SELECT 
  SRC.*
FROM {{ ref('V_STG_D_SUP_LU') }} SRC
