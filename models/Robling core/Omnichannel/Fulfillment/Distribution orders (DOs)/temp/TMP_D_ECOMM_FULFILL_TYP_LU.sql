{{ config(
    materialized='table',
    alias='TMP_D_ECOMM_FULFILL_TYP_LU',
    schema='DW_TMP',
    tags=['d_ecomm_fulfill_typ_ld'],
    pre_hook=["{{ start_script('d_ecomm_fulfill_typ_ld','RUNNING','NONE') }}"],
    post_hook=["{{ log_dml_audit(this,ref('V_STG_D_ECOMM_FULFILL_TYP_LU'),'CREATE_TABLE_AS_SELECT') }}"]
) }}
SELECT
    SRC.* 
FROM {{ ref('V_STG_D_ECOMM_FULFILL_TYP_LU') }} SRC

