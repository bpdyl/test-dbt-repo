{{ config(
    materialized='table',
    alias='TMP_D_PRD_UDA_ITM_MTX',
    schema='DW_TMP',
    on_schema_change = 'append_new_columns',
    tags=['d_prd_uda_itm_mtx_ld'],
    pre_hook=["{{ start_script('d_prd_uda_itm_mtx_ld','RUNNING','NONE') }}"],
    post_hook=["{{ log_dml_audit(this, ref('V_STG_D_PRD_UDA_ITM_MTX'),'CREATE_TABLE_AS_SELECT') }}"]
) }}

SELECT
    SRC.*
    ,COALESCE(ITM.ITM_KEY,{{ dbt_utils.generate_surrogate_key(['SRC.ITM_ID'])}},'-1')  AS ITM_KEY
FROM {{ ref('V_STG_D_PRD_UDA_ITM_MTX') }} SRC
LEFT OUTER JOIN {{ ref('V_DWH_D_PRD_ITM_LU') }} ITM
  ON SRC.ITM_ID = ITM.ITM_ID
ORDER BY TO_NUMBER(SRC.UDA_ID)

