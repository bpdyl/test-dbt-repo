{{ config(
    materialized='delete_merge',
    transient=false,
    alias='DWH_D_PRD_UDA_ITM_MTX',
    schema='DW_DWH',
    unique_key=['ITM_ID','UDA_ID','UDA_VALUE'],
    delete_matching_keys=['ITM_ID','UDA_ID','UDA_VALUE'],
    delete_constraint='NOT_IN_SRC',
    on_schema_change='append_new_columns',
    tags=['d_prd_uda_itm_mtx_ld'],
    post_hook = ["{{ log_dml_audit(this,ref('TMP_D_PRD_UDA_ITM_MTX'),'MERGE') }}"
                ,"{{ log_script_success(this) }}"
    ]
) }}

SELECT
    *
    ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                   AS RCD_INS_TS
    ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                   AS RCD_UPD_TS
FROM {{ ref('TMP_D_PRD_UDA_ITM_MTX') }}
