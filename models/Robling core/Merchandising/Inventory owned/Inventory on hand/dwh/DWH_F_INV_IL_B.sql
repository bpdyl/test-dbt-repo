{% set key_columns = ['ITM_ID','LOC_ID'] %}
{{ config(
    materialized='custom_merge',
    unique_key=key_columns,
    key_columns=key_columns,
    model_name="Inventory",
    alias='DWH_F_INV_IL_B',
    schema='DW_DWH',
    tags = ['f_inv_ild_ld'],
    pre_hook = '{{ insert_missing_into_temp(this) }}',
    post_hook = ["{{ log_dml_audit(this,ref('TMP_F_INV_ILD_B'),'MERGE') }}"]
) }}

SELECT
    TMP.* EXCLUDE(EFF_START_DT) -- Excluding EFF_START_DT as it is mapped to LAST_EFF_START_DT in curr table. 
    ,EFF_START_DT                          AS LAST_EFF_START_DT
    ,CURRENT_TIMESTAMP()::TIMESTAMP_NTZ    AS RCD_INS_TS
    ,CURRENT_TIMESTAMP()::TIMESTAMP_NTZ    AS RCD_UPD_TS
FROM {{ ref('TMP_F_INV_ILD_B') }} TMP

