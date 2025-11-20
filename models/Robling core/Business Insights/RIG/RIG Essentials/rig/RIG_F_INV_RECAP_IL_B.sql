{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    transient=false,
    alias='RIG_F_INV_RECAP_IL_B',
    schema='DW_RIG',
    on_schema_change='append_new_columns',
    tags=['f_rig_inv_recap_il_b'],
    post_hook = ["{{ log_dml_audit(this,ref('V_CFG_RIG_F_INV_RECAP_IL_B'),'INSERT') }}"
               ,"{{ upate_recap_md_dates_using_dwh_table() }}"]
) }}

SELECT 
    SRC.*  
    ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ       AS RCD_INS_TS     
    ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ       AS RCD_UPD_TS                       
FROM 
    {{ ref('V_CFG_RIG_F_INV_RECAP_IL_B') }} SRC