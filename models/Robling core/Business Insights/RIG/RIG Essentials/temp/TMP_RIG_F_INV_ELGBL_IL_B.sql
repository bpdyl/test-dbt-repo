{{ config(
    materialized='table',
    alias='TMP_RIG_F_INV_ELGBL_IL_B',
    schema='DW_TMP',
    on_schema_change = 'append_new_columns',
    tags=['f_rig_inv_elgbl_il_b'],
    pre_hook=["{{ start_script('f_rig_inv_elgbl_il_b','RUNNING','NONE') }}"],
    post_hook=["{{ log_dml_audit(this, ref('V_STG_RIG_F_INV_ELGBL_IL_B'),'CREATE_TABLE_AS_SELECT') }}"]
) }}

SELECT
    SRC.*
    ,{{ get_coalesced_surrogate_key('ITM.ITM_KEY','SRC.ITM_ID') }}               AS ITM_KEY
    ,{{ get_coalesced_surrogate_key('LOC.LOC_KEY','SRC.LOC_ID') }}               AS LOC_KEY
FROM 
    {{ ref('V_STG_RIG_F_INV_ELGBL_IL_B') }} SRC
    LEFT OUTER JOIN {{ ref("V_DWH_D_ORG_LOC_LU")}} LOC ON SRC.LOC_ID = LOC.LOC_ID
    LEFT OUTER JOIN {{ ref("V_DWH_D_PRD_ITM_LU")}} ITM ON SRC.ITM_ID = ITM.ITM_ID