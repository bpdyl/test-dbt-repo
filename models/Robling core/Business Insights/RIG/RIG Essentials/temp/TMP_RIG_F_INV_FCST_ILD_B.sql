{{ config(
    materialized='table',
    alias='TMP_RIG_F_INV_FCST_ILD_B',
    schema='DW_TMP',
    on_schema_change = 'append_new_columns',
    tags=['f_rig_inv_fcst_ild_b'],
    pre_hook=["{{ start_script('f_rig_inv_fcst_ild_b','RUNNING','NONE') }}"],
    post_hook=["{{ log_dml_audit(this, ref('V_STG_RIG_F_INV_FCST_ILD_B'),'CREATE_TABLE_AS_SELECT') }}"]
) }}

SELECT
    SRC.*
    ,{{ get_coalesced_surrogate_key('ITM.ITM_KEY','SRC.ITM_ID') }}               AS ITM_KEY
    ,{{ get_coalesced_surrogate_key('LOC.LOC_KEY','SRC.LOC_ID') }}               AS LOC_KEY
    ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
        THEN SRC.F_RIG_FCST_RTL_LCL
        ELSE SRC.F_RIG_FCST_RTL_LCL * EXCRT.EXCH_RATE
        END                                                                      AS F_RIG_FCST_RTL
    ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
        THEN SRC.F_RIG_FCST_CST_LCL
        ELSE SRC.F_RIG_FCST_CST_LCL * EXCRT.EXCH_RATE
        END                                                                      AS F_RIG_FCST_CST
FROM 
    {{ ref('V_STG_RIG_F_INV_FCST_ILD_B') }} SRC
LEFT OUTER JOIN {{ ref("V_DWH_D_ORG_LOC_LU")}} LOC ON SRC.LOC_ID = LOC.LOC_ID
LEFT OUTER JOIN {{ ref("V_DWH_D_PRD_ITM_LU")}} ITM ON SRC.ITM_ID = ITM.ITM_ID
LEFT OUTER JOIN DW_DWH_V.V_DWH_F_EXCH_RATE_LU EXCRT
ON (EXCRT.FROM_CNCY_CDE = SRC.LCL_CNCY_CDE AND EXCRT.TO_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
    AND TO_DATE(SRC.DAY_KEY) BETWEEN EXCRT.EFF_FROM_DT AND EXCRT.EFF_TO_DT)
-- ordering by TXN_DT, LOC_KEY for performance through Snowflake partitioning
ORDER BY SRC.DAY_KEY, LOC.LOC_KEY