{{ config(
    materialized='table',
    alias='TMP_F_INV_RTV_SUP_ILD_B',
    schema='DW_TMP',
    tags = ['f_inv_rtv_sup_ild_ld'],
    meta = {'recon_config_macro': 'mac_f_rtv_recon_script_sql'},
    pre_hook=["{{ start_script('f_inv_rtv_sup_ild_ld','RUNNING','NONE') }}"
             ,"{{ load_recon_data('RTV',recon_config_macro='mac_f_rtv_recon_script_sql', recon_step=0) }}"],
    post_hook=["{{ log_dml_audit(this,ref('V_STG_F_INV_RTV_SUP_ILD_B'),'CREATE_TABLE_AS_SELECT') }}"]
) }}
SELECT
  SRC.*
  ,{{ get_coalesced_surrogate_key('LOC.LOC_KEY','SRC.LOC_ID') }}                AS LOC_KEY
  ,{{ get_coalesced_surrogate_key('SUP.SUP_KEY','SRC.SUP_ID') }}                AS SUP_KEY
  ,{{ get_coalesced_surrogate_key('ITM.ITM_KEY','SRC.ITM_ID') }}                AS ITM_KEY
  ,INV.ITMLOC_STTS_CDE                                                          AS ITMLOC_STTS_CDE
  ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
          THEN F_RTV_CST_LCL
          ELSE F_RTV_CST_LCL * EXCRT.EXCH_RATE
        END                                                                     AS F_RTV_CST
  ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
          THEN SRC.F_RTV_RTL_LCL
          ELSE SRC.F_RTV_RTL_LCL * EXCRT.EXCH_RATE
        END                                                                      AS F_RTV_RTL
FROM {{ ref('V_STG_F_INV_RTV_SUP_ILD_B') }} SRC
LEFT OUTER JOIN {{ ref('V_DWH_D_ORG_LOC_LU') }} LOC
    ON SRC.LOC_ID = LOC.LOC_ID
LEFT OUTER JOIN {{ ref('V_DWH_D_SUP_LU') }} SUP
    ON SRC.SUP_ID = SUP.SUP_ID
LEFT OUTER JOIN {{ ref('V_DWH_D_PRD_ITM_LU') }} ITM
    ON SRC.ITM_ID = ITM.ITM_ID
LEFT OUTER JOIN {{ ref('V_DWH_F_INV_ILD_B') }} INV 
    ON SRC.LOC_ID = INV.LOC_ID AND SRC.ITM_ID = INV.ITM_ID 
    AND SRC.TXN_DT BETWEEN INV.EFF_START_DT AND INV.EFF_END_DT
LEFT OUTER JOIN DW_DWH_V.V_DWH_F_EXCH_RATE_LU EXCRT
    ON (EXCRT.FROM_CNCY_CDE = SRC.LCL_CNCY_CDE AND EXCRT.TO_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
            AND TO_DATE(SRC.TXN_DT) BETWEEN EXCRT.EFF_FROM_DT AND EXCRT.EFF_TO_DT)
-- ordering by TXN_DT, LOC_KEY for performance through Snowflake partitioning
ORDER BY TXN_DT, LOC.LOC_KEY
