{{ config(
    materialized='table',
    alias='TMP_F_SLS_TXN_LN_ITM_B',
    schema='DW_TMP',
    tags = ['f_sls_txn_ln_itm_ld'],
    pre_hook=["{{ start_script('f_sls_txn_ln_itm_ld','RUNNING','NONE') }}"
             ,"{{ load_recon_data('Sales',recon_config_macro='mac_f_sls_recon_script_sql', recon_step=0) }}"],
    post_hook=["{{ log_dml_audit(this,ref('V_STG_F_SLS_TXN_LN_ITM_B'),'CREATE_TABLE_AS_SELECT') }}"]
) }}
SELECT
  SRC.*
  ,1                                                                              AS IS_CURRENT
  ,TO_DATE(SRC.TXN_TS)                                                            AS TXN_DT
  ,_MIN.MIN_KEY                                                                   AS TXN_MIN_KEY
  ,{{ get_coalesced_surrogate_key('LOC.LOC_KEY','SRC.LOC_ID') }}                  AS LOC_KEY
  ,ECOM.DMND_LOC_KEY                                                              AS DMND_LOC_KEY
  ,{{ get_coalesced_surrogate_key('ITM.ITM_KEY','SRC.ITM_ID') }}                  AS ITM_KEY
  ,NVL(ATTR.DO_ID, '-1')                                                          AS DO_ID
  ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
          THEN INV.F_UNIT_WAC_CST_LCL * SRC.F_SLS_QTY
          ELSE (INV.F_UNIT_WAC_CST_LCL * SRC.F_SLS_QTY) * EXCRT.EXCH_RATE
        END                                                                       AS F_SLS_CST
  ,INV.F_UNIT_WAC_CST_LCL * SRC.F_SLS_QTY                                         AS F_SLS_CST_LCL
  ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
          THEN SRC.F_SLS_RTL_LCL
          ELSE SRC.F_SLS_RTL_LCL * EXCRT.EXCH_RATE
        END                                                                       AS F_SLS_RTL
  ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
          THEN SRC.F_TOT_DSC_AMT_LCL
          ELSE SRC.F_TOT_DSC_AMT_LCL * EXCRT.EXCH_RATE
        END                                                                       AS F_TOT_DSC_AMT
  ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
          THEN SRC.F_EMP_DSC_AMT_LCL
          ELSE SRC.F_EMP_DSC_AMT_LCL * EXCRT.EXCH_RATE
        END                                                                       AS F_EMP_DSC_AMT
  ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
          THEN SRC.F_SLS_TAX_AMT_LCL
          ELSE SRC.F_SLS_TAX_AMT_LCL * EXCRT.EXCH_RATE
        END                                                                       AS F_SLS_TAX_AMT
  ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
          THEN SRC.F_UNIT_RTL_LCL
          ELSE SRC.F_UNIT_RTL_LCL * EXCRT.EXCH_RATE
        END                                                                       AS F_UNIT_RTL
  ,INV.F_UNIT_WAC_CST                                                             AS F_UNIT_CST
  ,INV.F_UNIT_WAC_CST_LCL                                                         AS F_UNIT_CST_LCL
FROM {{ ref('V_STG_F_SLS_TXN_LN_ITM_B') }} SRC
LEFT JOIN DW_DWH_V.V_DWH_D_TIM_MIN_OF_DAY_LU _MIN
    ON _MIN.MIN_24HR_ID = TRIM(CONCAT(
                          LPAD(HOUR(SRC.TXN_TS), 2, '0'),
                          LPAD(MINUTE(SRC.TXN_TS), 2, '0')
                        ))
LEFT JOIN {{ ref('V_DWH_F_SLS_TXN_ATTR_LU') }} ATTR
    ON ATTR.TXN_ID = SRC.TXN_ID
LEFT JOIN {{ ref('V_DWH_D_ORG_LOC_LU') }} LOC
    ON SRC.LOC_ID = LOC.LOC_ID
LEFT JOIN DW_DWH_V.V_DWH_F_ECOMM_CO_LN_ITM_B ECOM
    ON SRC.CO_ID = ECOM.CO_ID AND SRC.CO_LN_ID = ECOM.CO_LN_ID
LEFT JOIN {{ ref('V_DWH_D_PRD_ITM_LU') }} ITM
    ON SRC.ITM_ID = ITM.ITM_ID
LEFT JOIN DW_DWH_V.V_DWH_F_INV_ILD_B INV ON  INV.ITM_ID=ITM.ITM_ID AND INV.LOC_ID=LOC.LOC_ID
  AND TO_DATE(SRC.POST_DT) BETWEEN INV.EFF_START_DT AND INV.EFF_END_DT
LEFT OUTER JOIN DW_DWH_V.V_DWH_F_EXCH_RATE_LU EXCRT
    ON (EXCRT.FROM_CNCY_CDE = SRC.LCL_CNCY_CDE AND EXCRT.TO_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
            AND TO_DATE(SRC.TXN_TS) BETWEEN EXCRT.EFF_FROM_DT AND EXCRT.EFF_TO_DT)
-- ordering by TXN_DT, LOC_KEY for performance through Snowflake partitioning
ORDER BY TXN_DT, LOC.LOC_KEY
