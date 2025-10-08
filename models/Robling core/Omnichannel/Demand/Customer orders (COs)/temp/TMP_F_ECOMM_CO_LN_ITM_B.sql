{{ config(
    materialized='table',
    alias='TMP_F_ECOMM_CO_LN_ITM_B',
    schema='DW_TMP',
    tags = ['f_ecomm_co_ln_itm_ld'],
    pre_hook=["{{ start_script('f_ecomm_co_ln_itm_ld','RUNNING','NONE') }}"
             ,"{{ load_recon_data('Customer Order',recon_config_macro='mac_f_co_recon_script_sql', recon_step=0) }}"],
    post_hook=["{{ log_dml_audit(this,ref('V_STG_F_ECOMM_CO_LN_ITM_B'),'CREATE_TABLE_AS_SELECT') }}"
              ,"{{ update_cst_from_inv() }}"]
) }}
SELECT
  SRC.* EXCLUDE(ITM_ID, CO_LN_ITM_STTS, DLVRY_TYP, DMND_LOC_ID) -- Excluding columns that are redefined in the select statement to avoid duplicate column issues 
  ,TO_DATE(SRC.CO_ORD_TS)                                        AS CO_ORD_DT
  ,COALESCE(LOC.LOC_KEY, COH.DMND_LOC_KEY)                       AS DMND_LOC_KEY
  ,COH.DMND_LOC_ID                                               AS DMND_LOC_ID
  ,{{ get_coalesced_surrogate_key('ITM.ITM_KEY','SRC.ITM_ID') }} AS ITM_KEY
  ,COALESCE(SRC.ITM_ID, '-1')                                    AS ITM_ID
  ,'-1'                                                          AS ITMLOC_STTS_CDE
  ,COALESCE(SRC.CO_LN_ITM_STTS, '-1')                            AS CO_LN_ITM_STTS
  ,ORD_MIN.MIN_KEY                                               AS CO_ORD_MIN_KEY
  ,TO_DATE(SRC.DO_CREATED_TS)                                    AS DO_CREATED_DT
  ,DO_CREATED_MIN.MIN_KEY                                        AS DO_CREATED_MIN_KEY
  ,TO_DATE(SRC.BACK_ORD_TS)                                      AS BACK_ORD_DT
  ,BACK_ORD_MIN.MIN_KEY                                          AS BACK_ORD_MIN_KEY
  ,TO_DATE(SRC.INVOICE_TS)                                       AS INVOICE_DT
  ,INVOICE_MIN.MIN_KEY                                           AS INVOICE_MIN_KEY
  ,TO_DATE(SRC.CANCLD_TS)                                        AS CANCLD_DT
  ,CANCLD_MIN.MIN_KEY                                            AS CANCLD_MIN_KEY
  ,TO_DATE(SRC.RTRN_TS)                                          AS RTRN_DT
  ,RTRN_MIN.MIN_KEY                                              AS RTRN_MIN_KEY
  ,COALESCE(SRC.DLVRY_TYP, '-1')                                 AS DLVRY_TYP
  ,CASE
      WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
          THEN SRC.F_CO_ORIG_UNIT_RTL_LCL
      ELSE SRC.F_CO_ORIG_UNIT_RTL_LCL * EXCRT.EXCH_RATE
    END                                                          AS F_CO_ORIG_UNIT_RTL
  ,CASE
      WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
          THEN SRC.F_CO_PAID_UNIT_RTL_LCL
      ELSE SRC.F_CO_PAID_UNIT_RTL_LCL * EXCRT.EXCH_RATE
    END                                                          AS F_CO_PAID_UNIT_RTL
  ,NULL                                                          AS F_CO_UNIT_CST_LCL
  ,NULL                                                          AS F_CO_UNIT_CST
  ,CASE
      WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
          THEN SRC.F_CO_ORD_RTL_LCL
      ELSE SRC.F_CO_ORD_RTL_LCL * EXCRT.EXCH_RATE
    END                                                          AS F_CO_ORD_RTL
  ,NULL                                                          AS F_CO_ORD_CST_LCL
  ,NULL                                                          AS F_CO_ORD_CST
  ,CASE
      WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
          THEN SRC.F_CO_DSC_AMT_LCL
      ELSE SRC.F_CO_DSC_AMT_LCL * EXCRT.EXCH_RATE
    END                                                          AS F_CO_DSC_AMT
  ,CASE
      WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
          THEN SRC.F_CO_TAX_AMT_LCL
      ELSE SRC.F_CO_TAX_AMT_LCL * EXCRT.EXCH_RATE
    END                                                          AS F_CO_TAX_AMT
  ,CASE
      WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
          THEN SRC.F_CO_LN_ADDTNL_CHRGS_LCL
      ELSE SRC.F_CO_LN_ADDTNL_CHRGS_LCL * EXCRT.EXCH_RATE
    END                                                          AS F_CO_LN_ADDTNL_CHRGS
  ,CASE
      WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
          THEN SRC.F_CO_LN_ORD_TOT_AMT_LCL
      ELSE SRC.F_CO_LN_ORD_TOT_AMT_LCL * EXCRT.EXCH_RATE
    END                                                          AS F_CO_LN_ORD_TOT_AMT
FROM {{ ref('V_STG_F_ECOMM_CO_LN_ITM_B') }} SRC
INNER JOIN {{ ref('V_DWH_F_ECOMM_CO_HDR_B') }} COH ON SRC.CO_ID = COH.CO_ID
LEFT JOIN {{ ref('V_DWH_D_ORG_LOC_LU') }} LOC ON COH.DMND_LOC_ID = LOC.LOC_ID
LEFT JOIN {{ ref('V_DWH_D_PRD_ITM_LU') }} ITM ON SRC.ITM_ID = ITM.ITM_ID
LEFT JOIN DW_DWH_V.V_DWH_D_TIM_MIN_OF_DAY_LU ORD_MIN ON ORD_MIN.MIN_24HR_ID =
  (CONCAT(
    TRIM(TO_CHAR(HOUR(COALESCE(SRC.CO_ORD_TS,TO_TIMESTAMP('9999-12-31 00:00:00','yyyy-mm-dd hh24:mi:ss'))), '00'))
    ,TRIM(TO_CHAR(MINUTE(COALESCE(SRC.CO_ORD_TS,TO_TIMESTAMP('9999-12-31 00:00:00','yyyy-mm-dd hh24:mi:ss'))), '00'))
  ))
LEFT JOIN DW_DWH_V.V_DWH_D_TIM_MIN_OF_DAY_LU DO_CREATED_MIN ON DO_CREATED_MIN.MIN_24HR_ID =
  (CONCAT(
    TRIM(TO_CHAR(HOUR(COALESCE(SRC.DO_CREATED_TS,TO_TIMESTAMP('9999-12-31 00:00:00','yyyy-mm-dd hh24:mi:ss'))), '00'))
    ,TRIM(TO_CHAR(MINUTE(COALESCE(SRC.DO_CREATED_TS,TO_TIMESTAMP('9999-12-31 00:00:00','yyyy-mm-dd hh24:mi:ss'))), '00'))
  ))
LEFT JOIN DW_DWH_V.V_DWH_D_TIM_MIN_OF_DAY_LU INVOICE_MIN ON INVOICE_MIN.MIN_24HR_ID =
  (CONCAT(
    TRIM(TO_CHAR(HOUR(COALESCE(SRC.INVOICE_TS,TO_TIMESTAMP('9999-12-31 00:00:00','yyyy-mm-dd hh24:mi:ss'))), '00'))
    ,TRIM(TO_CHAR(MINUTE(COALESCE(SRC.INVOICE_TS,TO_TIMESTAMP('9999-12-31 00:00:00','yyyy-mm-dd hh24:mi:ss'))), '00'))
  ))
LEFT JOIN DW_DWH_V.V_DWH_D_TIM_MIN_OF_DAY_LU BACK_ORD_MIN ON BACK_ORD_MIN.MIN_24HR_ID =
  (CONCAT(
    TRIM(TO_CHAR(HOUR(COALESCE(SRC.BACK_ORD_TS,TO_TIMESTAMP('9999-12-31 00:00:00','yyyy-mm-dd hh24:mi:ss'))), '00'))
    ,TRIM(TO_CHAR(MINUTE(COALESCE(SRC.BACK_ORD_TS,TO_TIMESTAMP('9999-12-31 00:00:00','yyyy-mm-dd hh24:mi:ss'))), '00'))
  ))
LEFT JOIN DW_DWH_V.V_DWH_D_TIM_MIN_OF_DAY_LU CANCLD_MIN ON CANCLD_MIN.MIN_24HR_ID =
  (CONCAT(
      TRIM(TO_CHAR(HOUR(COALESCE(SRC.CANCLD_TS,TO_TIMESTAMP('9999-12-31 00:00:00','yyyy-mm-dd hh24:mi:ss'))), '00'))
      ,TRIM(TO_CHAR(MINUTE(COALESCE(SRC.CANCLD_TS,TO_TIMESTAMP('9999-12-31 00:00:00','yyyy-mm-dd hh24:mi:ss'))), '00'))
  ))
LEFT JOIN DW_DWH_V.V_DWH_D_TIM_MIN_OF_DAY_LU RTRN_MIN ON RTRN_MIN.MIN_24HR_ID =
  (CONCAT(
    TRIM(TO_CHAR(HOUR(COALESCE(SRC.RTRN_TS,TO_TIMESTAMP('9999-12-31 00:00:00','yyyy-mm-dd hh24:mi:ss'))), '00'))
    ,TRIM(TO_CHAR(MINUTE(COALESCE(SRC.RTRN_TS,TO_TIMESTAMP('9999-12-31 00:00:00','yyyy-mm-dd hh24:mi:ss'))), '00'))
  ))
LEFT JOIN DW_DWH_V.V_DWH_F_EXCH_RATE_LU EXCRT
    ON (EXCRT.FROM_CNCY_CDE = SRC.LCL_CNCY_CDE AND EXCRT.TO_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
            AND TO_DATE(SRC.CO_ORD_TS) BETWEEN EXCRT.EFF_FROM_DT AND EXCRT.EFF_TO_DT)
-- ordering by CO_ORD_TS, LOC_KEY for performance through Snowflake partitioning
ORDER BY SRC.CO_ORD_TS, LOC.LOC_KEY