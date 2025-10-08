{{ config(
    materialized='table',
    alias='TMP_F_ECOMM_DO_LN_ITM_B',
    schema='DW_TMP',
    tags = ['f_ecomm_do_ln_itm_ld'],
    pre_hook=["{{ start_script('f_ecomm_do_ln_itm_ld','RUNNING','NONE') }}"
            ,"{{ load_recon_data('Fulfillment',recon_config_macro='mac_f_do_recon_script_sql', recon_step=0) }}"],
    post_hook=["{{ log_dml_audit(this,ref('V_STG_F_ECOMM_DO_LN_ITM_B'),'CREATE_TABLE_AS_SELECT') }}"
              ,"{{ update_do_cst_from_inv() }}"]
) }}
SELECT
  SRC.* 
  ,DO_HDR.DMND_LOC_KEY                                              AS DMND_LOC_KEY
  ,DO_HDR.DMND_LOC_ID                                               AS DMND_LOC_ID
  ,DO_HDR.FULMNT_LOC_KEY                                            AS FULMNT_LOC_KEY
  ,DO_HDR.FULMNT_LOC_ID                                             AS FULMNT_LOC_ID
  ,DO_HDR.INV_SRC_LOC_KEY                                           AS INV_SRC_LOC_KEY
  ,DO_HDR.INV_SRC_LOC_ID                                            AS INV_SRC_LOC_ID
  ,{{ get_coalesced_surrogate_key('ITM.ITM_KEY','SRC.ITM_ID') }}    AS ITM_KEY
  ,'-1'                                                             AS ITMLOC_STTS_CDE
  ,TO_DATE(DO_HDR.DO_CREATED_TS)                                    AS DO_CREATED_DT
  ,DO_MIN.MIN_KEY                                                   AS DO_CREATED_MIN_KEY
  ,DO_HDR.DO_CREATED_TS                                             AS DO_CREATED_TS
  ,CO_LN_ITM.F_CO_ORIG_UNIT_RTL                                     AS F_CO_ORIG_UNIT_RTL
  ,CO_LN_ITM.F_CO_ORIG_UNIT_RTL_LCL                                 AS F_CO_ORIG_UNIT_RTL_LCL
  ,CO_LN_ITM.F_CO_PAID_UNIT_RTL                                     AS F_CO_PAID_UNIT_RTL
  ,CO_LN_ITM.F_CO_PAID_UNIT_RTL_LCL                                 AS F_CO_PAID_UNIT_RTL_LCL
  ,NULL                                                             AS F_DO_UNIT_CST
  ,NULL                                                             AS F_DO_UNIT_CST_LCL
  ,CASE
      WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
          THEN SRC.F_DO_TAX_AMT_LCL
      ELSE SRC.F_DO_TAX_AMT_LCL * EXCRT.EXCH_RATE
    END                                                             AS F_DO_TAX_AMT
  ,CASE
      WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
          THEN SRC.F_DO_LN_ORD_TOT_AMT_LCL
      ELSE SRC.F_DO_LN_ORD_TOT_AMT_LCL * EXCRT.EXCH_RATE
    END                                                             AS F_DO_LN_ORD_TOT_AMT
FROM {{ ref('V_STG_F_ECOMM_DO_LN_ITM_B') }} SRC
INNER JOIN {{ ref('V_DWH_F_ECOMM_DO_HDR_B') }} DO_HDR ON SRC.DO_ID = DO_HDR.DO_ID
LEFT JOIN {{ ref('V_DWH_F_ECOMM_CO_LN_ITM_B') }} CO_LN_ITM ON SRC.CO_ID = CO_LN_ITM.CO_ID AND SRC.CO_LN_ID = CO_LN_ITM.CO_LN_ID
LEFT JOIN {{ ref('V_DWH_D_PRD_ITM_LU') }} ITM ON SRC.ITM_ID = ITM.ITM_ID
LEFT JOIN DW_DWH_V.V_DWH_D_TIM_MIN_OF_DAY_LU DO_MIN ON DO_MIN.MIN_24HR_ID =
  (CONCAT(
    TRIM(TO_CHAR(HOUR(COALESCE(DO_HDR.DO_CREATED_TS,TO_TIMESTAMP('9999-12-31 00:00:00','yyyy-mm-dd hh24:mi:ss'))), '00'))
    ,TRIM(TO_CHAR(MINUTE(COALESCE(DO_HDR.DO_CREATED_TS,TO_TIMESTAMP('9999-12-31 00:00:00','yyyy-mm-dd hh24:mi:ss'))), '00'))
  ))
LEFT JOIN DW_DWH_V.V_DWH_F_EXCH_RATE_LU EXCRT
    ON (EXCRT.FROM_CNCY_CDE = SRC.LCL_CNCY_CDE AND EXCRT.TO_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
            AND TO_DATE(DO_HDR.DO_CREATED_TS) BETWEEN EXCRT.EFF_FROM_DT AND EXCRT.EFF_TO_DT)
-- ordering by DO_CREATED_TS, FULMNT_LOC_KEY for performance through Snowflake partitioning
ORDER BY DO_HDR.DO_CREATED_TS, DO_HDR.FULMNT_LOC_KEY