{{ config(
    materialized='table',
    alias='TMP_F_ECOMM_DO_HDR_B',
    schema='DW_TMP',
    tags=['f_ecomm_do_hdr_ld'],
    pre_hook=["{{ start_script('f_ecomm_do_hdr_ld','RUNNING','NONE') }}"],
    post_hook=["{{ log_dml_audit(this,ref('V_STG_F_ECOMM_DO_HDR_B'),'CREATE_TABLE_AS_SELECT') }}"]
) }}
SELECT
    SRC.*  
    ,CO.CO_NUM                                                                          AS CO_NUM
    ,CO.DMND_LOC_KEY                                                                    AS DMND_LOC_KEY
    ,CO.DMND_LOC_ID                                                                     AS DMND_LOC_ID
    ,{{ get_coalesced_surrogate_key('FULMNT_LOC.LOC_KEY','SRC.FULMNT_LOC_ID') }}        AS FULMNT_LOC_KEY
    ,{{ get_coalesced_surrogate_key('INV_LOC.LOC_KEY','SRC.INV_SRC_LOC_ID') }}          AS INV_SRC_LOC_KEY
    ,TO_DATE(SRC.DO_CREATED_TS)                                                         AS DO_CREATED_DT
    ,DO_MIN.MIN_KEY                                                                     AS DO_CREATED_MIN_KEY 
    ,TO_DATE(SRC.DO_INVOICE_TS)                                                         AS DO_INVOICE_DT
    ,INVOICE_MIN.MIN_KEY                                                                AS DO_INVOICE_MIN_KEY 
    ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
            THEN SRC.F_DO_DUTY_AMT_LCL
            ELSE SRC.F_DO_DUTY_AMT_LCL * EXCRT.EXCH_RATE
        END                                                                             AS F_DO_DUTY_AMT
    ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
            THEN SRC.F_DO_SHIPPING_CST_LCL
            ELSE SRC.F_DO_SHIPPING_CST_LCL * EXCRT.EXCH_RATE
        END                                                                             AS F_DO_SHIPPING_CST
FROM {{ ref('V_STG_F_ECOMM_DO_HDR_B') }} SRC
INNER JOIN {{ ref('V_DWH_F_ECOMM_CO_HDR_B') }} CO ON CO.CO_ID = SRC.CO_ID
LEFT JOIN {{ ref('V_DWH_D_ORG_LOC_LU') }} FULMNT_LOC ON SRC.FULMNT_LOC_ID = FULMNT_LOC.LOC_ID
LEFT JOIN {{ ref('V_DWH_D_ORG_LOC_LU') }} INV_LOC ON SRC.INV_SRC_LOC_ID = INV_LOC.LOC_ID
LEFT JOIN DW_DWH_V.V_DWH_D_TIM_MIN_OF_DAY_LU DO_MIN
ON DO_MIN.MIN_24HR_ID =  
    (CONCAT(
            TRIM(TO_CHAR(HOUR(COALESCE(SRC.DO_CREATED_TS,TO_TIMESTAMP('9999-12-31 00:00:00','yyyy-mm-dd hh24:mi:ss'))), '00'))
            ,TRIM(TO_CHAR(MINUTE(COALESCE(SRC.DO_CREATED_TS,TO_TIMESTAMP('9999-12-31 00:00:00','yyyy-mm-dd hh24:mi:ss'))), '00'))
        ))
LEFT JOIN DW_DWH_V.V_DWH_D_TIM_MIN_OF_DAY_LU INVOICE_MIN
ON INVOICE_MIN.MIN_24HR_ID =  
    (CONCAT(
        TRIM(TO_CHAR(HOUR(COALESCE(SRC.DO_INVOICE_TS,TO_TIMESTAMP('9999-12-31 00:00:00','yyyy-mm-dd hh24:mi:ss'))), '00'))
        ,TRIM(TO_CHAR(MINUTE(COALESCE(SRC.DO_INVOICE_TS,TO_TIMESTAMP('9999-12-31 00:00:00','yyyy-mm-dd hh24:mi:ss'))), '00'))
    ))
LEFT JOIN DW_DWH_V.V_DWH_F_EXCH_RATE_LU EXCRT
    ON (EXCRT.FROM_CNCY_CDE = SRC.LCL_CNCY_CDE AND EXCRT.TO_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
    AND TO_DATE(SRC.DO_CREATED_TS) BETWEEN EXCRT.EFF_FROM_DT AND EXCRT.EFF_TO_DT)
-- ordering by DO_CREATED_TS, DMND_LOC_KEY for performance through Snowflake partitioning
ORDER BY SRC.DO_CREATED_TS, CO.DMND_LOC_KEY
