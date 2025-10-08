{{ config(
    materialized='table',
    alias='TMP_F_ECOMM_CO_HDR_B',
    schema='DW_TMP',
    tags=['f_ecomm_co_hdr_ld'],
    pre_hook=["{{ start_script('f_ecomm_co_hdr_ld','RUNNING','NONE') }}"],
    post_hook=["{{ log_dml_audit(this,ref('V_STG_F_ECOMM_CO_HDR_B'),'CREATE_TABLE_AS_SELECT') }}"]
) }}
SELECT
    SRC.* EXCLUDE(DMND_LOC_ID) -- Excluding columns that are redefined in the select statement to avoid duplicate column issues 
    ,TO_DATE(SRC.CO_ORD_TS)                                             AS CO_ORD_DT
    ,{{ get_coalesced_surrogate_key('LOC.LOC_KEY','SRC.DMND_LOC_ID') }} AS DMND_LOC_KEY
    ,COALESCE(SRC.DMND_LOC_ID,'-1')                                     AS DMND_LOC_ID
    ,_MIN.MIN_KEY                                                       AS CO_ORD_MIN_KEY
    ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
            THEN SRC.F_CO_ORD_RTL_LCL
            ELSE SRC.F_CO_ORD_RTL_LCL * EXCRT.EXCH_RATE
        END                                                             AS F_CO_ORD_RTL
    ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
            THEN SRC.F_CO_DSC_AMT_LCL
            ELSE SRC.F_CO_DSC_AMT_LCL * EXCRT.EXCH_RATE
        END                                                             AS F_CO_DSC_AMT
    ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
            THEN SRC.F_CO_TAX_AMT_LCL
            ELSE SRC.F_CO_TAX_AMT_LCL * EXCRT.EXCH_RATE
        END                                                             AS F_CO_TAX_AMT
    ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
            THEN SRC.F_CO_SHIPPING_RTL_LCL
            ELSE SRC.F_CO_SHIPPING_RTL_LCL * EXCRT.EXCH_RATE
        END                                                             AS F_CO_SHIPPING_RTL
    ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
            THEN SRC.F_CO_SHIPPING_CST_LCL
            ELSE SRC.F_CO_SHIPPING_CST_LCL * EXCRT.EXCH_RATE
        END                                                             AS F_CO_SHIPPING_CST
    ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
            THEN SRC.F_CO_HDR_ADDTNL_CHRGS_LCL
            ELSE SRC.F_CO_HDR_ADDTNL_CHRGS_LCL * EXCRT.EXCH_RATE
        END                                                             AS F_CO_HDR_ADDTNL_CHRGS
    ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
            THEN SRC.F_CO_LN_ADDTNL_CHRGS_LCL
            ELSE SRC.F_CO_LN_ADDTNL_CHRGS_LCL * EXCRT.EXCH_RATE
        END                                                             AS F_CO_LN_ADDTNL_CHRGS
    ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
            THEN SRC.F_CO_ORD_TOT_AMT_LCL
            ELSE SRC.F_CO_ORD_TOT_AMT_LCL * EXCRT.EXCH_RATE
        END                                                             AS F_CO_ORD_TOT_AMT
FROM {{ ref('V_STG_F_ECOMM_CO_HDR_B') }} SRC
LEFT JOIN {{ ref('V_DWH_D_ORG_LOC_LU') }} LOC ON LOC.LOC_ID = SRC.DMND_LOC_ID
LEFT JOIN DW_DWH_V.V_DWH_D_TIM_MIN_OF_DAY_LU _MIN
ON _MIN.MIN_24HR_ID =  (CONCAT (
                        TRIM(TO_CHAR(HOUR(SRC.CO_ORD_TS), '00'))
                        ,TRIM(TO_CHAR(MINUTE(SRC.CO_ORD_TS), '00'))
                        ))
LEFT JOIN DW_DWH_V.V_DWH_F_EXCH_RATE_LU EXCRT
    ON (EXCRT.FROM_CNCY_CDE = SRC.LCL_CNCY_CDE AND EXCRT.TO_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
    AND TO_DATE(SRC.CO_ORD_TS) BETWEEN EXCRT.EFF_FROM_DT AND EXCRT.EFF_TO_DT)
-- ordering by CO_ORD_TS, LOC_KEY for performance through Snowflake partitioning
ORDER BY SRC.CO_ORD_TS, LOC.LOC_KEY
