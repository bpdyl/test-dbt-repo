{{ config(
    materialized='table',
    alias='TMP_F_SLS_TXN_ATTR_LU',
    schema='DW_TMP',
    tags=['f_sls_txn_attr_ld'],
    pre_hook=["{{ start_script('f_sls_txn_attr_ld','RUNNING','NONE') }}"],
    post_hook=["{{ log_dml_audit(this,ref('V_STG_F_SLS_TXN_ATTR_LU'),'CREATE_TABLE_AS_SELECT') }}"]
) }}
SELECT
    SRC.*
    ,TO_DATE(SRC.TXN_TS)                                            AS TXN_DT
    ,_MIN.MIN_KEY                                                   AS TXN_MIN_KEY
    ,{{ get_coalesced_surrogate_key('LOC.LOC_KEY','SRC.LOC_ID') }}  AS LOC_KEY
FROM {{ ref('V_STG_F_SLS_TXN_ATTR_LU') }} src
LEFT JOIN DW_DWH_V.V_DWH_D_TIM_MIN_OF_DAY_LU _MIN
ON _MIN.MIN_24HR_ID = TRIM(CONCAT(
                        LPAD(HOUR(SRC.TXN_TS), 2, '0'),
                        LPAD(MINUTE(SRC.TXN_TS), 2, '0')
                        ))
LEFT JOIN {{ ref('V_DWH_D_ORG_LOC_LU') }} LOC
ON LOC.LOC_ID = src.LOC_ID
-- ordering by TXN_DT, LOC_KEY for performance through Snowflake partitioning
ORDER BY TXN_DT, LOC.LOC_KEY
