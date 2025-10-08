{{ config(
    materialized='table',
    alias='TMP_F_INV_TXN_ILD_B',
    schema='DW_TMP',
    on_schema_change = 'append_new_columns',
    tags = ['f_inv_txn_ild_ld'],
    pre_hook=["{{ start_script('f_inv_txn_ild_ld','RUNNING','NONE') }}"
             ,"{{ load_recon_data('Inventory Transactions', recon_config_macro='mac_f_inv_txn_recon_script_sql', recon_step=0) }}"],
    post_hook=["{{ log_dml_audit(this,ref('V_STG_F_INV_TXN_ILD_B'),'CREATE_TABLE_AS_SELECT') }}"]
) }}
SELECT
 SRC.*
,TO_DATE(SRC.TXN_TS)                                                               AS TXN_DT
,_MIN.MIN_KEY                                                                      AS TXN_MIN_KEY
,{{ get_coalesced_surrogate_key('LOC.LOC_KEY','SRC.LOC_ID') }}                     AS LOC_KEY
,{{ get_coalesced_surrogate_key('LOC_2.LOC_KEY','SRC.LOC_2_ID') }}                 AS LOC_2_KEY
,{{ get_coalesced_surrogate_key('ITM.ITM_KEY','SRC.ITM_ID') }}                     AS ITM_KEY
,{{ get_coalesced_surrogate_key('SUP.SUP_KEY','SRC.SUP_ID') }}                     AS SUP_KEY                
,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
    THEN SRC.F_INV_TXN_CST_LCL
    ELSE SRC.F_INV_TXN_CST_LCL * EXCH1.EXCH_RATE
 END                                                                               AS F_INV_TXN_CST
,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
    THEN SRC.F_INV_TXN_RTL_LCL
    ELSE SRC.F_INV_TXN_RTL_LCL * EXCH1.EXCH_RATE
 END                                                                               AS F_INV_TXN_RTL
FROM {{ ref("V_STG_F_INV_TXN_ILD_B") }} SRC
LEFT OUTER JOIN {{ ref("V_DWH_D_ORG_LOC_LU")}} LOC ON SRC.LOC_ID = LOC.LOC_ID
LEFT OUTER JOIN {{ ref("V_DWH_D_ORG_LOC_LU")}} LOC_2 ON SRC.LOC_2_ID = LOC_2.LOC_ID
LEFT OUTER JOIN {{ ref("V_DWH_D_PRD_ITM_LU")}} ITM ON SRC.ITM_ID = ITM.ITM_ID
LEFT OUTER JOIN {{ ref("V_DWH_D_SUP_LU") }} SUP ON SRC.SUP_ID = SUP.SUP_ID
LEFT OUTER JOIN {{ source("MIN_OF_DAY_LU","V_DWH_D_TIM_MIN_OF_DAY_LU") }} _MIN
    ON _MIN.MIN_24HR_ID = TRIM(CONCAT(
                            LPAD(HOUR(SRC.TXN_TS), 2, '0'),
                            LPAD(MINUTE(SRC.TXN_TS), 2, '0')
                          ))
LEFT OUTER JOIN {{ source("EXCHG_RATE_LU","V_DWH_F_EXCH_RATE_LU") }} EXCH1
   ON EXCH1.FROM_CNCY_CDE = SRC.LCL_CNCY_CDE AND EXCH1.TO_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
       AND (TO_DATE(SRC.TXN_TS) BETWEEN EXCH1.EFF_FROM_DT AND EXCH1.EFF_TO_DT)
