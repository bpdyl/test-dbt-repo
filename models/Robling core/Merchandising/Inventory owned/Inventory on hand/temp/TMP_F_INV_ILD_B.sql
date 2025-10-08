{{ config(
    materialized='table',
    alias='TMP_F_INV_ILD_B',
    schema='DW_TMP',
    on_schema_change = 'append_new_columns',
    tags = ['f_inv_ild_ld'],
    pre_hook=["{{ start_script('f_inv_ild_ld','RUNNING','NONE') }}"
             ,"{{ load_recon_data('Inventory On-Hand',recon_config_macro='mac_f_inv_recon_script_sql', recon_step=0) }}"],
    post_hook=["{{ log_dml_audit(this,ref('V_STG_F_INV_ILD_B'),'CREATE_TABLE_AS_SELECT') }}"]
) }}
SELECT
    SRC.*
    ,{{ get_coalesced_surrogate_key('ITM.ITM_KEY','SRC.ITM_ID') }}                     AS ITM_KEY
    ,{{ get_coalesced_surrogate_key('LOC.LOC_KEY','SRC.LOC_ID') }}                     AS LOC_KEY
    ,LOC.LOC_TYP_CDE                                                                AS LOC_TYP_CDE
    ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
        THEN SRC.F_OH_CST_LCL
        ELSE SRC.F_OH_CST_LCL * EXCRT.EXCH_RATE
    END                                                                             AS F_OH_CST
    ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
        THEN SRC.F_OH_RTL_LCL
        ELSE SRC.F_OH_RTL_LCL * EXCRT.EXCH_RATE
    END                                                                             AS F_OH_RTL
    ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
        THEN SRC.F_IT_CST_LCL
        ELSE SRC.F_IT_CST_LCL * EXCRT.EXCH_RATE
    END                                                                             AS F_IT_CST
    ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
        THEN SRC.F_IT_RTL_LCL
        ELSE SRC.F_IT_RTL_LCL * EXCRT.EXCH_RATE
    END                                                                             AS F_IT_RTL
    ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
        THEN SRC.F_UNIT_WAC_CST_LCL
        ELSE SRC.F_UNIT_WAC_CST_LCL * EXCRT.EXCH_RATE
    END                                                                             AS F_UNIT_WAC_CST
    ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
        THEN SRC.F_UNIT_RTL_LCL
        ELSE SRC.F_UNIT_RTL_LCL * EXCRT.EXCH_RATE
    END                                                                             AS F_UNIT_RTL
    ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
        THEN SRC.F_REG_UNIT_RTL_LCL
        ELSE SRC.F_REG_UNIT_RTL_LCL * EXCRT.EXCH_RATE
    END                                                                             AS F_REG_UNIT_RTL
    ,CASE WHEN SRC.LCL_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
        THEN SRC.F_PROMO_RTL_LCL
        ELSE SRC.F_PROMO_RTL_LCL * EXCRT.EXCH_RATE
    END                                                                             AS F_PROMO_RTL
    FROM {{ ref('V_STG_F_INV_ILD_B') }} SRC
    LEFT JOIN {{ ref('V_DWH_D_ORG_LOC_LU') }} LOC
        ON SRC.LOC_ID = LOC.LOC_ID
    LEFT JOIN {{ ref('V_DWH_D_PRD_ITM_LU') }} ITM
        ON SRC.ITM_ID = ITM.ITM_ID
    LEFT JOIN DW_DWH_V.V_DWH_F_EXCH_RATE_LU EXCRT
    ON (EXCRT.FROM_CNCY_CDE = SRC.LCL_CNCY_CDE AND EXCRT.TO_CNCY_CDE = '{{ var("PRIMARY_CNCY_CDE") }}'
            AND SRC.EFF_START_DT BETWEEN EXCRT.EFF_FROM_DT AND EXCRT.EFF_TO_DT)
    -- ordering by EFF_START_DT, LOC_KEY for performance through Snowflake partitioning
    ORDER BY SRC.EFF_START_DT
            ,LOC.LOC_KEY
