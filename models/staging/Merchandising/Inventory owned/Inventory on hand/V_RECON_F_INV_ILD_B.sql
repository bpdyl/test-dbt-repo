{{ config(
    materialized='view',
    alias='V_RECON_F_INV_ILD_B',
    schema='DW_STG_V',
    tags=['f_inv_ild_ld']
) }}
SELECT
     TRIM(IL.ITM_ID)             AS ITM_ID
    ,TRIM(IL.LOC_ID)             AS LOC_ID
    ,IL.DAY_ID                   AS EFF_START_DT
    ,IL.ITMLOC_STTS_CDE          AS ITMLOC_STTS_CDE
    ,IL.F_OH_QTY                 AS F_OH_QTY
    ,IL.F_OH_CST_LCL             AS F_OH_CST_LCL
    ,IL.F_OH_RTL_LCL             AS F_OH_RTL_LCL
    ,IL.F_IT_QTY                 AS F_IT_QTY
    ,IL.F_IT_CST_LCL             AS F_IT_CST_LCL
    ,IL.F_IT_RTL_LCL             AS F_IT_RTL_LCL
    ,IL.F_UNIT_WAC_CST_LCL       AS F_UNIT_WAC_CST_LCL
    ,IL.F_UNIT_RTL_LCL           AS F_UNIT_RTL_LCL
    ,IL.F_REG_UNIT_RTL_LCL       AS F_REG_UNIT_RTL_LCL
    ,IL.F_PROMO_RTL_LCL          AS F_PROMO_RTL_LCL
    ,TRIM(IL.LCL_CNCY_CDE)       AS LCL_CNCY_CDE
FROM {{ source('INVENTORY_OH_SRC','LND_F_INV_IL_B') }} IL 
GROUP BY ALL