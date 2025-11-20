{{ config(
    materialized='view',
    alias='V_RIG_F_INV_IMMNT_STKOUT_ILD_B',
    schema='DW_RIG_V',
    tags=['f_rig_inv_immnt_stkout_ild_b']
) }}
SELECT 
  SRC.*
FROM {{ ref('RIG_F_INV_IMMNT_STKOUT_ILD_B') }} SRC
