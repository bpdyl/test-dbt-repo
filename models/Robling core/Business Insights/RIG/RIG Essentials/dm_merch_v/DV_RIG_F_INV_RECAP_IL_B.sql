{{ config(
    materialized='view',
    alias='DV_RIG_F_INV_RECAP_IL_B',
    schema='DM_MERCH_V',
    tags=['f_rig_inv_recap_il_b']
) }}
SELECT 
  SRC.*
FROM {{ ref('RIG_F_INV_RECAP_IL_B') }} SRC
