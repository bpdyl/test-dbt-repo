{{ config(
    materialized='view',
    alias='V_DWH_F_INV_IL_B',
    schema='DW_DWH_V',
    tags = ['f_inv_ild_ld'],
) }}
SELECT
    SRC.* 
FROM {{ ref('DWH_F_INV_IL_B') }} SRC

