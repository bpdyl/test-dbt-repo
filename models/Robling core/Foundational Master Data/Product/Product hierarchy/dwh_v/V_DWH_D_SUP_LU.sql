{{ config(
    materialized='view',
    alias='V_DWH_D_SUP_LU',
    schema='DW_DWH_V',
    tags=['d_sup_ld'],
) }}
SELECT 
  SRC.*
FROM {{ref('DWH_D_SUP_LU')}} SRC
