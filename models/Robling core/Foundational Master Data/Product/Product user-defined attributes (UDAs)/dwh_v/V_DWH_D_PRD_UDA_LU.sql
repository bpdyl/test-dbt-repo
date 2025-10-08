{{ config(
    materialized='view',
    alias='V_DWH_D_PRD_UDA_LU',
    schema='DW_DWH_V',
    tags=['d_prd_uda_ld'],
) }}
SELECT
  SRC.*
FROM {{ref('DWH_D_PRD_UDA_LU')}} SRC