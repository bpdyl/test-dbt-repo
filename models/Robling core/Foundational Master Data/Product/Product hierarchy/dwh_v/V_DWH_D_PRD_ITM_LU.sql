{{ config(
    materialized='view',
    alias='V_DWH_D_PRD_ITM_LU',
    schema='DW_DWH_V',
    tags=['d_prd_itm_ld']
) }}
SELECT
  SRC.*
FROM {{ ref('DWH_D_PRD_ITM_LU') }} SRC
