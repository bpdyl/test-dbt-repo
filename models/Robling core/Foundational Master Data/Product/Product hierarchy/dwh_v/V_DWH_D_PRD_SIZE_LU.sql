{{ config(
    materialized='view',
    alias='V_DWH_D_PRD_SIZE_LU',
    schema='DW_DWH_V',
    tags=['d_prd_size_ld']
) }}
SELECT
  SRC.*
FROM {{ ref('DWH_D_PRD_SIZE_LU') }} SRC
