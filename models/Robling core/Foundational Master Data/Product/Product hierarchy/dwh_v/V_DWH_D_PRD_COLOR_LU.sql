{{ config(
    materialized='view',
    alias='V_DWH_D_PRD_COLOR_LU',
    schema='DW_DWH_V',
    tags=['d_prd_color_ld']
) }}
SELECT
  SRC.*
FROM {{ ref('DWH_D_PRD_COLOR_LU') }} SRC
