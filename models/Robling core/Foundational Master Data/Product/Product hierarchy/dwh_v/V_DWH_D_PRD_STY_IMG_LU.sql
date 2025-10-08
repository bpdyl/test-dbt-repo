{{ config(
    materialized='view',
    alias='V_DWH_D_PRD_STY_IMG_LU',
    schema='DW_DWH_V',
    tags=['d_prd_sty_img_ld']
) }}
SELECT 
  SRC.*
FROM {{ ref('DWH_D_PRD_STY_IMG_LU') }} SRC
