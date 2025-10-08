{{ config(
    materialized='view',
    alias='V_DWH_D_PRD_CLS_LU',
    schema='DW_DWH_V',
    tags=['d_prd_cls_ld']
) }}
SELECT 
  SRC.*
FROM {{ ref('DWH_D_PRD_CLS_LU') }} SRC
