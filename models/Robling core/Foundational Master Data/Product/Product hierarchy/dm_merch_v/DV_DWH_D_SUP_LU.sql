{{ config(
    materialized='view',
    alias='DV_DWH_D_SUP_LU',
    schema='DM_MERCH_V',
    tags=['d_sup_ld']
) }}
SELECT 
  SRC.*
FROM {{ ref('DWH_D_SUP_LU') }} SRC
