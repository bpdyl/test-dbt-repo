{{ config(
    materialized='view',
    alias='DV_DWH_F_INTEGRITY_CHK',
    schema='DM_MERCH_V',
) }}
SELECT 
  SRC.*
FROM {{ ref('DWH_F_INTEGRITY_CHK') }} SRC
