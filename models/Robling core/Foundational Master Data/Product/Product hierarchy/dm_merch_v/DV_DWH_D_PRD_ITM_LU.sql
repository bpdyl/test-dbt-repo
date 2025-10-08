{{ config(
    materialized='view',
    alias='DV_DWH_D_PRD_ITM_LU',
    schema='DM_MERCH_V',
    tags=['d_prd_itm_ld']
) }}
SELECT
    SRC.*
    ,DATEDIFF('DD', SRC.FIRST_RCVD_DT, CURRENT_DATE)       AS DAYS_SINCE_FIRST_RECEIPT
FROM {{ ref('DWH_D_PRD_ITM_LU') }} SRC