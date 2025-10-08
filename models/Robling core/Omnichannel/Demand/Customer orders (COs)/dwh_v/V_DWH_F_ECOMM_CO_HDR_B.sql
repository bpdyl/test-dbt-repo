{{ config(
    materialized='view',
    alias='V_DWH_F_ECOMM_CO_HDR_B',
    schema='DW_DWH_V',
    tags=['f_ecomm_co_hdr_ld']
) }}
SELECT
  SRC.*
FROM {{ ref('DWH_F_ECOMM_CO_HDR_B') }} SRC
