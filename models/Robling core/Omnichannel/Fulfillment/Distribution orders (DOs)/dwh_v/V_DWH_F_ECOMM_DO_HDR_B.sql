{{ config(
    materialized='view',
    alias='V_DWH_F_ECOMM_DO_HDR_B',
    schema='DW_DWH_V',
    tags=['f_ecomm_do_hdr_ld']
) }}
SELECT
  SRC.*
FROM {{ ref('DWH_F_ECOMM_DO_HDR_B') }} SRC
