{{ config(
    materialized='view',
    alias='DV_DWH_F_ECOMM_DO_HDR_B',
    schema='DM_MERCH_V',
    tags=['f_ecomm_do_hdr_ld']
) }}
SELECT
    SRC.*
FROM {{ ref('DWH_F_ECOMM_DO_HDR_B') }} SRC