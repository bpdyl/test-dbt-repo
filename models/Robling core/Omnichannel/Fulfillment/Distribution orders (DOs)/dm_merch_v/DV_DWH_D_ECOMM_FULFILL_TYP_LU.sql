{{ config(
    materialized='view',
    alias='DV_DWH_D_ECOMM_FULFILL_TYP_LU',
    schema='DM_MERCH_V',
    tags=['d_ecomm_fulfill_typ_ld']
) }}
SELECT
    SRC.*
FROM {{ ref('DWH_D_ECOMM_FULFILL_TYP_LU') }} SRC