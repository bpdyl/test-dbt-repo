{{ config(
    materialized='view',
    alias='V_DWH_D_ORG_RGN_LU',
    schema='DW_DWH_V',
    tags=['d_org_rgn_ld']
) }}

SELECT
    SRC.*
FROM {{ ref('DWH_D_ORG_RGN_LU') }} SRC