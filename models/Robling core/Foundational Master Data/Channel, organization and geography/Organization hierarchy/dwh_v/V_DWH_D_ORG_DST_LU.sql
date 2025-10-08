{{ config(
    materialized='view',
    alias='V_DWH_D_ORG_DST_LU',
    schema='DW_DWH_V',
    tags=['d_org_dst_ld']
) }}

SELECT
    SRC.*
FROM {{ ref('DWH_D_ORG_DST_LU') }} SRC