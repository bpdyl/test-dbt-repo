{{ config(
    materialized='view',
    alias='V_DWH_D_ORG_ARA_LU',
    schema='DW_DWH_V',
    tags=['d_org_ara_ld']
) }}

SELECT
    SRC.*
FROM {{ ref('DWH_D_ORG_ARA_LU') }} SRC