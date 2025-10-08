{{ config(
    materialized='view',
    alias='DV_DWH_D_ORG_CHN_LU',
    schema='DM_MERCH_V',
    tags=['d_org_chn_ld']
) }}

SELECT
    SRC.*
FROM {{ ref('DWH_D_ORG_CHN_LU') }} SRC 