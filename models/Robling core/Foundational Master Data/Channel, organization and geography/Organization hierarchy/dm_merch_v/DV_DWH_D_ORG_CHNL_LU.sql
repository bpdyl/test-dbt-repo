{{ config(
    materialized='view',
    alias='DV_DWH_D_ORG_CHNL_LU',
    schema='DM_MERCH_V',
    tags=['d_org_chnl_ld']
) }}

SELECT
    SRC.*
FROM {{ ref('DWH_D_ORG_CHNL_LU') }} SRC 