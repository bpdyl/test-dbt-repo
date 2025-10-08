{{ config(
    materialized='view',
    alias='V_DWH_D_ORG_CHNL_LU',
    schema='DW_DWH_V',
    tags=['d_org_chnl_ld']
) }}

SELECT
    SRC.*
FROM {{ ref('DWH_D_ORG_CHNL_LU') }} SRC