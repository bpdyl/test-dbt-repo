{{ config(
    materialized='view',
    alias='V_DWH_F_SLS_TXN_ATTR_LU',
    schema='DW_DWH_V',
    tags=['f_sls_txn_attr_ld']
) }}
SELECT
  SRC.*
FROM {{ ref('DWH_F_SLS_TXN_ATTR_LU') }} SRC
