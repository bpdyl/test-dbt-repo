{{ config(
    materialized='view',
    alias='DV_DWH_F_SLS_TXN_ATTR_LU',
    schema='DM_MERCH_V',
    tags=['f_sls_txn_attr_ld']
) }}
SELECT
     TXN_ID                                                     AS TXN_ID
    ,TXN_DT                                                     AS TXN_DT
	,REGISTER_ID                                                AS REGISTER_ID
	,POS_TXN_NUM                                                AS POS_TXN_NUM
	,CONCAT(LOC_ID, '-', REGISTER_ID, '-', POS_TXN_NUM)         AS FULL_TXN_NUM
FROM {{ ref('DWH_F_SLS_TXN_ATTR_LU') }}