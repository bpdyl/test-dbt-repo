{# Note: following macro is called to switch between source 
and source_chg and it is applicable for robling product only.
It ensures that first load in daily batch is done using _LND schema 
and second load in daily batch is done using _LND_CHG schema#}
{% set curr_day = robling_product.get_business_date() | string | trim %}
{% set src_name = select_stg_source(
    base_source_name = 'INVENTORY_TXN_SRC',
    curr_day = curr_day,
    switch_date = '2023-12-28'
) %}

{{ config(
    materialized='view',
    alias='V_STG_F_INV_TXN_ILD_B',
    schema='DW_STG_V',
    unique_key = ['ROW_ID'],
    tags=['f_inv_txn_ild_ld']
) }}
SELECT
    TXN.ROW_ID                                           AS ROW_ID
    ,TXN.POST_DT                                         AS POST_DT
    ,TXN.TXN_TS                                          AS TXN_TS
    ,TXN.LOC_ID                                          AS LOC_ID
    ,TXN.LOC_2_ID                                        AS LOC_2_ID
    ,TXN.ITM_ID                                          AS ITM_ID
    ,NULL                                                AS SUP_ID
    ,TXN.INV_TXN_TYP                                     AS INV_TXN_TYP
    ,TXN.INV_TXN_DOC_ID                                  AS INV_TXN_DOC_ID
    ,TXN.ITMLOC_STTS_CDE                                 AS ITMLOC_STTS_CDE
    ,TXN.F_INV_TXN_QTY                                   AS F_INV_TXN_QTY
    ,TXN.F_INV_TXN_CST_LCL                               AS F_INV_TXN_CST_LCL
    ,TXN.F_INV_TXN_RTL_LCL                               AS F_INV_TXN_RTL_LCL
    ,TXN.LCL_CNCY_CDE                                    AS LCL_CNCY_CDE
FROM {{ source(src_name,'LND_F_INV_TXN_ILD_B') }} TXN
WHERE TO_DATE(TXN.TXN_TS) BETWEEN '2022-01-30' AND '{{ curr_day }}'
GROUP BY ALL
