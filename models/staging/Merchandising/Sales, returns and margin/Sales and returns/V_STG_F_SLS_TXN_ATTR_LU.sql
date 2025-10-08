{# Note: following macro is called to switch between source 
and source_chg and it is applicable for robling product only.
It ensures that first load in daily batch is done using _LND schema 
and second load in daily batch is done using _LND_CHG schema#}
{% set curr_day = robling_product.get_business_date() | string | trim %}
{% set src_name = select_stg_source(
    base_source_name = 'SALES_AND_RETURNS_SRC',
    curr_day = curr_day,
    switch_date = '2023-12-28'
) %}

{{ config(
    materialized='view',
    alias='V_STG_F_SLS_TXN_ATTR_LU',
    schema='DW_STG_V',
    unique_key = ['TXN_ID'],
    tags=['f_sls_txn_attr_ld']
) }}

SELECT
     SLS_ATTR.TXN_ID                                         AS TXN_ID
    ,TO_DATE(SELECT PARAM_VALUE FROM DW_DWH.DWH_C_PARAM 
            WHERE PARAM_NAME = 'CURR_DAY')                   AS POST_DT
    ,SLS_ATTR.TXN_CREATE_TS                                  AS TXN_TS
    ,SLS_ATTR.LOC_ID                                         AS LOC_ID
    ,SLS_ATTR.REGISTER_ID                                    AS REGISTER_ID
    ,SLS_ATTR.POS_TXN_ID                                     AS POS_TXN_NUM
    ,SLS_ATTR.TXN_TYP                                        AS TXN_TYP
    ,SLS_ATTR.EMP_ID                                         AS SOLD_TO_EMP_ID
    ,SLS_ATTR.POS_CASHIER_ID                                 AS POS_CASHIER_ID
    ,SLS_ATTR.POS_SALESPERSON_ID                             AS POS_SALESPERSON_ID
    ,SLS_ATTR.DO_ID                                          AS DO_ID
    ,SLS_ATTR.LCL_CNCY_CDE                                   AS LCL_CNCY_CDE
    --ADDING CUSTOM FLEX FIELDS TO TEST IF DBT AUTOMATICALLY PROPAGATE IT TO DWH 
    ,1                                                       AS FLEX_FIELD_1
    ,'2'                                                     AS FLEX_FIELD_2
FROM {{source (src_name,'LND_F_SLS_TXN_ATTR_LU')}} SLS_ATTR
WHERE SLS_ATTR.TXN_DT_KEY BETWEEN '2022-01-30' AND  '{{ curr_day }}'
