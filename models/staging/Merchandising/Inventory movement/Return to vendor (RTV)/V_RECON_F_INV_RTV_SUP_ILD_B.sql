{# Note: following macro is called to switch between source 
and source_chg and it is applicable for robling product only.
It ensures that first load in daily batch is done using _LND schema 
and second load in daily batch is done using _LND_CHG schema#}
{% set curr_day = robling_product.get_business_date() | string | trim %}
{% set src_name = select_stg_source(
    base_source_name = 'INVENTORY_MOVEMENT_RTV_SRC',
    curr_day = curr_day,
    switch_date = '2023-12-28'
) %}

{{ config(
    materialized='view',
    alias='V_RECON_F_INV_RTV_SUP_ILD_B',
    schema='DW_STG_V',
    tags=['f_inv_rtv_sup_ild_ld']
) }}

SELECT
    RTV.POST_DT                                                       AS POST_DT
    ,RTV.TXN_DT                                                       AS TXN_DT
    ,RTV.LOC_ID                                                       AS LOC_ID
    ,RTV.F_RTV_QTY                                                    AS F_RTV_QTY
    ,RTV.F_RTV_CST_LCL                                                AS F_RTV_CST_LCL
    ,RTV.F_RTV_RTL_LCL                                                AS F_RTV_RTL_LCL
    ,RTV.LCL_CNCY_CDE                                                 AS LCL_CNCY_CDE
FROM {{ source('INVENTORY_MOVEMENT_RTV_SRC','LND_F_INV_RTV_SUP_ILD_B') }} RTV
WHERE RTV.TXN_DT BETWEEN '2022-01-30' AND  '{{ curr_day }}'
