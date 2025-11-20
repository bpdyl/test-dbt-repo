{# Note: following macro is called to switch between source
and source_chg and it is applicable for robling product only.
It ensures that first load in daily batch is done using _LND schema
and second load in daily batch is done using _LND_CHG schema#}
{% set curr_day = robling_product.get_business_date() | string | trim %}
{% set src_name = select_stg_source(
    base_source_name = 'FCST_STG_SRC',
    curr_day = curr_day,
    switch_date = '2023-12-28'
) %}

/*
Default Logic Used:
    - Forecast quantity (F_RIG_FCST_QTY) is calculated as the average daily sales units for the last 28 days.
    - Forecast cost (F_RIG_FCST_CST_LCL) is the average daily cost over the last 28 days.
    - Forecast retail (F_RIG_FCST_RTL_LCL) is the average daily retail over the last 28 days.
*/

{% if execute %}
{% set rig_load_type_query = run_query("
    -- To find the load type for RIG from the DWH_C_PARAM table
    SELECT PARAM_VALUE FROM DW_DWH.DWH_C_PARAM WHERE PARAM_NAME = 'RIG_LOAD' ") %}
{% set rig_load_type = rig_load_type_query.columns[0].values()[0] %}
{% endif %}

{{ config(
    materialized='view',
    alias='V_STG_RIG_F_INV_FCST_ILD_B',
    schema='DW_STG_V',
    unique_key = ['DAY_KEY','LOC_ID','ITM_ID'],
    tags=['f_rig_inv_fcst_ild_b']
) }}
-- depends_on: {{ ref('V_CFG_RIG_F_INV_FCST_ILD_B') }}
/* This part loads the forecast data for every day until the current day */
SELECT * FROM 
    (SELECT
     SRC.DAY_KEY                                      AS DAY_KEY
    ,SRC.ITM_ID                                       AS ITM_ID
    ,SRC.LOC_ID                                       AS LOC_ID
    ,SRC.F_RIG_FCST_QTY                               AS F_RIG_FCST_QTY
    ,SRC.F_RIG_FCST_CST_LCL                           AS F_RIG_FCST_CST_LCL
    ,SRC.F_RIG_FCST_RTL_LCL                           AS F_RIG_FCST_RTL_LCL
    ,SRC.LCL_CNCY_CDE                                 AS LCL_CNCY_CDE    
FROM {{ ref('V_CFG_RIG_F_INV_FCST_ILD_B') }} SRC
UNION ALL 
-- This part projects historical day 42 days (6 weeks) into the future 6 weeks
SELECT
     SRC.DAY_KEY + 42                                 AS DAY_KEY
    ,SRC.ITM_ID                                       AS ITM_ID
    ,SRC.LOC_ID                                       AS LOC_ID
    ,SRC.F_RIG_FCST_QTY                               AS F_RIG_FCST_QTY
    ,SRC.F_RIG_FCST_CST_LCL                           AS F_RIG_FCST_CST_LCL
    ,SRC.F_RIG_FCST_RTL_LCL                           AS F_RIG_FCST_RTL_LCL
    ,SRC.LCL_CNCY_CDE                                 AS LCL_CNCY_CDE    
FROM {{ ref('V_CFG_RIG_F_INV_FCST_ILD_B') }} SRC
WHERE SRC.DAY_KEY -42 > TO_DATE('{{ robling_product.get_business_date() }}')            -- Only consider historical days that are to be projected 6 weeks into the future
)

{# 
-- FOR DAILY PROCESSING: Set the value of RIG_LOAD param to 'DAILY' 
-- FOR HISTORICAL LOADS: Set the value of RIG_LOAD param to 'FULL'
#}
{% if rig_load_type|string == 'DAILY' %}
WHERE DAY_KEY =  TO_DATE('{{ robling_product.get_business_date() }}')
{% endif %}
