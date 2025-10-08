{{
    config(
        unique_key='PARAM_NAME',
        materialized='incremental',
        schema='DW_DWH'
    )
}}
SELECT PARAM_NAME
       ,TO_DATE(PARAM_VALUE) + 1 PARAM_VALUE
FROM DW_DWH.DWH_C_PARAM