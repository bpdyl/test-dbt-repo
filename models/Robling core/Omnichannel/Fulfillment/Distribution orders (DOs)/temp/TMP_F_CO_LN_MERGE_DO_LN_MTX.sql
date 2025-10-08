{{ config(
    materialized='view',
    alias='TMP_F_CO_LN_MERGE_DO_LN_MTX',
    schema='DW_TMP',
    tags=['f_ecomm_do_ln_itm_ld']
) }}

WITH DO_LN AS (
    SELECT 
    DO_LN.CO_LN_ID                          AS CO_LN_ID
    ,COUNT(*)                               AS DO_LINE_COUNT
    ,SUM(coalesce(DO_LN.F_DO_CURR_QTY,0))   AS F_DO_ALLOCATED_QTY
    ,SUM(coalesce(DO_LN.F_FULFILL_QTY,0) )  AS F_DO_FULFILL_QTY
    FROM {{ ref('DWH_F_ECOMM_DO_LN_ITM_B') }} DO_LN 
    JOIN {{ ref('DWH_F_ECOMM_DO_HDR_B') }} DO_HDR 
    ON DO_LN.DO_ID = DO_HDR.DO_ID 
    WHERE 
        -- exclude cancelled do's 
        DO_HDR.DO_HDR_STTS IN (
            SELECT DISTINCT DO_HDR_STTS FROM {{ ref('V_CFG_F_ECOMM_DO_HDR_LN_FILTERED') }}
        )
        -- Exclude deleted records 
        AND DO_LN.IS_DELETED = '0' 
        AND DO_HDR.IS_DELETED = '0' 
GROUP BY DO_LN.CO_LN_ID )

SELECT 
    CO_LN.CO_ID                 AS CO_ID
    ,CO_LN.CO_LN_ID             AS CO_LN_ID
    ,CO_LN.CO_ORD_DT            AS CO_ORD_DT
    ,CO_LN.F_CO_ORD_QTY         AS F_CO_ORD_QTY
    ,CO_LN.F_CO_ALLOCATED_QTY   AS F_CO_ALLOCATED_QTY
    ,DO_LN.F_DO_ALLOCATED_QTY   AS F_DO_ALLOCATED_QTY
    ,CO_LN.F_CO_FULFILL_QTY     AS F_CO_FULFILL_QTY
    ,DO_LN.F_DO_FULFILL_QTY     AS F_DO_FULFILL_QTY
    ,DO_LN.DO_LINE_COUNT        AS DO_LINE_COUNT
FROM (SELECT * FROM {{ ref('DWH_F_ECOMM_CO_LN_ITM_B') }} WHERE IS_DELETED = '0') CO_LN
LEFT OUTER JOIN DO_LN ON CO_LN.CO_LN_ID = DO_LN.CO_LN_ID
WHERE 
 -- perfect split co lines.
(   NVL(F_CO_ALLOCATED_QTY,0) = NVL(F_DO_ALLOCATED_QTY,0)  
    -- do_quantityallocated <= co_quantityallocated
 AND NVL(F_CO_FULFILL_QTY,0) = NVL(F_DO_FULFILL_QTY,0)  
 AND  F_DO_ALLOCATED_QTY  <> 0
)