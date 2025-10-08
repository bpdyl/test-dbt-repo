{#-
This model classifies each Customer Order Line (CO_LN) into one of three 
categories — "Perfectly Splitted", "Partially Fulfilled", or "Mismatched".  

It does so by comparing allocated and fulfilled quantities from the CO lines 
against the aggregated values from the corresponding Delivery Order (DO) lines.  

Purpose:
- Provides a standardized way to evaluate order splits between demand and shipment.  
- Makes the categorization reusable across data marts and validation queries.  
- Keeps configuration logic centralized in a dedicated view, so any future changes 
  (e.g., additional statuses or new business rules) only need to be applied here.  

This ensures consistency, easier maintenance, and cleaner downstream queries.  
-#}
{{ config(
    materialized='view',
    alias='V_CFG_F_ECOMM_SPLIT_SHPMNT_TYP',
    schema='DW_CFG',
    tags=['f_ecomm_do_ln_itm_ld']
) }}
WITH DO_LN_AGG AS (
    SELECT 
        CO_LN_ID
        ,SUM(F_DO_CURR_QTY) AS TOTAL_DO_ALLOCATED_QTY
        ,SUM(F_FULFILL_QTY) AS TOTAL_DO_FULFILLED_QTY
    FROM {{ ref('V_CFG_F_ECOMM_DO_HDR_LN_FILTERED') }}
    GROUP BY CO_LN_ID
)
SELECT
CO_LN.CO_LN_ID
-- Case statement to categorize each CO line
,CASE
    WHEN CO_LN_ITM_STTS = 'Shipped' AND COALESCE(CO_LN.f_co_allocated_qty,0) = COALESCE(DLA.TOTAL_DO_ALLOCATED_QTY,0) 
    AND COALESCE(CO_LN.F_CO_FULFILL_QTY,0) = COALESCE(DLA.TOTAL_DO_FULFILLED_QTY,0) THEN 'Perfectly Splitted'
    WHEN 
    CO_LN_ITM_STTS = 'Partially Shipped' THEN 'Partially Fulfilled'
    ELSE 'Mismatched'
END AS SPLIT_SHPMNT_TYPE
FROM {{ ref('V_DWH_F_ECOMM_CO_LN_ITM_B') }} CO_LN
LEFT JOIN DO_LN_AGG DLA ON CO_LN.CO_LN_ID = DLA.CO_LN_ID
WHERE CO_LN.IS_DELETED = '0'
