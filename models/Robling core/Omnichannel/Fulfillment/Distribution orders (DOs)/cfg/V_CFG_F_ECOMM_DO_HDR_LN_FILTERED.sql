{#- This model encapsulates the filtering logic for matching the CO and DO lines
to be used in downstream fact tables. This makes the logic reusable and
easily maintainable. If the filtering conditions change, we only need to
update this one model. -#}
{{ config(
    materialized='view',
    alias='V_CFG_F_ECOMM_DO_HDR_LN_FILTERED',
    schema='DW_CFG',
    tags=['f_ecomm_do_ln_itm_ld']
) }}
SELECT
    DO_HDR.FULFILL_TYP,
    DO_HDR.DO_BOPIS_FLG,
    DO_HDR.DO_HDR_STTS,
    DO_HDR.DO_NUM,
    DO_HDR.DO_INVOICE_DT,
    DO_LN_TMP.*
FROM {{ ref('V_DWH_F_ECOMM_DO_LN_ITM_B') }} DO_LN_TMP
INNER JOIN {{ ref('V_DWH_F_ECOMM_DO_HDR_B') }} DO_HDR
    ON DO_LN_TMP.DO_ID = DO_HDR.DO_ID
WHERE
    -- Only DO's with actual quantities
    (DO_LN_TMP.F_DO_CURR_QTY <> 0)
    -- exclude deleted records
    AND DO_LN_TMP.IS_DELETED = '0'
    -- Exclude rejected DO's
    AND DO_HDR.DO_HDR_STTS NOT IN ('Cancelled', 'Rejected')
    AND DO_HDR.IS_DELETED = '0'
