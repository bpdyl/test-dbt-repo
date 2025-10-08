{% macro update_current_flg() %}
    {# 
        OVERVIEW:
        This macro updates the IS_CURRENT flag to 0 for all records in the target table for which a more recent version of the same line item exists.
        
        INPUTS:
        None
        
        OUTPUTS:
        - None
    #}
    {% set update_sql %}
        -- Updating to not current (current_flg = 0) all records for which a more recent version of the same line item exists
        UPDATE DW_DWH.DWH_F_SLS_TXN_LN_ITM_B TGT
        SET TGT.IS_CURRENT = 0
        FROM {{ source('SLS_UPD_SRC_TMP','TMP_F_SLS_TXN_LN_ITM_B') }} SRC
        WHERE TGT.TXN_ID = SRC.TXN_ID
        AND TGT.TXN_LN_ID = SRC.TXN_LN_ID
        AND TGT.VERSION_ID <>
            (SELECT MAX(TO_NUMBER(VERSION_ID))
            FROM {{ source('SLS_UPD_SRC_DWH','V_DWH_F_SLS_TXN_LN_ITM_B') }}
            WHERE TXN_ID = TGT.TXN_ID
            AND TXN_LN_ID = TGT.TXN_LN_ID
            )
        AND TGT.IS_CURRENT <> 0 -- Avoids updating records that are already flagged as non-current
    {% endset %}
    {% do run_query(update_sql) %}
{% endmacro %}