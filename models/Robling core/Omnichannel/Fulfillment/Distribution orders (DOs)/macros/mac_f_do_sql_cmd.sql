{% macro update_do_cst_from_inv() %}
    {# 
        OVERVIEW:
        This macro updates CST columns and ITMLOC_STTS_CDE column from Inventory table to the Temp table of fulfillment.
        
        INPUTS:
        None
        
        OUTPUTS:
        - None
    #}
    {% set update_sql %}
        -- Updating CST columns and ITMLOC_STTS_CDE column from Inventory table
        UPDATE DW_TMP.TMP_F_ECOMM_DO_LN_ITM_B SRC
        SET F_DO_UNIT_CST_LCL   = INV.F_UNIT_WAC_CST_LCL
            ,F_DO_UNIT_CST      = INV.F_UNIT_WAC_CST
            ,ITMLOC_STTS_CDE    = INV.ITMLOC_STTS_CDE
        FROM {{ source('DO_TMP_UPD_SRC','V_DWH_F_INV_ILD_B') }} INV
        WHERE SRC.ITM_ID =INV.ITM_ID
        AND SRC.FULMNT_LOC_ID = INV.LOC_ID
        AND SRC.DO_CREATED_DT BETWEEN INV.EFF_START_DT and INV.EFF_END_DT
    {% endset %}
    {% do run_query(update_sql) %}
{% endmacro %}