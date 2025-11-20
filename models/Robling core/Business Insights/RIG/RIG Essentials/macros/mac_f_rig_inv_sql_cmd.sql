{% macro upate_recap_md_dates_using_dwh_table() %}
    {# 
        OVERVIEW:
        This macro updates FIRST_CC_LOC_MD_DT and FIRST_ITM_MD_DT in the recap table 
        using the DWH inventory table to capture missing clearance dates 
        that were filtered out in the datamart.       
        
        INPUTS:
        None
        
        OUTPUTS:
        - None. This runs an update on RIG_F_INV_RECAP_IL_B table 
    #}
    {% set update_sql %}
        UPDATE DW_RIG.RIG_F_INV_RECAP_IL_B RECAP
        SET
            RECAP.FIRST_CC_LOC_MD_DT = SRC.FIRST_CC_LOC_MD_DT,
            RECAP.FIRST_ITM_MD_DT = SRC.FIRST_ITM_MD_DT
        FROM (
            SELECT
                INV.ITM_ID                                                              AS ITM_ID
                ,INV.LOC_ID                                                             AS LOC_ID
                ,MIN(CASE WHEN INV.ITMLOC_STTS_CDE <> 'R' THEN INV.EFF_START_DT END) 
                    OVER (PARTITION BY ITM.STY_ID, ITM.COLOR_ID, INV.LOC_ID)            AS FIRST_CC_LOC_MD_DT
                ,MIN(CASE WHEN INV.ITMLOC_STTS_CDE <> 'R' THEN INV.EFF_START_DT END) 
                    OVER (PARTITION BY INV.ITM_ID, INV.LOC_ID)                          AS FIRST_ITM_MD_DT
            FROM {{ ref('V_DWH_F_INV_ILD_B') }} INV
            INNER JOIN {{ ref('V_DWH_D_PRD_ITM_LU') }} ITM ON ITM.ITM_ID = INV.ITM_ID
            QUALIFY ROW_NUMBER() OVER (PARTITION BY INV.ITM_ID, INV.LOC_ID ORDER BY INV.EFF_START_DT) = 1
        ) SRC
        WHERE RECAP.ITM_ID = SRC.ITM_ID
        AND RECAP.LOC_ID = SRC.LOC_ID
        AND HASH(RECAP.FIRST_CC_LOC_MD_DT, RECAP.FIRST_ITM_MD_DT) <> HASH(SRC.FIRST_CC_LOC_MD_DT, SRC.FIRST_ITM_MD_DT)
    {% endset %}
    {% do run_query(update_sql) %}
{% endmacro %}
