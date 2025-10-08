{% macro mac_f_rtv_recon_script_sql() %}
    {# 
        OVERVIEW:
        Generates a YAML configuration for RTV reconciliation steps, including SQL commands for each step.
        This configuration is used to load data from different source systems into the reconciliation table.
        Reconciliation check macro will use this configuration to perform the reconciliation check.
        
        INPUTS:
        - None
        
        OUTPUTS:
        - Returns a dictionary parsed from YAML containing reconciliation steps and SQL commands.
        - Each step includes the source system, fact type, SQL command, and validation range.
    #}
{%- set recon_yaml -%}
RTV:
    {# Optional subject-level override for delete filter.
        If specified, this replaces the default "LOAD_DT = curr_day" filter in load_recon_data.
        Used to handle partition-based deletes (e.g., RTV by POST_DT). #}
    recon_post_dt_filter : LOAD_DT IN (SELECT DISTINCT POST_DT FROM {{ source('RTV_RECON_SRC_STG','V_STG_F_INV_RTV_SUP_ILD_B') }})
    recon_steps:
      - recon_step: 0
        src_system: LND
        fact_typ: RTV
        sql: |
            INSERT INTO DW_DWH.DWH_F_RECON_LD (
                FACT_TYP
                ,SRC_SYSTEM
                ,SRC_TABLE
                ,LOAD_DT
                ,TXN_DT
                ,LOC_ID
                ,LCL_CNCY_CDE
                ,FACT_QTY
                ,FACT_RTL_LCL
                ,FACT_CST_LCL
                ,ATTR_1_NAME
                ,ATTR_1_VALUE
                ,RCD_INS_TS
                ,RCD_UPD_TS
                )
            /*This select loads RTV data to recon table from recon view */
            SELECT 'RTV'                       AS FACT_TYP
                ,'LND'                         AS SRC_SYSTEM
                ,'V_RECON_F_INV_RTV_SUP_ILD_B' AS SRC_TABLE
                ,SRC.POST_DT                   AS LOAD_DT -- Mapping updated from CURR_DAY to POST_DT to support deletion and comparison based on POST_DT from STG during reconciliation
                ,TO_DATE(SRC.TXN_DT)           AS TXN_DT
                ,SRC.LOC_ID                    AS LOC_ID
                ,SRC.LCL_CNCY_CDE              AS LCL_CNCY_CDE
                ,SUM(SRC.F_RTV_QTY)            AS FACT_QTY
                ,SUM(SRC.F_RTV_RTL_LCL)        AS FACT_RTL_LCL
                ,SUM(SRC.F_RTV_CST_LCL)        AS FACT_CST_LCL
                ,NULL                          AS ATTR_1_NAME
                ,NULL                          AS ATTR_1_VALUE
                ,CURRENT_TIMESTAMP()           AS RCD_INS_TS
                ,CURRENT_TIMESTAMP()           AS RCD_UPD_TS
            FROM {{ source('RTV_RECON_SRC_STG','V_RECON_F_INV_RTV_SUP_ILD_B') }} SRC
            GROUP BY LOAD_DT
                    ,TXN_DT
                    ,LOC_ID
                    ,LCL_CNCY_CDE
        validation_range: "-1,1"
        format: "unit,unit,unit,percent,money,money,money,percent,money,money,money,percent"

      - recon_step: 0
        src_system: STG_V
        fact_typ: RTV
        sql: |
            INSERT INTO DW_DWH.DWH_F_RECON_LD (
                FACT_TYP
                ,SRC_SYSTEM
                ,SRC_TABLE
                ,LOAD_DT
                ,TXN_DT
                ,LOC_ID
                ,LCL_CNCY_CDE
                ,FACT_QTY
                ,FACT_RTL_LCL
                ,FACT_CST_LCL
                ,ATTR_1_NAME
                ,ATTR_1_VALUE
                ,RCD_INS_TS
                ,RCD_UPD_TS
                )
            /*This select loads RTV data to recon table from staging view */
            SELECT 'RTV'                     AS FACT_TYP
                ,'STG_V'                     AS SRC_SYSTEM
                ,'V_STG_F_INV_RTV_SUP_ILD_B' AS SRC_TABLE
                ,SRC.POST_DT                 AS LOAD_DT -- Mapping updated from CURR_DAY to POST_DT to support deletion and comparison based on POST_DT from STG during reconciliation
                ,TO_DATE(SRC.TXN_DT)         AS TXN_DT
                ,SRC.LOC_ID                  AS LOC_ID
                ,SRC.LCL_CNCY_CDE            AS LCL_CNCY_CDE
                ,SUM(SRC.F_RTV_QTY)          AS FACT_QTY
                ,SUM(SRC.F_RTV_RTL_LCL)      AS FACT_RTL_LCL
                ,SUM(SRC.F_RTV_CST_LCL)      AS FACT_CST_LCL
                ,NULL                        AS ATTR_1_NAME
                ,NULL                        AS ATTR_1_VALUE
                ,CURRENT_TIMESTAMP()         AS RCD_INS_TS
                ,CURRENT_TIMESTAMP()         AS RCD_UPD_TS
            FROM {{ source('RTV_RECON_SRC_STG','V_STG_F_INV_RTV_SUP_ILD_B') }} SRC
            GROUP BY LOAD_DT
                    ,TXN_DT
                    ,LOC_ID
                    ,LCL_CNCY_CDE
        validation_range: "-1,1"
        format: "unit,unit,unit,percent,money,money,money,percent,money,money,money,percent"


      - recon_step: 1
        src_system: DWH
        fact_typ: RTV
        sql: |
            INSERT INTO DW_DWH.DWH_F_RECON_LD (
                FACT_TYP,
                SRC_SYSTEM,
                SRC_TABLE,
                LOAD_DT,
                TXN_DT,
                LOC_ID,
                LCL_CNCY_CDE,
                FACT_QTY,
                FACT_RTL_LCL,
                FACT_CST_LCL,
                ATTR_1_NAME,
                ATTR_1_VALUE,
                RCD_INS_TS,
                RCD_UPD_TS
            )
             /*This select loads RTV data to recon table from dwh view */
            SELECT 'RTV'                      AS FACT_TYP
                ,'DWH'                        AS SRC_SYSTEM
                ,'DWH_F_INV_RTV_SUP_ILD_B'    AS SRC_TABLE
                ,SRC.POST_DT                  AS LOAD_DT -- Mapping updated from CURR_DAY to POST_DT to support deletion and comparison based on POST_DT from STG during reconciliation
                ,SRC.TXN_DT                   AS TXN_DT
                ,SRC.LOC_ID                   AS LOC_ID
                ,SRC.LCL_CNCY_CDE             AS LCL_CNCY_CDE
                ,SUM(SRC.F_RTV_QTY)           AS FACT_QTY
                ,SUM(SRC.F_RTV_RTL_LCL)       AS FACT_RTL_LCL
                ,SUM(SRC.F_RTV_CST_LCL)       AS FACT_CST_LCL
                ,NULL                         AS ATTR_1_NAME
                ,NULL                         AS ATTR_1_VALUE
                ,CURRENT_TIMESTAMP()          AS RCD_INS_TS
                ,CURRENT_TIMESTAMP()          AS RCD_UPD_TS
            FROM {{ source('RTV_RECON_SRC_DWH','V_DWH_F_INV_RTV_SUP_ILD_B') }} SRC
            WHERE SRC.POST_DT IN (SELECT DISTINCT POST_DT FROM {{ source('RTV_RECON_SRC_STG','V_STG_F_INV_RTV_SUP_ILD_B') }} )
            GROUP BY LOAD_DT, TXN_DT, LOC_ID, LCL_CNCY_CDE
        validation_range: "-1,1"
        format: "unit,unit,unit,percent,money,money,money,percent,money,money,money,percent"

      - recon_step: 2
        src_system: DM
        fact_typ: RTV
        sql: |
            INSERT INTO DW_DWH.DWH_F_RECON_LD (
                FACT_TYP,
                SRC_SYSTEM,
                SRC_TABLE,
                LOAD_DT,
                TXN_DT,
                LOC_ID,
                LCL_CNCY_CDE,
                FACT_QTY,
                FACT_RTL_LCL,
                FACT_CST_LCL,
                ATTR_1_NAME,
                ATTR_1_VALUE,
                RCD_INS_TS,
                RCD_UPD_TS
            )
             /*This select loads RTV data to recon table from datamart table */
            SELECT 'RTV'                      AS FACT_TYP
                ,'DM'                         AS SRC_SYSTEM
                ,'DM_F_MEAS_FACT_ILD_B'       AS SRC_TABLE
                ,SRC.POST_DT                  AS LOAD_DT -- Mapping updated from CURR_DAY to POST_DT to support deletion and comparison based on POST_DT from STG during reconciliation
                ,SRC.MEAS_DT                  AS TXN_DT
                ,SRC.LOC_ID                   AS LOC_ID
                ,SRC.LCL_CNCY_CDE             AS LCL_CNCY_CDE
                ,SUM(SRC.F_FACT_QTY)          AS FACT_QTY
                ,SUM(SRC.F_FACT_RTL)          AS FACT_RTL_LCL
                ,SUM(SRC.F_FACT_CST)          AS FACT_CST_LCL
                ,NULL                         AS ATTR_1_NAME
                ,NULL                         AS ATTR_1_VALUE
                ,CURRENT_TIMESTAMP            AS RCD_INS_TS
                ,CURRENT_TIMESTAMP            AS RCD_UPD_TS
            FROM {{ source('RTV_RECON_SRC_DM','DM_F_MEAS_FACT_ILD_B') }} SRC
            WHERE SRC.FACT_CDE = 'RTV' 
                AND SRC.POST_DT IN (SELECT DISTINCT POST_DT FROM {{ source('RTV_RECON_SRC_STG','V_STG_F_INV_RTV_SUP_ILD_B') }} )
                GROUP BY LOAD_DT
                        ,TXN_DT
                        ,LOC_ID
                        ,LCL_CNCY_CDE    
        validation_range: "-1,1"
        format: "unit,unit,unit,percent,money,money,money,percent,money,money,money,percent"
{%- endset -%}
    
    {% set config = fromyaml(recon_yaml) %}
    {{ return(config) }}
{% endmacro %}
