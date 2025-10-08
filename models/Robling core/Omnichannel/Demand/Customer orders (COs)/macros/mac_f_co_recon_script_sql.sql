{% macro mac_f_co_recon_script_sql() %}
    {# 
        OVERVIEW:
        Generates a YAML configuration for Customer Order reconciliation steps, including SQL commands for each step.
        This configuration is used to load data from different source systems into the reconciliation table.
        Reconciliation check macro will use this configuration to perform the reconciliation check.
        
        INPUTS:
        - None
        
        OUTPUTS:
        - Returns a dictionary parsed from YAML containing reconciliation steps and SQL commands.
        - Each step includes the source system, fact type, SQL command, and validation range.
    #}
{%- set recon_yaml -%}
Customer Order:
    recon_steps:
      - recon_step: 0
        src_system: LND
        fact_typ: Customer Order
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
            /*This select loads Customer Order data from landing tables for current business date */
            SELECT 'Customer Order'             AS FACT_TYP
                ,'LND'                          AS SRC_SYSTEM
                ,'V_RECON_F_ECOMM_CO_LN_ITM_B'  AS SRC_TABLE
                ,'${CURR_DAY}'                  AS LOAD_DT
                ,TO_DATE(SRC.CO_ORD_TS)         AS TXN_DT
                ,SRC.DMND_LOC_ID                AS LOC_ID
                ,SRC.LCL_CNCY_CDE               AS LCL_CNCY_CDE
                ,SUM(SRC.F_CO_ORD_QTY)          AS FACT_QTY
                ,SUM(SRC.F_CO_ORD_RTL_LCL)      AS FACT_RTL_LCL
                ,NULL                           AS FACT_CST_LCL
                ,NULL                           AS ATTR_1_NAME
                ,NULL                           AS ATTR_1_VALUE
                ,CURRENT_TIMESTAMP()            AS RCD_INS_TS
                ,CURRENT_TIMESTAMP()            AS RCD_UPD_TS
            FROM {{ source('CO_RECON_SRC_STG','V_RECON_F_ECOMM_CO_LN_ITM_B') }} SRC
            GROUP BY LOAD_DT
                    ,TXN_DT
                    ,LOC_ID
                    ,LCL_CNCY_CDE
        validation_range: "-1,1"
        format: "unit,unit,unit,percent,money,money,money,percent,money,money,money,percent"

      - recon_step: 0
        src_system: STG_V
        fact_typ: Customer Order
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
            SELECT 'Customer Order'           AS FACT_TYP
                ,'STG_V'                      AS SRC_SYSTEM
                ,'V_STG_F_ECOMM_CO_LN_ITM_B'  AS SRC_TABLE
                ,'${CURR_DAY}'                AS LOAD_DT
                ,TO_DATE(SRC.CO_ORD_TS)       AS TXN_DT
                ,SRC.DMND_LOC_ID              AS LOC_ID
                ,SRC.LCL_CNCY_CDE             AS LCL_CNCY_CDE
                ,SUM(SRC.F_CO_ORD_QTY)        AS FACT_QTY
                ,SUM(SRC.F_CO_ORD_RTL_LCL)    AS FACT_RTL_LCL
                ,NULL                         AS FACT_CST_LCL
                ,NULL                         AS ATTR_1_NAME
                ,NULL                         AS ATTR_1_VALUE
                ,CURRENT_TIMESTAMP()          AS RCD_INS_TS
                ,CURRENT_TIMESTAMP()          AS RCD_UPD_TS
            FROM {{ source('CO_RECON_SRC_STG','V_STG_F_ECOMM_CO_LN_ITM_B') }} SRC
            GROUP BY LOAD_DT
                    ,TXN_DT
                    ,LOC_ID
                    ,LCL_CNCY_CDE
        validation_range: "-1,1"
        format: "unit,unit,unit,percent,money,money,money,percent,money,money,money,percent"


      - recon_step: 1
        src_system: DWH
        fact_typ: Customer Order
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
            SELECT 'Customer Order'           AS FACT_TYP
                ,'DWH'                        AS SRC_SYSTEM
                ,'DWH_F_ECOMM_CO_LN_ITM_B'    AS SRC_TABLE
                ,'${CURR_DAY}'                AS LOAD_DT
                ,SRC.CO_ORD_DT                AS TXN_DT
                ,SRC.DMND_LOC_ID              AS LOC_ID
                ,SRC.LCL_CNCY_CDE             AS LCL_CNCY_CDE
                ,SUM(SRC.F_CO_ORD_QTY)        AS FACT_QTY
                ,SUM(SRC.F_CO_ORD_RTL_LCL)    AS FACT_RTL_LCL
                ,NULL                         AS FACT_CST_LCL
                ,NULL                         AS ATTR_1_NAME
                ,NULL                         AS ATTR_1_VALUE
                ,CURRENT_TIMESTAMP()          AS RCD_INS_TS
                ,CURRENT_TIMESTAMP()          AS RCD_UPD_TS
            FROM {{ source('CO_RECON_SRC_DWH','V_DWH_F_ECOMM_CO_LN_ITM_B') }} SRC
            GROUP BY LOAD_DT
                    ,TXN_DT
                    ,LOC_ID
                    ,LCL_CNCY_CDE
        validation_range: "-1,1"
        format: "unit,unit,unit,percent,money,money,money,percent,money,money,money,percent"

      - recon_step: 2
        src_system: DM
        fact_typ: Customer Order
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
            SELECT 'Customer Order'           AS FACT_TYP
                ,'DM'                         AS SRC_SYSTEM
                ,'DM_F_MEAS_FACT_ILD_B'       AS SRC_TABLE
                ,'${CURR_DAY}'                AS LOAD_DT
                ,SRC.MEAS_DT                  AS TXN_DT
                ,SRC.DMND_LOC_ID              AS LOC_ID
                ,SRC.LCL_CNCY_CDE             AS LCL_CNCY_CDE
                ,SUM(SRC.F_FACT_QTY)          AS FACT_QTY
                ,SUM(SRC.F_FACT_RTL)          AS FACT_RTL_LCL
                ,NULL                         AS FACT_CST_LCL
                ,NULL                         AS ATTR_1_NAME
                ,NULL                         AS ATTR_1_VALUE
                ,CURRENT_TIMESTAMP            AS RCD_INS_TS
                ,CURRENT_TIMESTAMP            AS RCD_UPD_TS
            FROM {{ source('CO_RECON_SRC_DM','DM_F_MEAS_FACT_ILD_B') }} SRC
            WHERE SRC.FACT_CDE = 'CO_ORD' 
            GROUP BY LOAD_DT
                    ,TXN_DT
                    ,DMND_LOC_ID
                    ,LCL_CNCY_CDE    
        validation_range: "-1,1"
        format: "unit,unit,unit,percent,money,money,money,percent,money,money,money,percent"
{%- endset -%}
    
    {% set config = fromyaml(recon_yaml) %}
    {{ return(config) }}
{% endmacro %}
