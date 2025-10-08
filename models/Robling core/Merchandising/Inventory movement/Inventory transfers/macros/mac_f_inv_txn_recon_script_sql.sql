{% macro mac_f_inv_txn_recon_script_sql() %}
    {# 
        OVERVIEW:
        Generates a YAML configuration for daily inventory reconciliation steps, including SQL commands for each step.
        This configuration is used to load data from different source systems into the reconciliation table.
        Reconciliation check macro will use this configuration to perform the reconciliation check.
        
        INPUTS:
        - None
        
        OUTPUTS:
        - Returns a dictionary parsed from YAML containing reconciliation steps and SQL commands.
        - Each step includes the source system, fact type, SQL command, and validation range.
    #}
{%- set recon_yaml -%}
Inventory Transactions:
    recon_steps:
      - recon_step: 0
        src_system: LND
        fact_typ: Inventory Transactions
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
            /*This select loads Inventory OH data from landing table for current business date */
            SELECT
                'Inventory Transactions'             AS FACT_TYP
                ,'LND'                               AS SRC_SYSTEM
                ,'V_RECON_F_INV_TXN_ILD_B'           AS SRC_TABLE
                ,'${CURR_DAY}'                       AS LOAD_DT
                ,TO_DATE(SRC.TXN_TS)                 AS TXN_DT
                ,SRC.LOC_ID                          AS LOC_ID
                ,SRC.LCL_CNCY_CDE                    AS LCL_CNCY_CDE
                ,SUM(SRC.F_INV_TXN_QTY)              AS FACT_QTY
                ,SUM(SRC.F_INV_TXN_RTL_LCL)          AS FACT_RTL_LCL
                ,SUM(SRC.F_INV_TXN_CST_LCL)          AS FACT_CST_LCL
                ,NULL                                AS ATTR_1_NAME
                ,NULL                                AS ATTR_1_VALUE
                ,CURRENT_TIMESTAMP()                 AS RCD_INS_TS
                ,CURRENT_TIMESTAMP()                 AS RCD_UPD_TS
            FROM {{ source('INV_TXN_SRC','V_RECON_F_INV_TXN_ILD_B') }} SRC
            WHERE SRC.POST_DT = '${CURR_DAY}'
            GROUP BY LOAD_DT
                    ,TXN_DT
                    ,LOC_ID
                    ,LCL_CNCY_CDE
            ORDER BY TXN_DT, LOC_ID
        validation_range: "-1,1"
        format: "unit,unit,unit,percent,money,money,money,percent,money,money,money,percent"

      - recon_step: 0
        src_system: STG_V
        fact_typ: Inventory Transactions
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
            /*This select loads Inventory OH data from staging view for current business date */
            SELECT
                 'Inventory Transactions'            AS FACT_TYP
                ,'STG_V'                             AS SRC_SYSTEM
                ,'V_STG_F_INV_TXN_ILD_B'             AS SRC_TABLE
                ,'${CURR_DAY}'                       AS LOAD_DT
                ,TO_DATE(SRC.TXN_TS)                 AS TXN_DT
                ,SRC.LOC_ID                          AS LOC_ID
                ,SRC.LCL_CNCY_CDE                    AS LCL_CNCY_CDE
                ,SUM(SRC.F_INV_TXN_QTY)              AS FACT_QTY
                ,SUM(SRC.F_INV_TXN_RTL_LCL)          AS FACT_RTL_LCL
                ,SUM(SRC.F_INV_TXN_CST_LCL)          AS FACT_CST_LCL
                ,NULL                                AS ATTR_1_NAME
                ,NULL                                AS ATTR_1_VALUE
                ,CURRENT_TIMESTAMP()                 AS RCD_INS_TS
                ,CURRENT_TIMESTAMP()                 AS RCD_UPD_TS
            FROM {{ source('INV_TXN_SRC','V_STG_F_INV_TXN_ILD_B') }} SRC
            WHERE SRC.POST_DT = '${CURR_DAY}'
            GROUP BY LOAD_DT
                    ,TXN_DT
                    ,LOC_ID
                    ,LCL_CNCY_CDE
        validation_range: "-1,1"
        format: "unit,unit,unit,percent,money,money,money,percent,money,money,money,percent"


      - recon_step: 1
        src_system: DWH
        fact_typ: Inventory Transactions
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
            /*This select loads Inventory OH data from DWH view for current business date */
            SELECT
                 'Inventory Transactions'            AS FACT_TYP
                ,'DWH'                               AS SRC_SYSTEM
                ,'DWH_F_INV_TXN_ILD_B'               AS SRC_TABLE
                ,'${CURR_DAY}'                       AS LOAD_DT
                ,SRC.TXN_DT                          AS TXN_DT
                ,SRC.LOC_ID                          AS LOC_ID
                ,SRC.LCL_CNCY_CDE                    AS LCL_CNCY_CDE
                ,SUM(SRC.F_INV_TXN_QTY)              AS FACT_QTY
                ,SUM(SRC.F_INV_TXN_RTL_LCL)          AS FACT_RTL_LCL
                ,SUM(SRC.F_INV_TXN_CST_LCL)          AS FACT_CST_LCL
                ,NULL                                AS ATTR_1_NAME
                ,NULL                                AS ATTR_1_VALUE
                ,CURRENT_TIMESTAMP()                 AS RCD_INS_TS
                ,CURRENT_TIMESTAMP()                 AS RCD_UPD_TS
            FROM {{ source('INV_TXN_DWH', 'DWH_F_INV_TXN_ILD_B') }} SRC
            WHERE SRC.POST_DT = '${CURR_DAY}'
            GROUP BY LOAD_DT
                    ,TXN_DT
                    ,LOC_ID
                    ,LCL_CNCY_CDE
            ORDER BY TXN_DT, LOC_ID
        validation_range: "-1,1"
        format: "unit,unit,unit,percent,money,money,money,percent,money,money,money,percent"

      - recon_step: 2
        src_system: DM
        fact_typ: Inventory Transactions
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
                FACT_CST_LCL,
                FACT_RTL_LCL,
                ATTR_1_NAME,
                ATTR_1_VALUE,
                RCD_INS_TS,
                RCD_UPD_TS
            )
            /*This select loads Inventory OH data from datamart table for current business date */
            SELECT 
                'Inventory Transactions'      AS FACT_TYP
                ,'DM'                         AS SRC_SYSTEM
                ,'DM_F_MEAS_FACT_ILD_B'       AS SRC_TABLE
                ,'${CURR_DAY}'                AS LOAD_DT
                ,MEAS_DT                      AS TXN_DT
                ,SRC.LOC_ID                   AS LOC_ID
                ,SRC.LCL_CNCY_CDE             AS LCL_CNCY_CDE
                ,SUM(F_FACT_QTY)              AS FACT_QTY
                ,SUM(F_FACT_CST)              AS FACT_CST_LCL
                ,SUM(F_FACT_RTL)              AS FACT_RTL_LCL
                ,NULL                         AS ATTR_1_NAME
                ,NULL                         AS ATTR_1_VALUE
                ,CURRENT_TIMESTAMP            AS RCD_INS_TS
                ,CURRENT_TIMESTAMP            AS RCD_UPD_TS
            FROM {{ source('INV_TXN_DM','DM_F_MEAS_FACT_ILD_B') }} SRC
            INNER JOIN {{ source('INV_TXN_CFG_LU', 'CFG_INV_TXN_LU') }} 
              CFG ON CFG.FACT_CDE = SRC.FACT_CDE 
            GROUP BY LOAD_DT
                    ,TXN_DT
                    ,LOC_ID
                    ,LCL_CNCY_CDE    
            ORDER BY TXN_DT, LOC_ID
        validation_range: "-1,1"
        format: "unit,unit,unit,percent,money,money,money,percent,money,money,money,percent"
{%- endset -%}
    
    {% set config = fromyaml(recon_yaml) %}
    {{ return(config) }}
{% endmacro %}
