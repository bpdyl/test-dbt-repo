{% set key_columns = ['FACT_CDE', 'CO_ID', 'ATTR_VARCHAR_COL14'] %}
{% set hash_columns = [
    'POST_DT', 'MEAS_DT', 'ORD_DOC_CREATED_DT', 'CANCLD_DT', 'MIN_KEY', 'CHN_KEY', 'CHN_ID', 'CHNL_KEY'
    , 'CHNL_ID', 'LOC_KEY', 'LOC_ID', 'DMND_LOC_KEY', 'DMND_LOC_ID', 'LOC_TYP_CDE', 'POSTAL_CDE'
    , 'STATE_PROVINCE_CDE', 'COUNTRY_CDE', 'DIV_KEY', 'DIV_ID', 'ITM_KEY', 'ITM_ID', 'ITMLOC_STTS_CDE'
    , 'RSN_ID', 'STTS_CDE', 'ORD_DOC_LN_STTS_CDE', 'LCL_CNCY_CDE', 'F_FACT_QTY', 'F_FACT_CST'
    , 'F_FACT_RTL'
] %}

{{
    config(
        materialized='custom_merge',
        unique_key=key_columns,
        key_columns=key_columns,
        hash_columns=hash_columns,
        insert_columns=key_columns + hash_columns,
        target_table = 'DM_F_MEAS_FACT_ILD_B',
        schema='DM_MERCH',
        tags = ['dm_f_co_cancld_meas_fact_ild'],
        pre_hook=["{{ start_script('dm_f_co_cancld_meas_fact_ild','RUNNING','NONE') }}"],
        post_hook = ["{{ log_script_success(this) }}"]
    )
}}
SELECT   
    'CO_CANCLD'                                                         AS FACT_CDE
    ,SRC.CANCLD_DT                                                      AS POST_DT
    ,SRC.CANCLD_DT                                                      AS MEAS_DT
    ,SRC.CO_ORD_DT                                                      AS ORD_DOC_CREATED_DT
    ,SRC.CANCLD_DT                                                      AS CANCLD_DT
    ,SRC.CANCLD_MIN_KEY                                                 AS MIN_KEY
    ,{{ get_key_with_fallback_value('DMND_LOC.CHN_KEY') }}              AS CHN_KEY
    ,DMND_LOC.CHN_ID                                                    AS CHN_ID
    ,{{ get_key_with_fallback_value('DMND_LOC.CHNL_KEY') }}             AS CHNL_KEY
    ,DMND_LOC.CHNL_ID                                                   AS CHNL_ID
    ,SRC.DMND_LOC_KEY                                                   AS LOC_KEY
    ,SRC.DMND_LOC_ID                                                    AS LOC_ID
    ,SRC.DMND_LOC_KEY                                                   AS DMND_LOC_KEY
    ,SRC.DMND_LOC_ID                                                    AS DMND_LOC_ID
    ,DMND_LOC.LOC_TYP_CDE                                               AS LOC_TYP_CDE
    ,SRC.DLVRY_POSTAL_CDE                                               AS POSTAL_CDE
    ,SRC.DLVRY_STATE                                                    AS STATE_PROVINCE_CDE
    ,SRC.DLVRY_COUNTRY_CDE                                              AS COUNTRY_CDE
    ,{{ get_key_with_fallback_value('ITM.DIV_KEY') }}                   AS DIV_KEY
    ,ITM.DIV_ID                                                         AS DIV_ID
    ,SRC.ITM_KEY                                                        AS ITM_KEY
    ,SRC.ITM_ID                                                         AS ITM_ID
    ,SRC.ITMLOC_STTS_CDE                                                AS ITMLOC_STTS_CDE
    ,SRC.CANCLD_RSN                                                     AS RSN_ID
    ,SRC.CO_ID                                                          AS CO_ID
    ,SRC.CO_LN_ITM_STTS                                                 AS STTS_CDE
    ,SRC.CO_LN_ITM_STTS                                                 AS ORD_DOC_LN_STTS_CDE
    ,SRC.LCL_CNCY_CDE                                                   AS LCL_CNCY_CDE
    ,SRC.F_CO_CANCLD_QTY                                                AS F_FACT_QTY
    ,SRC.F_CO_CANCLD_QTY * SRC.F_CO_UNIT_CST_LCL                        AS F_FACT_CST
    ,SRC.F_CO_CANCLD_QTY * SRC.F_CO_PAID_UNIT_RTL_LCL                   AS F_FACT_RTL
    ,SRC.CO_LN_ID                                                       AS ATTR_VARCHAR_COL14 --CO_LN_ID is a part of PK for the DWH table and is used as part of the merge condition as well.
    ,CURRENT_TIMESTAMP()                                                AS RCD_INS_TS                                
    ,CURRENT_TIMESTAMP()                                                AS RCD_UPD_TS
FROM {{ source('CO_SRC_DM','V_DWH_F_ECOMM_CO_LN_ITM_B') }} SRC
LEFT JOIN {{ ref('V_DWH_D_ORG_LOC_LU') }} DMND_LOC ON SRC.DMND_LOC_ID = DMND_LOC.LOC_ID
LEFT JOIN {{ ref('V_DWH_D_PRD_ITM_LU') }} ITM ON SRC.ITM_ID = ITM.ITM_ID
WHERE (
        SRC.F_CO_CANCLD_QTY <> 0
        OR SRC.F_CO_CANCLD_QTY * SRC.F_CO_UNIT_CST_LCL <> 0
        OR SRC.F_CO_CANCLD_QTY * SRC.F_CO_PAID_UNIT_RTL_LCL <> 0
    )
-- ordering by SRC.CANCLD_DT, SRC.DMND_LOC_KEY for performance through Snowflake partitioning
ORDER BY SRC.CANCLD_DT
        ,SRC.DMND_LOC_KEY 
