{{ config(
    materialized='view',
    alias='V_DWH_F_SLS_TXN_LN_ITM_B',
    schema='DW_DWH_V',
    tags=['f_sls_txn_ln_itm_ld'],
    post_hook = ["{{ load_recon_data('Sales',recon_config_macro='mac_f_sls_recon_script_sql', recon_step=1) }}"
                  ,"{{ log_script_success(this) }}"]
) }}
SELECT
  SRC.*
FROM {{ ref('DWH_F_SLS_TXN_LN_ITM_B') }} SRC
