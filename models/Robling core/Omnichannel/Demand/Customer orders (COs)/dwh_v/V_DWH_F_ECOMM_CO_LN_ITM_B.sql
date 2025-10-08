{{ config(
    materialized='view',
    alias='V_DWH_F_ECOMM_CO_LN_ITM_B',
    schema='DW_DWH_V',
    tags=['f_ecomm_co_ln_itm_ld'],
    post_hook = ["{{ load_recon_data('Customer Order',recon_config_macro='mac_f_co_recon_script_sql', recon_step=1) }}"
                  ,"{{ log_script_success(this) }}"]
) }}
SELECT
  SRC.*
FROM {{ ref('DWH_F_ECOMM_CO_LN_ITM_B') }} SRC
