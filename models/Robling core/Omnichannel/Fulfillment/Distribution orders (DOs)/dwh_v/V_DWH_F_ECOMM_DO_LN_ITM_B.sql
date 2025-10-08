{{ config(
    materialized='view',
    alias='V_DWH_F_ECOMM_DO_LN_ITM_B',
    schema='DW_DWH_V',
    tags=['f_ecomm_do_ln_itm_ld'],
    post_hook = ["{{ load_recon_data('Fulfillment',recon_config_macro='mac_f_do_recon_script_sql', recon_step=1) }}"
                  ,"{{ log_script_success(this) }}"]
) }}
SELECT
  SRC.*
FROM {{ ref('DWH_F_ECOMM_DO_LN_ITM_B') }} SRC
