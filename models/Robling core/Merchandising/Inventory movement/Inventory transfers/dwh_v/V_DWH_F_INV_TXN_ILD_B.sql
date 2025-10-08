{{ config(
    materialized='view',
    alias='V_DWH_F_INV_TXN_ILD_B',
    schema='DW_DWH_V',
    tags = ['f_inv_txn_ild_ld'],
    post_hook = ["{{ load_recon_data('Inventory Transactions',recon_config_macro='mac_f_inv_txn_recon_script_sql', recon_step=1) }}"
                ,"{{ log_script_success(this) }}"]
) }}
SELECT
    SRC.* 
FROM {{ ref('DWH_F_INV_TXN_ILD_B') }} SRC

