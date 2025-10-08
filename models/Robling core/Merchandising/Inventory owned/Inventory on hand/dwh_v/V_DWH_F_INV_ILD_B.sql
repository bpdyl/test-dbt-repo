{{ config(
    materialized='view',
    alias='V_DWH_F_INV_ILD_B',
    schema='DW_DWH_V',
    tags = ['f_inv_ild_ld'],
    post_hook = ["{{ load_recon_data('Inventory On-Hand',recon_config_macro='mac_f_inv_recon_script_sql', recon_step=1) }}"
                    ,"{{ update_staging_recon() }}"   
                    ,"{{ log_script_success(this) }}"]
) }}
SELECT
    SRC.* 
FROM {{ ref('DWH_F_INV_ILD_B') }} SRC

