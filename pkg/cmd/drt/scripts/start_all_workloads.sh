sudo systemd-run --working-directory=/home/ubuntu --service-type exec --collect --unit cct_kv ./kv_run.sh
sudo systemd-run --working-directory=/home/ubuntu --service-type exec --collect --unit cct_tpcc ./tpcc_run.sh
sudo systemd-run --working-directory=/home/ubuntu --service-type exec --collect --unit cct_tpcc_drop ./cct_tpcc_drop.sh
sudo systemd-run --working-directory=/home/ubuntu --service-type exec --collect --unit cct_schemachange ./schemachange_run.sh
