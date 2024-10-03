# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

sudo systemd-run --working-directory=/home/ubuntu --service-type exec --collect --unit cct_kv --uid 1000 --gid 1000 ./kv_run.sh
sudo systemd-run --working-directory=/home/ubuntu --service-type exec --collect --unit cct_tpcc --uid 1000 --gid 1000 ./tpcc_run.sh
sudo systemd-run --working-directory=/home/ubuntu --service-type exec --collect --unit cct_tpcc_drop --uid 1000 --gid 1000 ./cct_tpcc_drop.sh
sudo systemd-run --working-directory=/home/ubuntu --service-type exec --collect --unit cct_schemachange --uid 1000 --gid 1000 ./schemachange_run.sh
sudo systemd-run --working-directory=/home/ubuntu --service-type exec --collect --unit chaos_test --uid=1000 --gid=1000 ./chaos_helper.sh
sudo systemd-run --working-directory=/home/ubuntu --service-type exec --collect --unit roachtest_operations --uid 1000 --gid 1000 ./roachtest_operations_run.sh
