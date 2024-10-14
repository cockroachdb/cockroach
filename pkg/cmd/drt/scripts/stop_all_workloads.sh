# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

sudo systemctl stop cct_schemachange
sudo systemctl stop cct_kv
sudo systemctl stop cct_tpcc
sudo systemctl stop cct_tpcc_drop
sudo systemctl stop chaos_test
