#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


echo psql | kinit tester
while true
  do ./cockroach sql --url "postgresql://tester:psql@localhost:26257?sslmode=require" -e 'SELECT 1;'
done
