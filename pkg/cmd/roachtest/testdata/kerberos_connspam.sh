#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


echo psql | kinit tester
while true
  do env PGHOST=localhost PGPORT=26257 PGUSER=tester psql 'sslmode=require' -c 'select 1;'
done
