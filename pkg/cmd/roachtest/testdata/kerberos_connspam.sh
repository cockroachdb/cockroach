#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


echo $1 | kinit $2
while true
  do /home/ubuntu/cockroach sql --url "postgresql://$2:$1@localhost:26257?sslmode=require" -e 'SELECT 1;'
done
