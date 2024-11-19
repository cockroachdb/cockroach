#!/bin/sh

# Copyright 2018 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -e

echo "Available certs:"
ls -l /certs

echo "Environment:"
env

echo "Creating a k5s token..."
echo psql | kinit tester@MY.EX

echo "Preparing SQL user ahead of test"
env \
    PGSSLKEY=/certs/client.root.key \
    PGSSLCERT=/certs/client.root.crt \
    psql -U root -c "ALTER USER root WITH PASSWORD 'rootpw'"

echo "Running test"
./gss.test
