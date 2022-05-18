#!/bin/sh

set -e

echo psql | kinit tester@MY.EX

echo "Preparing SQL user ahead of test"
env \
    PGSSLKEY=/certs/client.root.key \
    PGSSLCERT=/certs/client.root.crt \
    psql -U root -c "ALTER USER root WITH PASSWORD rootpw"

echo "Running test"
./gss.test
