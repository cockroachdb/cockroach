#!/bin/sh

set -e

echo "Available certs:"
ls -l /certs

echo "Environment:"
env

echo "Creating a k5s token..."
echo psql | kinit tester@MY.EX

echo "Preparing SQL user ahead of test"
chown -R $USER /certs
env \
    PGSSLKEY=/certs/client.root.key \
    PGSSLCERT=/certs/client.root.crt \
    psql -c "ALTER USER root WITH PASSWORD rootpw"

echo "Running test"
./gss.test
