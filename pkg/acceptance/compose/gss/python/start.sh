#!/bin/bash

set -e

echo "Available certs:"
ls -l /certs

echo "Environment:"
env

echo "Creating a k5s token..."
echo psql | kinit tester@MY.EX

export PGSSLKEY=/certs/client.root.key
export PGSSLCERT=/certs/client.root.crt
export PGUSER=root

echo "Creating test user"
psql -c "CREATE USER tester"
echo "Configuring the HBA rule prior to running the test..."
psql -c "SET CLUSTER SETTING server.host_based_authentication.configuration = 'host all all all gss include_realm=0'"

echo "Testing the django connection..."

unset PGSSLKEY
unset PGSSLCERT
export PGUSER=tester

# Exit with error unless we find the expected error message.
python manage.py inspectdb 2>&1 | grep 'use of GSS authentication requires an enterprise license'
