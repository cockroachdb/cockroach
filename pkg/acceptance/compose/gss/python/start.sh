#!/bin/bash

set -e

export PGSSLKEY=/certs/client.root.key
export PGSSLCERT=/certs/client.root.crt
export PGUSER=root
export PGPASSWORD=unused

psql -c "SET CLUSTER SETTING server.host_based_authentication.configuration = 'host all all all gss include_realm=0'"
psql -c "CREATE USER tester"

echo psql | kinit tester@MY.EX

unset PGSSLKEY
unset PGSSLCERT
export PGUSER=tester

# Exit with error unless we find the expected error message.
python manage.py inspectdb 2>&1 | grep 'use of GSS authentication requires an enterprise license'
