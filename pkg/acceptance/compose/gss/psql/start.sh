#!/bin/sh

set -e

echo psql | kinit tester@MY.EX

env \
    PGSSLKEY=/certs/client.root.key \
    PGSSLCERT=/certs/client.root.crt \
    psql -c "ALTER USER root WITH PASSWORD rootpw"

./gss.test
