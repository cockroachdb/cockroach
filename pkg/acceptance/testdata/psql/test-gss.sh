#!/usr/bin/env bash

set -euo pipefail

krb5kdc
kadmin.local -q "addprinc -pw changeme tester@MY.EX"
echo changeme | kinit tester@MY.EX

psql -c "select 1" -U tester@MY.EX
