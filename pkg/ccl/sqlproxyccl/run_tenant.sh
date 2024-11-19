#!/bin/bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euo pipefail

# This script is used by run_proxy_indirect_crdb.sh to start a new tenant sql process. It can be customized to do
# additional actions when starting a new tenant process.

# The cockroach db executable to use
COCKROACH=$1
# The base directory for storing certs and data. Assumes $BASE/tenant is used for tenant and $BASE/client has client certs.
BASE=$2
TENANT=$BASE/tenant
# Ordinal instance
ORDINAL=$3

# The remaining argument (after the first 4) should contain --sql-addr, --http-addr and --tenant-id at the very least.

# Look for --tenant-id=<id> argument that should be present.
for i in "$@"; do case $i in --tenant-id=*) TENANT_ID="${i#*=}";;*);; esac done
if [ -z ${TENANT_ID+x} ]; then echo "The required --tenant-id=<id> argument is not present."; exit 1; fi
echo Tenant $TENANT_ID

# Look for --sql-addr <addr/host>[:<port>] argument that should be present.
for i in "$@"; do case $i in --sql-addr=*) SQL_ADDR="${i#*=}";;*);; esac done
if [ -z ${SQL_ADDR+x} ]; then echo "The required --sql-addr argument is not present."; exit 1; fi
echo SQL addr $SQL_ADDR

# Look for --kv-addrs strings argument that should be present.
for i in "$@"; do case $i in --kv-addrs=*) KV_ADDRS="${i#*=}";;*);; esac done
if [ -z ${KV_ADDRS+x} ]; then echo "The required --kv-addrs argument is not present."; exit 1; fi
echo KV addrs $KV_ADDRS
read -rd ',' KV_ADDR <<< "$KV_ADDRS,"
echo KV addr "$KV_ADDR"

if [ ! -d "$TENANT"/$TENANT_ID ]
then
  PASSWORD_UNSET=1
  echo Connect as root to the host cluster and create tenant
  COCKROACH_CA_KEY=$BASE/client/ca.key COCKROACH_CERTS_DIR=$BASE/client $COCKROACH sql --host="$KV_ADDR"  -e "
    SELECT CASE WHEN NOT EXISTS(SELECT * FROM system.tenants WHERE id = $TENANT_ID)
    THEN crdb_internal.create_tenant($TENANT_ID)
    END" > /dev/null
  echo Tenant directory "$TENANT"/$TENANT_ID does not exist. Initial tenant setup. Create directory.
  mkdir -p "$TENANT"/$TENANT_ID
  echo Create cert for tenant $TENANT_ID
  COCKROACH_CA_KEY=$TENANT/ca.key COCKROACH_CERTS_DIR=$TENANT $COCKROACH cert create-tenant-client $TENANT_ID 127.0.0.1 ::1 :: localhost "*.local"
fi  

if [ ! -d "$TENANT"/$TENANT_ID/"$ORDINAL" ]; then mkdir -p "$TENANT"/$TENANT_ID/"$ORDINAL"; fi

echo Start the tenant
COCKROACH_TRUST_CLIENT_PROVIDED_SQL_REMOTE_ADDR=true
$COCKROACH mt start-sql --certs-dir="$TENANT" --store="$TENANT"/$TENANT_ID/"$ORDINAL" "${@:4}" --log="{sinks: {stderr: {}}}" --background

if [ "$PASSWORD_UNSET" -eq "1" ]
then
  echo Create root user cert for the tenant
  COCKROACH_CA_KEY=$TENANT/ca.key COCKROACH_CERTS_DIR=$TENANT cockroach cert create-client root --tenant-scope=$TENANT_ID
  mv $TENANT/client.root.* $TENANT/$TENANT_ID/
  echo Set the password
  COCKROACH_CERTS_DIR=$TENANT/$TENANT_ID $COCKROACH sql --url="postgres://root@$SQL_ADDR?sslmode=verify-full&sslrootcert=$TENANT/ca.crt" -e "alter user root with password 'secret'" > "$TENANT"/$TENANT_ID/alter_root.log 2>&1
fi

wait
