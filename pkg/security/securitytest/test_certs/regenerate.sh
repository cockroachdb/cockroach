#!/bin/sh
set -eux

dir_n="pkg/security/securitytest/test_certs"
rm -f "${dir_n}"/*.{crt,key}
./cockroach cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca.key" create-ca
./cockroach cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca.key" create-node 127.0.0.1 ::1 localhost *.local
./cockroach cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca.key" create-client root
./cockroach cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca.key" create-client testuser

# Tenant certs
./cockroach mt cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca-client-tenant.key" create-tenant-client-ca
./cockroach mt cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca-client-tenant.key" create-tenant-client 123456789

./cockroach mt cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca-server-tenant.key" create-tenant-server-ca
./cockroach mt cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca-server-tenant.key" create-tenant-server 127.0.0.1 ::1 localhost *.local

# HACK(tbg): at the time of writing, the KV layer does not accept tenant client
# certs yet, but tenants do use them. We overwrite the embedded tenant client
# certs with those the internal node user would use to bridge this gap.
# This can be used during local development.
#
# cat "${dir_n}/node.crt" > "${dir_n}/client-tenant.123456789.crt"
# cat "${dir_n}/node.key" > "${dir_n}/client-tenant.123456789.key"
# cat "${dir_n}/ca.crt" > "${dir_n}/ca-server-tenant.crt"

make generate PKG=./pkg/security/securitytest
