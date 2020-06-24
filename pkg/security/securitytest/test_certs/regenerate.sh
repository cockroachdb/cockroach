#!/bin/sh
set -eux

dir_n="pkg/security/securitytest/test_certs"
rm -f "${dir_n}"/*.{crt,key}
./cockroach cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca.key" create-ca
./cockroach cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca.key" create-node 127.0.0.1 ::1 localhost *.local
./cockroach cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca.key" create-client root
./cockroach cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca.key" create-client testuser

# Tenant certs
dir_mt="${dir_n}/mt"
rm -f "${dir_mt}"/*.{crt,key}
./cockroach mt cert --certs-dir="${dir_mt}" --ca-key="${dir_mt}/ca-client-tenant.key" create-tenant-client-ca
./cockroach mt cert --certs-dir="${dir_mt}" --ca-key="${dir_mt}/ca-client-tenant.key" create-tenant-client 123456789

./cockroach mt cert --certs-dir="${dir_mt}" --ca-key="${dir_mt}/ca-server-tenant.key" create-tenant-server-ca
./cockroach mt cert --certs-dir="${dir_mt}" --ca-key="${dir_mt}/ca-server-tenant.key" create-tenant-server 127.0.0.1 ::1 localhost *.local

make generate PKG=./pkg/security/securitytest
