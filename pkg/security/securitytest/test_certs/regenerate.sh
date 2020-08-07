#!/bin/sh
set -eux

dir_n="pkg/security/securitytest/test_certs"
rm -f "${dir_n}"/*.{crt,key}
./cockroach cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca.key" create-ca
./cockroach cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca.key" create-node 127.0.0.1 ::1 localhost *.local
./cockroach cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca.key" create-client root
./cockroach cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca.key" create-client testuser
./cockroach cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca.key" create-client testuser2

# Tenant certs
./cockroach mt cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca-client-tenant.key" create-tenant-client-ca
for id in 10 11 20; do
./cockroach mt cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca-client-tenant.key" create-tenant-client "${id}"
done

make generate PKG=./pkg/security/securitytest
