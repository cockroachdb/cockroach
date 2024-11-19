#!/bin/sh

# Copyright 2020 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -eux

dir_n="pkg/security/securitytest/test_certs"
rm -f "${dir_n}"/*.{crt,key}
./cockroach cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca.key" create-ca
./cockroach cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca.key" create-node 127.0.0.1 ::1 localhost *.local

# Create client certs with tenant scopes.
./cockroach cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca.key" create-client root
./cockroach cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca.key" create-client testuser
./cockroach cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca.key" create-client testuser2
./cockroach cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca.key" create-client testuser3

# Tenant certs
./cockroach mt cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca-client-tenant.key" create-tenant-client-ca
for id in 2 10 11 20; do
./cockroach mt cert --certs-dir="${dir_n}" --ca-key="${dir_n}/ca-client-tenant.key" create-tenant-client "${id}" 127.0.0.1 ::1 localhost *.local
./cockroach mt cert --certs-dir="${dir_n}" create-tenant-signing "${id}"
done

# Below section is all OpenSSL commands to generate the certs that require more options than
# what "cockroach cert" supports.
touch index.txt; echo '01' > serial.txt

# san dns: only client certs
openssl genrsa -out ${dir_n}/client.testuser_san_only.key
openssl req -new  -key ${dir_n}/client.testuser_san_only.key -out ${dir_n}/client.testuser_san_only.csr -batch -addext "subjectAltName = DNS:testuser"
openssl ca -config ${dir_n}/ca.cnf -keyfile ${dir_n}/ca.key -cert ${dir_n}/ca.crt -policy signing_policy -extensions signing_client_req -out ${dir_n}/client.testuser_san_only.crt -outdir . -in ${dir_n}/client.testuser_san_only.csr -batch
rm ${dir_n}/client.testuser_san_only.csr

# CN only client certs
openssl genrsa -out ${dir_n}/client.testuser_cn_only.key
openssl req -new  -key ${dir_n}/client.testuser_cn_only.key -out ${dir_n}/client.testuser_cn_only.csr -batch -subj /CN=testuser
openssl ca -config ${dir_n}/ca.cnf -keyfile ${dir_n}/ca.key -cert ${dir_n}/ca.crt -policy signing_policy -extensions signing_client_req -out ${dir_n}/client.testuser_cn_only.crt -outdir . -in ${dir_n}/client.testuser_cn_only.csr -batch
rm ${dir_n}/client.testuser_cn_only.csr

# CN and san dns: client certs
openssl genrsa -out ${dir_n}/client.testuser_cn_and_san.key
openssl req -new  -key ${dir_n}/client.testuser_cn_and_san.key -out ${dir_n}/client.testuser_cn_and_san.csr -batch -subj /CN=testuser -addext "subjectAltName = DNS:testuser"
openssl ca -config ${dir_n}/ca.cnf  -keyfile ${dir_n}/ca.key -cert ${dir_n}/ca.crt -policy signing_policy -extensions signing_client_req -out ${dir_n}/client.testuser_cn_and_san.crt -outdir . -in ${dir_n}/client.testuser_cn_and_san.csr -batch
rm ${dir_n}/client.testuser_cn_and_san.csr

make generate PKG=./pkg/security/securitytest


