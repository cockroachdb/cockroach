#!/bin/bash

set -euxo pipefail

killall -9 cockroach || true

rm -rf certs-{sql,kv,user} cockroach-data
mkdir -p certs-{sql,kv,user}
sql="certs-sql"
kv="certs-kv"
cakey="${kv}/ca.key"

#### KV certs

./cockroach cert --certs-dir="${kv}" --ca-key="${cakey}" create-ca
./cockroach cert --certs-dir="${kv}" --ca-key="${cakey}" create-node 127.0.0.1 ::1 localhost *.local
# Used only to create the tenant.
# ./cockroach cert --certs-dir="${kv}" --ca-key="${cakey}" create-client root


#### SQL tenant certs

ln -s "../${kv}/ca.crt" "${sql}"
./cockroach cert --certs-dir="${sql}" --ca-key="${cakey}" create-node 127.0.0.1 ::1 localhost *.local
# TODO(darinpp): remove this step once tenant client CA falls back to ca.crt.
ln -s "../${sql}/ca.crt" "${sql}/ca-client-tenant.crt"
./cockroach mt cert --certs-dir="${sql}" --ca-key="${cakey}" create-tenant-client 10

#### End-user certs
./cockroach cert --certs-dir="${sql}" --ca-key="${cakey}" create-client root
mv "${sql}"/client.root.* certs-user
ln -s "../${sql}/ca.crt" certs-user

#### Start KV and create tenant

./cockroach start-single-node --certs-dir certs-kv --host 127.0.0.1 --background
./cockroach sql --certs-dir certs-user --host 127.0.0.1 -e 'select crdb_internal.create_tenant(10);'

#### Start SQL and create 'bob' using root client certs

./cockroach mt start-sql --certs-dir certs-sql --tenant-id 10 --kv-addrs 127.0.0.1:26257 --sql-addr 127.0.0.1:46257 &
sleep 1

./cockroach sql --url 'postgres://root@127.0.0.1:46257?sslmode=verify-ca&sslkey=certs-user/client.root.key&sslcert=certs-user/client.root.crt&sslrootcert=certs-user/ca.crt' -e "CREATE USER bob WITH PASSWORD 'foo';"


#### Start SQL proxy

./cockroach mt start-proxy --listen-addr 127.0.0.1:56257 --target-addr 127.0.0.1:46257 --listen-cert certs-sql/node.crt --listen-key certs-sql/node.key &
sleep 1

#### Connect as 'bob' through proxy, once using db, once using options.

./cockroach sql --url 'postgres://bob:foo@127.0.0.1:56257/prancing-pony.defaultdb?sslmode=verify-ca&sslrootcert=certs-user/ca.crt' -e "SELECT 'this worked!';"

./cockroach sql --url 'postgres://bob:foo@127.0.0.1:56257?options=--cluster=prancing-pony&sslmode=verify-ca&sslrootcert=certs-user/ca.crt' -e "SELECT 'that worked!';"

echo "Everything worked! Here are all the CA certs involved:"
find certs-* -type f -name 'ca*'
