#!/bin/bash

set -euxo pipefail

killall -9 cockroach || true

rm -rf certs-{sql,kv,user} cockroach-data
mkdir -p certs-{sql,kv,user}
sql="certs-sql"
kv="certs-kv"

#### KV certs

./cockroach cert --certs-dir="${kv}" --ca-key="${kv}/ca.key" create-ca
./cockroach cert --certs-dir="${kv}" --ca-key="${kv}/ca.key" create-node 127.0.0.1 ::1 localhost *.local
# Used only to create the tenant.
./cockroach cert --certs-dir="${kv}" --ca-key="${kv}/ca.key" create-client root

# We will use ca.crt for ca-server-tenant.crt and use the same CA for everything.
#
# TODO(darinpp): get rid of explicit ca-server-tenant.
ln -s "../${kv}/ca.crt" "${sql}/ca-server-tenant.crt"
ln -s "../${kv}/ca.crt" "${sql}"
ln -s "../${kv}/ca.key" "${sql}"

#### SQL certs

./cockroach cert --certs-dir="${sql}" --ca-key="${sql}/ca.key" create-node 127.0.0.1 ::1 localhost *.local

# TODO(darinpp): will we use a client CA here in prod? If this is also just
# staying with the one CA then we don't need the one-off root user that kv uses
# to create the tenant.
./cockroach cert --certs-dir="${sql}" --ca-key="${sql}/ca.key" create-client root
mv "${sql}"/client.root.* certs-user
ln -s "../${sql}/ca.crt" certs-user


# We will use ca.crt for ca-client-tenant.crt
#
# TODO(darinpp): remove theses step once this all falls back to ca.crt.
ln -s "../${kv}/ca.crt" "${kv}/ca-client-tenant.crt"
ln -s "../${kv}/ca.key" "${kv}/ca-client-tenant.key"
#ln -s "../${sql}/ca-client-tenant.crt" "${kv}"
# Awardkly, we have to call create-tenant-client in certs-kv since that's where ca-client-tenant.* lives.
# We then move the created certs to certs-sql.
./cockroach mt cert --certs-dir="${kv}" --ca-key="${kv}/ca-client-tenant.key" create-tenant-client 10
mv "${kv}"/client-tenant.* "${sql}"

#### Start KV and create tenant

./cockroach start-single-node --certs-dir certs-kv --host 127.0.0.1 --background
./cockroach sql --certs-dir certs-kv --host 127.0.0.1 -e 'select crdb_internal.create_tenant(10);'
rm certs-kv/client.root.*

#### Start SQL and create 'bob' using root client certs

./cockroach mt start-sql --certs-dir certs-sql --tenant-id 10 --kv-addrs 127.0.0.1:26257 --sql-addr 127.0.0.1:46257 &
sleep 1

./cockroach sql --url 'postgres://root@127.0.0.1:46257?sslmode=verify-ca&sslkey=certs-user/client.root.key&sslcert=certs-user/client.root.crt&sslrootcert=certs-user/ca.crt' -e "CREATE USER bob WITH PASSWORD 'foo';"


#### Start SQL proxy

./cockroach mt start-proxy --listen-addr 127.0.0.1:56257 --target-addr 127.0.0.1:46257 --listen-cert certs-sql/node.crt --listen-key certs-sql/node.key &
sleep 1

#### Connect as 'bob' through proxy, once using db, once using options.

./cockroach sql --url 'postgres://bob:foo@127.0.0.1:56257/prancing-pony.defaultdb?sslmode=verify-ca&sslrootcert=certs-user/ca.crt' -e "SELECT 'this worked!';"

./cockroach sql --url 'postgres://bob:foo@127.0.0.1:56257?options=prancing-pony&sslmode=verify-ca&sslrootcert=certs-user/ca.crt' -e "SELECT 'that worked!';"

echo "Everything worked! Here are all the CA certs involved:"
find certs-* -type f -name 'ca*'
