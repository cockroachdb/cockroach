** Test certificates directory **

Contains the following files:

* ca.crt: CA certificate
* ca.key: CA private key
* node.crt: RPC client/server certificate
* node.key: RPC client/server private key
* sql-server.crt: SQL server certificate
* sql-server.key: SQL server private key
* client.root.crt: admin client certificate
* client.root.key: admin client private key
* client.testuser.crt: testing user certificate
* client.testuser.key: testing user private key
* client.testuser2.crt: testing user 2 certificate
* client.testuser2.key: testing user 2 private key
* ca-client-tenant.crt: CA certificate used to sign client-tenant and tenant-signing certs.
* ca-client-tenant.key: CA private key for ca-client-tenant.crt.
* client-tenant.<ID>.crt: tenant client certificate (used to authn to the KV layer)
* client-tenant.<ID>.key: tenant client private key (used to authn to the KV layer)
* tenant-signing.<ID>.crt: tenant signing certificate (used to sign serialized SQL sessions)
* tenant-signing.<ID>.key: tenant signing private key (used to sign serialized SQL sessions)

The per-tenant files include IDs: 10, 11, and 20.

For a human-readable version of the certificate, run:
```bash
openssl x509 -in node.crt -text
```

To regenerate, run `regenerate.sh` from the repo root.
