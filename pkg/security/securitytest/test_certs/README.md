** Test certificates directory **

Contains the following files:

* ca.crt: CA certificate
* ca.key: CA private key
* node.crt: node client/server certificate
* node.key: node client/server private key
* client.root.crt: admin client certificate
* client.root.key: admin client private key
* client.testuser.crt: testing user certificate
* client.testuser.key: testing user private key
* client.testuser2.crt: testing user 2 certificate
* client.testuser2.key: testing user 2 private key
* client.testuser3.crt: testing user 3 certificate
* client.testuser3.key: testing user 3 private key
* ca-client-tenant.crt: tenant CA certificate
* ca-client-tenant.key: tenant CA private key
* client-tenant.<ID>.crt: tenant client certificate
* client-tenant.<ID>.key: tenant client private key
* tenant-signing.<ID>.crt: tenant signing certificate
* tenant-signing.<ID>.key: tenant signing private key

The per-tenant files include IDs: 10, 11, and 20.

For a human-readable version of the certificate, run:
```bash
openssl x509 -in node.crt -text
```

To regenerate, run `regenerate.sh` from the repo root.
