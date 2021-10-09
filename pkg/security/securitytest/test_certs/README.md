** Test certificates directory **

Contains the following files:

* ca.crt: CA certificate
* ca.key: CA private key
* node.crt: node client/server certificate
* node.key: node client/server private key
* sql-node.crt: SQL tenant client/server certificate for pod-to-pod comms
* sql-node.key: SQL tenant client/server private key for pod-to-pod comms
* client.root.crt: admin client certificate
* client.root.key: admin client private key
* client.testuser.crt: testing user certificate
* client.testuser.key: testing user private key
* client.testuser2.crt: testing user 2 certificate
* client.testuser2.key: testing user 2 private key
*

For a human-readable version of the certificate, run:
```bash
openssl x509 -in node.crt -text
```

To regenerate, run `regenerate.sh` from the repo root.
