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
*

For a human-readable version of the certificate, run:
```bash
openssl x509 -in node.crt -text
```

To regenerate:
```bash
rm -f pkg/security/securitytest/test_certs/*.{crt,key}
./cockroach cert --certs-dir=pkg/security/securitytest/test_certs --ca-key=pkg/security/securitytest/test_certs/ca.key create-ca
./cockroach cert --certs-dir=pkg/security/securitytest/test_certs --ca-key=pkg/security/securitytest/test_certs/ca.key create-node 127.0.0.1 ::1 localhost *.local
./cockroach cert --certs-dir=pkg/security/securitytest/test_certs --ca-key=pkg/security/securitytest/test_certs/ca.key create-client root
./cockroach cert --certs-dir=pkg/security/securitytest/test_certs --ca-key=pkg/security/securitytest/test_certs/ca.key create-client testuser
make generate PKG=./pkg/security/securitytest
```
