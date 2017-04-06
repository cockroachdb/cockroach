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
./cockroach cert --ca-cert=pkg/security/securitytest/test_certs/ca.crt --ca-key=pkg/security/securitytest/test_certs/ca.key create-ca
./cockroach cert --ca-cert=pkg/security/securitytest/test_certs/ca.crt --ca-key=pkg/security/securitytest/test_certs/ca.key --cert=pkg/security/securitytest/test_certs/node.crt --key=pkg/security/securitytest/test_certs/node.key create-node 127.0.0.1 ::1 localhost *.local
./cockroach cert --ca-cert=pkg/security/securitytest/test_certs/ca.crt --ca-key=pkg/security/securitytest/test_certs/ca.key --cert=pkg/security/securitytest/test_certs/client.root.crt --key=pkg/security/securitytest/test_certs/client.root.key create-client root
./cockroach cert --ca-cert=pkg/security/securitytest/test_certs/ca.crt --ca-key=pkg/security/securitytest/test_certs/ca.key --cert=pkg/security/securitytest/test_certs/client.testuser.crt --key=pkg/security/securitytest/test_certs/client.testuser.key create-client testuser
make generate ./pkg/security/securitytest
```
