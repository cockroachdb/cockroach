** Test certificates directory **

Contains the following files:

* ca.crt: CA certificate
* ca.key: CA private key
* node.crt: node client/server certificate
* node.key: node client/server private key
* root.crt: admin client certificate
* root.key: admin client private key
* testuser.crt: testing user certificate
* testuser.key: testing user private key
*

For a human-readable version of the certificate, run:
```bash
openssl x509 -in node.crt -text
```

To regenerate:
```bash
rm -f resource/test_certs/*.{crt,key}
./cockroach cert --ca-cert=resource/test_certs/ca.crt --ca-key=resource/test_certs/ca.key create-ca
./cockroach cert --ca-cert=resource/test_certs/ca.crt --ca-key=resource/test_certs/ca.key --cert=resource/test_certs/node.crt --key=resource/test_certs/node.key create-node 127.0.0.1 ::1 localhost *.local
./cockroach cert --ca-cert=resource/test_certs/ca.crt --ca-key=resource/test_certs/ca.key --cert=resource/test_certs/root.crt --key=resource/test_certs/root.key create-client root
./cockroach cert --ca-cert=resource/test_certs/ca.crt --ca-key=resource/test_certs/ca.key --cert=resource/test_certs/testuser.crt --key=resource/test_certs/testuser.key create-client testuser
go generate security/securitytest/securitytest.go
```
