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
cockroach cert --ssl-ca=resource/test_certs/ca.crt --ssl-ca-key=resource/test_certs/ca.key create-ca
cockroach cert --ssl-ca=resource/test_certs/ca.crt --ssl-ca-key=resource/test_certs/ca.key --ssl-cert=resource/test_certs/node.crt --ssl-cert-key=resource/test_certs/node.key create-node 127.0.0.1 localhost $(seq -f "roach%g.local" 0 99)
cockroach cert --ssl-ca=resource/test_certs/ca.crt --ssl-ca-key=resource/test_certs/ca.key --ssl-cert=resource/test_certs/root.crt --ssl-cert-key=resource/test_certs/root.key create-client root
cockroach cert --ssl-ca=resource/test_certs/ca.crt --ssl-ca-key=resource/test_certs/ca.key --ssl-cert=resource/test_certs/testuser.crt --ssl-cert-key=resource/test_certs/testuser.key create-client testuser
go generate security/securitytest/securitytest.go
```
