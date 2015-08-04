** Test certificates directory **

Contains the following files:

* ca.crt: CA certificate
* ca.key: CA private key
* node.client.crt: node client certificate
* node.client.key: node client private key
* node.server.crt: node server certificate
* node.server.key: node server private key
* root.client.crt: admin client certificate
* root.client.key: admin client private key
* testuser.client.crt: testing user certificate
* testuser.client.key: testing user private key
*

For a human-readable version of the certificate, run:
```bash
openssl x509 -in node.server.crt -text
```

To regenerate:
```bash
rm -f resource/test_certs/*.{crt,key}
cockroach cert --certs=resource/test_certs create-ca --key-size=512
cockroach cert --certs=resource/test_certs create-node --key-size=512 127.0.0.1 localhost $(seq -f "roach%g.local" 0 99)
cockroach cert --certs=resource/test_certs create-client --key-size=512 root
cockroach cert --certs=resource/test_certs create-client --key-size=512 testuser
go generate security/securitytest/securitytest.go
```
