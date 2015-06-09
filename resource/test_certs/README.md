** Test certificates directory **

Contains the following files:

* ca.crt: CA certificate
* ca.key: CA private key
* node.crt: server certificate
* node.key: server private key

For a human-readable version of the certificate, run:
```bash
openssl x509 -in node.crt -text
```

To regenerate:
```bash
rm -f resource/test_certs/{ca,node}.{crt,key}
cockroach cert --certs=resource/test_certs create-ca --key-size=512
cockroach cert --certs=resource/test_certs create-node --key-size=512 127.0.0.1 localhost $(seq -f "roach%g.local" 0 99)
go generate security/securitytest/securitytest.go
```
