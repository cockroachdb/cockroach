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
cockroach cert --certs=resource/test_certs create-ca
cockroach cert --certs=resource/test_certs create-node 127.0.0.1 localhost $(seq -f "roach%g.local" 0 99)
```
