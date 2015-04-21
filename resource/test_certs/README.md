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
