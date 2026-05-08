** Test certificates directory **

Test certificates are now generated programmatically at runtime by
`pkg/security/securitytest/certgen.go`. This eliminates the need for static
certificate files that expire periodically and require manual regeneration.

The following certificates are generated on first access:

* ca.crt/key: CA certificate
* ca-client-tenant.crt/key: tenant CA certificate
* node.crt/key: node client/server certificate
* client.root.crt/key: admin client certificate
* client.testuser.crt/key: testing user certificate
* client.testuser2.crt/key: testing user 2 certificate
* client.testuser3.crt/key: testing user 3 certificate
* client.testuser_cn_only.crt/key: client cert with CommonName only (no SAN)
* client.testuser_san_only.crt/key: client cert with DNS SAN only (no CN)
* client.testuser_cn_and_san.crt/key: client cert with both CN and DNS SAN
* client-tenant.<ID>.crt/key: tenant client certificates (see EmbeddedTenantIDs in testcerts.go)
* tenant-signing.<ID>.crt/key: tenant signing certificates (see EmbeddedTenantIDs in testcerts.go)

See `certgen.go` for implementation details.
