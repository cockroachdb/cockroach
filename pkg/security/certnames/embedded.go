// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package certnames

// EmbeddedCertsDir is the certs directory inside embedded assets.
// Embedded*{Cert,Key} are the filenames for embedded certs.
const (
	EmbeddedCertsDir     = "test_certs"
	EmbeddedCACert       = "ca.crt"
	EmbeddedCAKey        = "ca.key"
	EmbeddedClientCACert = "ca-client.crt"
	EmbeddedClientCAKey  = "ca-client.key"
	EmbeddedUICACert     = "ca-ui.crt"
	EmbeddedUICAKey      = "ca-ui.key"
	EmbeddedNodeCert     = "node.crt"
	EmbeddedNodeKey      = "node.key"
	EmbeddedRootCert     = "client.root.crt"
	EmbeddedRootKey      = "client.root.key"
	EmbeddedTestUserCert = "client.testuser.crt"
	EmbeddedTestUserKey  = "client.testuser.key"
)

// Embedded certificates specific to multi-tenancy testing.
const (
	EmbeddedTenantCACert = "ca-client-tenant.crt" // CA for client connections
	EmbeddedTenantCAKey  = "ca-client-tenant.key" // CA for client connections
)
