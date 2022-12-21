// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package certnames

// EmbeddedCertsDir is the certs directory inside embedded assets.
// Embedded*{Cert,Key} are the filenames for embedded certs.
const (
	EmbeddedCertsDir        = "test_certs"
	EmbeddedCACert          = "ca.crt"
	EmbeddedCAKey           = "ca.key"
	EmbeddedClientCACert    = "ca-client.crt"
	EmbeddedClientCAKey     = "ca-client.key"
	EmbeddedSQLServerCACert = "ca-sql.crt"
	EmbeddedSQLServerCAKey  = "ca-sql.key"
	EmbeddedUICACert        = "ca-ui.crt"
	EmbeddedUICAKey         = "ca-ui.key"
	EmbeddedNodeCert        = "node.crt"
	EmbeddedNodeKey         = "node.key"
	EmbeddedSQLServerCert   = "sql-server.crt"
	EmbeddedSQLServerKey    = "sql-server.key"
	EmbeddedRootCert        = "client.root.crt"
	EmbeddedRootKey         = "client.root.key"
	EmbeddedTestUserCert    = "client.testuser.crt"
	EmbeddedTestUserKey     = "client.testuser.key"
)

// Embedded certificates specific to multi-tenancy testing. This CA
// cert is used to sign `client-tenant.NN.crt` certs, used to
// authenticate tenant servers to the KV layer and other tenant
// servers.
const (
	EmbeddedTenantKVClientCACert = "ca-client-tenant.crt"
	EmbeddedTenantKVClientCAKey  = "ca-client-tenant.key"
)
