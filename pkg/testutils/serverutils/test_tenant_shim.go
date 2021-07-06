// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// This file provides generic interfaces that allow tests to set up test tenants
// without importing the server package (avoiding circular dependencies). This

package serverutils

import "github.com/cockroachdb/cockroach/pkg/base"

// TestTenantInterface defines SQL-only tenant functionality that tests need; it
// is implemented by server.TestTenant.
type TestTenantInterface interface {
	// SQLInstanceID is the ephemeral ID assigned to a running instance of the
	// SQLServer. Each tenant can have zero or more running SQLServer instances.
	SQLInstanceID() base.SQLInstanceID

	// SQLAddr returns the tenant's SQL address.
	SQLAddr() string

	// HTTPAddr returns the tenant's http address.
	HTTPAddr() string

	// PGServer returns the tenant's *pgwire.Server as an interface{}.
	PGServer() interface{}

	// DiagnosticsReporter returns the tenant's *diagnostics.Reporter as an
	// interface{}. The DiagnosticsReporter periodically phones home to report
	// diagnostics and usage.
	DiagnosticsReporter() interface{}

	// StatusServer returns the tenant's *server.SQLStatusServer as an
	// interface{}.
	StatusServer() interface{}

	// DistSQLServer returns the *distsql.ServerImpl as an
	// interface{}.
	DistSQLServer() interface{}

	// JobRegistry returns the *jobs.Registry as an interface{}.
	JobRegistry() interface{}
}
