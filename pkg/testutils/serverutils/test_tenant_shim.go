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

import "context"

// TestTenantInterface defines test tenant functionality that tests need; it is
// implemented by server.TestTenant.
type TestTenantInterface interface {
	// SQLAddr returns the tenant's SQL address.
	SQLAddr() string

	// HTTPAddr returns the tenant's http address.
	HTTPAddr() string

	// SQLServer returns the *sql.Server as an interface{}.
	SQLServer() interface{}

	// ReportDiagnostics phones home to report diagnostics.
	//
	// If using this for testing, consider setting DiagnosticsReportingEnabled to
	// false so the periodic reporting doesn't interfere with the test.
	//
	// This can be slow because of cloud detection; use cloudinfo.Disable() in
	// tests to avoid that.
	ReportDiagnostics(ctx context.Context)
}
