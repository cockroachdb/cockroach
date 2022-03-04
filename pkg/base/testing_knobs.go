// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package base

// ModuleTestingKnobs is an interface for testing knobs for a submodule.
type ModuleTestingKnobs interface {
	// ModuleTestingKnobs is a dummy function.
	ModuleTestingKnobs()
}

// TestingKnobs contains facilities for controlling various parts of the
// system for testing.
type TestingKnobs struct {
	Store                        ModuleTestingKnobs
	KVClient                     ModuleTestingKnobs
	RangeFeed                    ModuleTestingKnobs
	SQLExecutor                  ModuleTestingKnobs
	SQLLeaseManager              ModuleTestingKnobs
	SQLSchemaChanger             ModuleTestingKnobs
	SQLDeclarativeSchemaChanger  ModuleTestingKnobs
	SQLTypeSchemaChanger         ModuleTestingKnobs
	GCJob                        ModuleTestingKnobs
	PGWireTestingKnobs           ModuleTestingKnobs
	StartupMigrationManager      ModuleTestingKnobs
	DistSQL                      ModuleTestingKnobs
	SQLEvalContext               ModuleTestingKnobs
	NodeLiveness                 ModuleTestingKnobs
	Server                       ModuleTestingKnobs
	TenantTestingKnobs           ModuleTestingKnobs
	JobsTestingKnobs             ModuleTestingKnobs
	BackupRestore                ModuleTestingKnobs
	TTL                          ModuleTestingKnobs
	Streaming                    ModuleTestingKnobs
	MigrationManager             ModuleTestingKnobs
	IndexUsageStatsKnobs         ModuleTestingKnobs
	SQLStatsKnobs                ModuleTestingKnobs
	SpanConfig                   ModuleTestingKnobs
	SQLLivenessKnobs             ModuleTestingKnobs
	TelemetryLoggingKnobs        ModuleTestingKnobs
	DialerKnobs                  ModuleTestingKnobs
	ProtectedTS                  ModuleTestingKnobs
	CapturedIndexUsageStatsKnobs ModuleTestingKnobs
	AdmissionControl             ModuleTestingKnobs
}
