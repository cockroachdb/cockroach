// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package base

// ModuleTestingKnobs is an interface for testing knobs for a submodule.
type ModuleTestingKnobs interface {
	// ModuleTestingKnobs is a dummy function.
	ModuleTestingKnobs()
}

// TestingKnobs contains facilities for controlling various parts of the
// system for testing.
type TestingKnobs struct {
	Store                          ModuleTestingKnobs
	KVClient                       ModuleTestingKnobs
	RangeFeed                      ModuleTestingKnobs
	SQLExecutor                    ModuleTestingKnobs
	SQLLeaseManager                ModuleTestingKnobs
	SQLSchemaChanger               ModuleTestingKnobs
	SQLDeclarativeSchemaChanger    ModuleTestingKnobs
	SQLTypeSchemaChanger           ModuleTestingKnobs
	GCJob                          ModuleTestingKnobs
	PGWireTestingKnobs             ModuleTestingKnobs
	DistSQL                        ModuleTestingKnobs
	SQLEvalContext                 ModuleTestingKnobs
	NodeLiveness                   ModuleTestingKnobs
	Server                         ModuleTestingKnobs
	TenantTestingKnobs             ModuleTestingKnobs
	JobsTestingKnobs               ModuleTestingKnobs
	BackupRestore                  ModuleTestingKnobs
	TTL                            ModuleTestingKnobs
	SchemaTelemetry                ModuleTestingKnobs
	Streaming                      ModuleTestingKnobs
	UpgradeManager                 ModuleTestingKnobs
	IndexUsageStatsKnobs           ModuleTestingKnobs
	SQLStatsKnobs                  ModuleTestingKnobs
	SpanConfig                     ModuleTestingKnobs
	SQLLivenessKnobs               ModuleTestingKnobs
	TelemetryLoggingKnobs          ModuleTestingKnobs
	DialerKnobs                    ModuleTestingKnobs
	ProtectedTS                    ModuleTestingKnobs
	CapturedIndexUsageStatsKnobs   ModuleTestingKnobs
	AdmissionControlOptions        ModuleTestingKnobs // TODO(irfansharif): Remove.
	AdmissionControl               ModuleTestingKnobs
	RaftTransport                  ModuleTestingKnobs
	UnusedIndexRecommendKnobs      ModuleTestingKnobs
	ExternalConnection             ModuleTestingKnobs
	EventExporter                  ModuleTestingKnobs
	EventLog                       ModuleTestingKnobs
	LOQRecovery                    ModuleTestingKnobs
	KeyVisualizer                  ModuleTestingKnobs
	TenantCapabilitiesTestingKnobs ModuleTestingKnobs
	TableStatsKnobs                ModuleTestingKnobs
	Insights                       ModuleTestingKnobs
	TableMetadata                  ModuleTestingKnobs
	LicenseTestingKnobs            ModuleTestingKnobs
	VecIndexTestingKnobs           ModuleTestingKnobs
}
