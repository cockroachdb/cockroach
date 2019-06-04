// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package base

// ModuleTestingKnobs is an interface for testing knobs for a submodule.
type ModuleTestingKnobs interface {
	// ModuleTestingKnobs is a dummy function.
	ModuleTestingKnobs()
}

// TestingKnobs contains facilities for controlling various parts of the
// system for testing.
type TestingKnobs struct {
	Store               ModuleTestingKnobs
	KVClient            ModuleTestingKnobs
	SQLExecutor         ModuleTestingKnobs
	SQLLeaseManager     ModuleTestingKnobs
	SQLSchemaChanger    ModuleTestingKnobs
	PGWireTestingKnobs  ModuleTestingKnobs
	SQLMigrationManager ModuleTestingKnobs
	DistSQL             ModuleTestingKnobs
	SQLEvalContext      ModuleTestingKnobs
	RegistryLiveness    ModuleTestingKnobs
	Server              ModuleTestingKnobs
}
