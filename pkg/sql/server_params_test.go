// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
)

// createTestServerParamsAllowTenants creates a set of params suitable for SQL tests. It
// enables some EndTxn sanity checking and installs a flexible
// TestingEvalFilter.
// TODO(andrei): this function is not used consistently by SQL tests.
func createTestServerParamsAllowTenants() (base.TestServerArgs, *tests.CommandFilters) {
	var cmdFilters tests.CommandFilters
	params := base.TestServerArgs{}
	// Disable the default test tenant as limits to the number of spans in a
	// secondary tenant cause this test to fail. Tracked with #76378.
	params.Knobs.SQLStatsKnobs = sqlstats.CreateTestingKnobs()
	params.Knobs.Store = &kvserver.StoreTestingKnobs{
		EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
			TestingEvalFilter: cmdFilters.RunFilters,
		},
	}
	return params, &cmdFilters
}

// createTestServerParams creates a set of params suitable for SQL
// tests with randomized tenant testing disabled. New tests should
// prefer createTestServerParamsAllowTenants(). See
// createTestServerParamsAllowTenants for additional details.
//
// TODO(ssd): Rename this and createTestServerParamsAllowTenants once
// disabling tenant testing is less common than enabling it.
func createTestServerParams() (base.TestServerArgs, *tests.CommandFilters) {
	params, cmdFilters := createTestServerParamsAllowTenants()
	params.DefaultTestTenant = base.TODOTestTenantDisabled
	return params, cmdFilters
}
