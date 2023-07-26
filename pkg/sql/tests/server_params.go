// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
)

// CreateTestServerParams creates a set of params suitable for SQL tests. It
// enables some EndTxn sanity checking and installs a flexible
// TestingEvalFilter.
// TODO(andrei): this function is not used consistently by SQL tests. Figure out
// if the EndTxn checks are important.
func CreateTestServerParams() (base.TestServerArgs, *CommandFilters) {
	var cmdFilters CommandFilters
	params := base.TestServerArgs{}
	// Disable the default test tenant as limits to the number of spans in a
	// secondary tenant cause this test to fail. Tracked with #76378.
	params.DefaultTestTenant = base.TODOTestTenantDisabled
	params.Knobs.SQLStatsKnobs = sqlstats.CreateTestingKnobs()
	params.Knobs.Store = &kvserver.StoreTestingKnobs{
		EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
			TestingEvalFilter: cmdFilters.RunFilters,
		},
	}
	return params, &cmdFilters
}

// CreateTestTenantParams creates a set of params suitable for SQL Tenant Tests.
func CreateTestTenantParams(tenantID roachpb.TenantID) base.TestTenantArgs {
	return base.TestTenantArgs{
		TenantID: tenantID,
		TestingKnobs: base.TestingKnobs{
			SQLStatsKnobs: sqlstats.CreateTestingKnobs(),
		},
	}
}
