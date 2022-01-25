// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigsqltranslator

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestProtectedTimestampStateReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	mkRecordAndAddToState := func(state *ptpb.State, ts hlc.Timestamp, target *ptpb.Target) {
		recordID := uuid.MakeV4()
		rec := jobsprotectedts.MakeRecord(recordID, int64(1), ts, nil, /* deprecatedSpans */
			jobsprotectedts.Jobs, target)
		state.Records = append(state.Records, *rec)
	}

	protectSchemaObject := func(state *ptpb.State, ts hlc.Timestamp, ids []descpb.ID) {
		mkRecordAndAddToState(state, ts, ptpb.MakeSchemaObjectsTarget(ids))
	}

	protectCluster := func(state *ptpb.State, ts hlc.Timestamp) {
		mkRecordAndAddToState(state, ts, ptpb.MakeClusterTarget())
	}

	protectTenants := func(state *ptpb.State, ts hlc.Timestamp, ids []roachpb.TenantID) {
		mkRecordAndAddToState(state, ts, ptpb.MakeTenantsTarget(ids))
	}

	ts := func(seconds int) hlc.Timestamp {
		return hlc.Timestamp{WallTime: (time.Duration(seconds) * time.Second).Nanoseconds()}
	}

	// Create some ptpb.State and then run the ProtectedTimestampStateReader on it
	// to ensure it outputs the expected records.
	state := &ptpb.State{}
	protectSchemaObject(state, ts(1), []descpb.ID{56})
	protectSchemaObject(state, ts(2), []descpb.ID{56, 57})
	protectCluster(state, ts(3))
	protectTenants(state, ts(4), []roachpb.TenantID{roachpb.MakeTenantID(1)})
	protectTenants(state, ts(5), []roachpb.TenantID{roachpb.MakeTenantID(2)})
	protectTenants(state, ts(6), []roachpb.TenantID{roachpb.MakeTenantID(2)})

	ptsStateReader := newProtectedTimestampStateReader(context.Background(), *state)
	clusterTimestamps := ptsStateReader.getProtectionPoliciesForCluster()
	require.Len(t, clusterTimestamps, 1)
	require.Equal(t, []roachpb.ProtectionPolicy{{ProtectedTimestamp: ts(3)}}, clusterTimestamps)

	tenantTimestamps := ptsStateReader.getProtectionPoliciesForTenants()
	sort.Slice(tenantTimestamps, func(i, j int) bool {
		return tenantTimestamps[i].tenantID.ToUint64() < tenantTimestamps[j].tenantID.ToUint64()
	})
	require.Len(t, tenantTimestamps, 2)
	require.Equal(t, []tenantProtectedTimestamps{
		{
			tenantID:    roachpb.MakeTenantID(1),
			protections: []roachpb.ProtectionPolicy{{ProtectedTimestamp: ts(4)}},
		},
		{
			tenantID: roachpb.MakeTenantID(2),
			protections: []roachpb.ProtectionPolicy{{ProtectedTimestamp: ts(5)},
				{ProtectedTimestamp: ts(6)}},
		},
	}, tenantTimestamps)

	tableTimestamps := ptsStateReader.getProtectionPoliciesForSchemaObject(56)
	sort.Slice(tableTimestamps, func(i, j int) bool {
		return tableTimestamps[i].ProtectedTimestamp.Less(tableTimestamps[j].ProtectedTimestamp)
	})
	require.Len(t, tableTimestamps, 2)
	require.Equal(t, []roachpb.ProtectionPolicy{{ProtectedTimestamp: ts(1)},
		{ProtectedTimestamp: ts(2)}}, tableTimestamps)

	tableTimestamps2 := ptsStateReader.getProtectionPoliciesForSchemaObject(57)
	require.Len(t, tableTimestamps2, 1)
	require.Equal(t, []roachpb.ProtectionPolicy{{ProtectedTimestamp: ts(2)}}, tableTimestamps2)
}
