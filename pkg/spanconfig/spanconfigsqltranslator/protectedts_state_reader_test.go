// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigsqltranslator_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/partitionccl"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigsqltranslator"
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

	// Create some ptpb.State and then run the ProtectedTimestampStateReader on it
	// to ensure it outputs the expected records.
	state := &ptpb.State{}
	protectSchemaObject(state, hlc.Timestamp{WallTime: 1}, []descpb.ID{56})
	protectSchemaObject(state, hlc.Timestamp{WallTime: 2}, []descpb.ID{56, 57})
	protectCluster(state, hlc.Timestamp{WallTime: 3})
	protectTenants(state, hlc.Timestamp{WallTime: 4}, []roachpb.TenantID{roachpb.MakeTenantID(1)})
	protectTenants(state, hlc.Timestamp{WallTime: 5}, []roachpb.TenantID{roachpb.MakeTenantID(2)})
	protectTenants(state, hlc.Timestamp{WallTime: 6}, []roachpb.TenantID{roachpb.MakeTenantID(2)})

	ptsStateReader, err := spanconfigsqltranslator.TestingNewProtectedTimestampStateReader(context.Background(), *state)
	require.NoError(t, err)
	require.Equal(t, `[ts=3]`, outputTimestamps(ptsStateReader.GetProtectedTimestampsForCluster()))
	require.Equal(t, `tenant_id=1 [ts=4]
tenant_id=2 [ts=5 ts=6]`, outputTenants(ptsStateReader.GetProtectedTimestampsForTenants()))

	require.Equal(t, `[ts=1 ts=2]`,
		outputTimestamps(ptsStateReader.GetProtectedTimestampsForSchemaObject(56)))
	require.Equal(t, `[ts=2]`,
		outputTimestamps(ptsStateReader.GetProtectedTimestampsForSchemaObject(57)))
}

// Returns the spanconfigsqltranslator.TenantProtectedTimestamps formatted as:
//
// tenant_id=<int> [ts=<int>, ts=<int>...]
func outputTenants(tenantProtections []spanconfigsqltranslator.TenantProtectedTimestamps) string {
	sort.Slice(tenantProtections, func(i, j int) bool {
		return tenantProtections[i].TenantID.ToUint64() < tenantProtections[j].TenantID.ToUint64()
	})
	var output strings.Builder
	for i, p := range tenantProtections {
		output.WriteString(fmt.Sprintf("tenant_id=%d %s", p.TenantID.ToUint64(),
			outputTimestamps(p.Protections)))
		if i != len(tenantProtections)-1 {
			output.WriteString("\n")
		}
	}
	return output.String()
}

// Returns the timestamps formatted as:
//
// [ts=<int>, ts=<int>...]
func outputTimestamps(timestamps []hlc.Timestamp) string {
	sort.Slice(timestamps, func(i, j int) bool {
		return timestamps[i].Less(timestamps[j])
	})

	var output strings.Builder
	output.WriteString(fmt.Sprintf("[%s]", func() string {
		var s strings.Builder
		for i, pts := range timestamps {
			s.WriteString(fmt.Sprintf("ts=%d", int(pts.WallTime)))
			if i != len(timestamps)-1 {
				s.WriteString(" ")
			}
		}
		return s.String()
	}()))
	return output.String()
}
