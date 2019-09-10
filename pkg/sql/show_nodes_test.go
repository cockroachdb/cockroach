// Copyright 2019 The Cockroach Authors.
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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestShowNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numNodes = 3
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	result := sqlDB.QueryStr(t, `SHOW NODES`)
	tctx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	// The timestamp output is formatted differently than tree.TimestampOutputFormat
	const TimeParseFormat = "2006-01-02 15:04:05.999999 +0000 +0000"

	const idIdx = 0
	const addrIdx = 1
	const sqlAddrIdx = 2
	const buildIdx = 3
	const startedAtIdx = 4
	const localityIdx = 6
	const isAvailableIdx = 7
	const isLiveIdx = 8
	for i, row := range result {
		nodeDesc, err := tc.Servers[i].Gossip().GetNodeDescriptor(roachpb.NodeID(i + 1))
		if err != nil {
			t.Fatal(err)
		}

		if row[idIdx] != fmt.Sprintf("%d", (i+1)) {
			t.Fatalf("expected %d found %s for node_id", i+1, row[idIdx])
		}

		if row[addrIdx] != tc.Servers[i].ServingRPCAddr() {
			t.Fatalf("expected address for node %d to have address %s but found %s", i+1, tc.Servers[i].ServingRPCAddr(), row[addrIdx])
		}

		if row[sqlAddrIdx] != tc.Servers[i].ServingSQLAddr() {
			t.Fatalf("expected sql address for node %d to have address %s but found %s", i+1, tc.Servers[i].ServingSQLAddr(), row[addrIdx])
		}

		if row[buildIdx] != nodeDesc.BuildTag {
			t.Fatalf("expected build tag for node %d to be %s but found %s", i+1, nodeDesc.BuildTag, row[buildIdx])
		}

		startedAt := tree.MakeDTimestampTZ(timeutil.Unix(0, nodeDesc.StartedAt), time.Microsecond)
		queryStartedAtTime, err := time.Parse(TimeParseFormat, row[startedAtIdx])
		if err != nil {
			t.Fatal(err)
		}
		queryStartedAt := tree.MakeDTimestamp(queryStartedAtTime, time.Microsecond)
		if queryStartedAt.Compare(tctx, startedAt) != 0 {
			t.Fatalf("expected start time for node %d to be %s but found %s", i+1, startedAt.String(), queryStartedAt.String())
		}

		// TODO (rohany): It seems unsafe to try and test the value of updated_at -- getting the "correct" value requires looking at
		// the gossip k/v information. However, this information could change between when the SHOW NODES query is run and when
		// the validation here is performed.

		if row[localityIdx] != nodeDesc.Locality.String() {
			t.Fatalf("expected locality for node %d to be %s but found %s", i+1, nodeDesc.Locality.String(), row[localityIdx])
		}

		if row[isAvailableIdx] != "true" {
			t.Fatalf("expected is_available for node %d to be true but found %s", i+1, row[isAvailableIdx])
		}

		if row[isLiveIdx] != "true" {
			t.Fatalf("expected is_live for node %d to be true but found %s", i+1, row[isLiveIdx])
		}
	}
}
