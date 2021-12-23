// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server_test

import (
	"context"
	"fmt"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestStatusLocalStacks verifies that goroutine stack traces are available
// via the /_status/stacks/local endpoint.
func TestStatusLocalStacks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRaceWithIssue(t, 74133)

	ctx := context.Background()
	var args base.TestClusterArgs
	tc := testcluster.StartTestCluster(t, 1 /* nodes */, args)
	defer tc.Stopper().Stop(ctx)

	cc, err := tc.GetStatusClient(ctx, t, 0 /* idx */)
	require.NoError(t, err)

	testCases := []struct {
		stackType serverpb.StacksType
		re        *regexp.Regexp
	}{
		{serverpb.StacksType_GOROUTINE_STACKS, regexp.MustCompile("(?s)goroutine [0-9]+.*goroutine [0-9]+.*")},
		// At least `labels: {"pebble":"table-cache"}` and `labels: {"pebble":"wal-sync"}`
		// should be present.
		{serverpb.StacksType_GOROUTINE_STACKS_DEBUG_1, regexp.MustCompile("(?s)labels: {.*labels: {.*")},
	}

	for _, tt := range testCases {
		t.Run(fmt.Sprintf("debug=%s", tt.stackType), func(t *testing.T) {
			var stacks serverpb.JSONResponse
			for _, nodeID := range []string{"local", "1"} {
				request := serverpb.StacksRequest{
					NodeId: nodeID, Type: tt.stackType,
				}
				response, err := cc.Stacks(context.Background(), &request)
				if err != nil {
					t.Fatal(err)
				}
				if !tt.re.Match(response.Data) {
					t.Errorf("expected %s to match %s", stacks.Data, tt.re)
				}
			}
		})
	}
}
