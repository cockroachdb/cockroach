// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage_api_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestRaftDebug(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	var resp serverpb.RaftDebugResponse
	if err := srvtestutils.GetStatusJSONProto(s, "raft", &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Ranges) == 0 {
		t.Errorf("didn't get any ranges")
	}

	if len(resp.Ranges) < 3 {
		t.Errorf("expected more than 2 ranges, got %d", len(resp.Ranges))
	}

	reqURI := "raft"
	requestedIDs := []roachpb.RangeID{}
	for id := range resp.Ranges {
		if len(requestedIDs) == 0 {
			reqURI += "?"
		} else {
			reqURI += "&"
		}
		reqURI += fmt.Sprintf("range_ids=%d", id)
		requestedIDs = append(requestedIDs, id)
		if len(requestedIDs) >= 2 {
			break
		}
	}

	if err := srvtestutils.GetStatusJSONProto(s, reqURI, &resp); err != nil {
		t.Fatal(err)
	}

	// Make sure we get exactly two ranges back.
	if len(resp.Ranges) != 2 {
		t.Errorf("expected exactly two ranges in response, got %d", len(resp.Ranges))
	}

	// Make sure the ranges returned are those requested.
	for _, reqID := range requestedIDs {
		if _, ok := resp.Ranges[reqID]; !ok {
			t.Errorf("request URI was %s, but range ID %d not returned: %+v", reqURI, reqID, resp.Ranges)
		}
	}
}
