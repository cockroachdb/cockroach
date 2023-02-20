// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/redact"
)

func TestAssertNoCmdClosedTimestampRegression(t *testing.T) {
	w := echotest.NewWalker(t, filepath.Join(datapathutils.TestDataPath(t, t.Name())))

	for _, tc := range []struct {
		name         string
		oldCS, curCS hlc.Timestamp
	}{
		{
			name: "noop",
		},
		{
			name:  "pass",
			oldCS: hlc.Timestamp{WallTime: 1},
			curCS: hlc.Timestamp{WallTime: 1},
		},
		{
			name:  "trip",
			oldCS: hlc.Timestamp{WallTime: 2},
			curCS: hlc.Timestamp{WallTime: 1},
		},
	} {
		t.Run(tc.name, w.Run(t, tc.name, func(t *testing.T) string {
			const rangeID = roachpb.RangeID(123)
			eng := storage.NewDefaultInMemForTesting()
			defer eng.Close()
			cmd := &replicatedCmd{ReplicatedCmd: raftlog.ReplicatedCmd{Entry: &raftlog.Entry{
				RaftEntry: raftlog.RaftEntry{
					Term:  1,
					Index: 2,
					Type:  3,
				},
			}}}
			cmd.Cmd.ClosedTimestamp = &tc.curCS
			cmd.ID = "deadbeef"
			state := kvserverpb.ReplicaState{
				RaftClosedTimestamp: tc.oldCS,
			}
			ctSetter := closedTimestampSetterInfo{}
			return string(redact.Sprint(assertNoCmdClosedTimestampRegression(rangeID, eng, cmd, state, ctSetter)))
		}))
	}
}
