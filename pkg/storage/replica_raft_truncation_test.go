// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storage

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/testutils/datadriven"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestHandleTruncatedStateBelowRaft(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// This test verifies the expected behavior of the downstream-of-Raft log
	// truncation code, in particular regarding the migration below:
	_ = cluster.VersionUnreplicatedRaftTruncatedState

	ctx := context.Background()

	// neither exists (migration)
	// old one exists (no migration)
	// new one exists (migrated already)
	// truncstate regresses

	var prevTruncatedState roachpb.RaftTruncatedState
	datadriven.Walk(t, "testdata/truncated_state_migration", func(t *testing.T, path string) {
		const rangeID = 12
		loader := stateloader.Make(rangeID)
		eng := engine.NewInMem(roachpb.Attributes{}, 1<<20)
		defer eng.Close()

		datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "prev":
				d.ScanArgs(t, "index", &prevTruncatedState.Index)
				d.ScanArgs(t, "term", &prevTruncatedState.Term)
				return ""
			case "put":
				var index uint64
				var term uint64
				var legacy bool
				d.ScanArgs(t, "index", &index)
				d.ScanArgs(t, "term", &term)
				d.ScanArgs(t, "legacy", &legacy)

				truncState := &roachpb.RaftTruncatedState{
					Index: index,
					Term:  term,
				}

				if legacy {
					assert.NoError(t, loader.SetLegacyRaftTruncatedState(ctx, eng, nil, truncState))
				} else {
					assert.NoError(t, loader.SetRaftTruncatedState(ctx, eng, truncState))
				}
				return ""
			case "handle":
				var buf bytes.Buffer

				var index uint64
				var term uint64
				d.ScanArgs(t, "index", &index)
				d.ScanArgs(t, "term", &term)

				newTruncatedState := &roachpb.RaftTruncatedState{
					Index: index,
					Term:  term,
				}

				apply, err := handleTruncatedStateBelowRaft(ctx, &prevTruncatedState, newTruncatedState, loader, eng)
				if err != nil {
					return err.Error()
				}
				fmt.Fprintf(&buf, "apply: %t\n", apply)

				for _, key := range []roachpb.Key{
					keys.RaftTruncatedStateLegacyKey(rangeID),
					keys.RaftTruncatedStateKey(rangeID),
				} {
					var truncatedState roachpb.RaftTruncatedState
					ok, err := engine.MVCCGetProto(ctx, eng, key, hlc.Timestamp{}, &truncatedState, engine.MVCCGetOptions{})
					if err != nil {
						t.Fatal(err)
					}
					if !ok {
						continue
					}
					fmt.Fprintf(&buf, "%s -> index=%d term=%d\n", key, truncatedState.Index, truncatedState.Term)
				}
				return buf.String()
			default:
			}
			return fmt.Sprintf("unsupported: %s", d.Cmd)
		})
	})
}
