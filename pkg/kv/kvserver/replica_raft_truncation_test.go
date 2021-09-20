// Copyright 2019 The Cockroach Authors.
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
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/assert"
)

func TestHandleTruncatedStateBelowRaft(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test verifies the expected behavior of the downstream-of-Raft log
	// truncation code, in particular regarding the
	// VersionUnreplicatedRaftTruncatedState migration.

	ctx := context.Background()

	// neither exists (migration)
	// old one exists (no migration)
	// new one exists (migrated already)
	// truncstate regresses

	var prevTruncatedState roachpb.RaftTruncatedState
	datadriven.Walk(t, "testdata/truncated_state_migration", func(t *testing.T, path string) {
		const rangeID = 12
		loader := stateloader.Make(rangeID)
		eng := storage.NewDefaultInMemForTesting()
		defer eng.Close()

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
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

				apply, err := handleTruncatedStateBelowRaft(ctx, &prevTruncatedState, newTruncatedState, loader, eng, false)
				if err != nil {
					return err.Error()
				}
				fmt.Fprintf(&buf, "apply: %t\n", apply)

				for _, key := range []roachpb.Key{
					keys.RaftTruncatedStateLegacyKey(rangeID),
					keys.RaftTruncatedStateKey(rangeID),
				} {
					var truncatedState roachpb.RaftTruncatedState
					ok, err := storage.MVCCGetProto(ctx, eng, key, hlc.Timestamp{}, &truncatedState, storage.MVCCGetOptions{})
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
