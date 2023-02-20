// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvstorage

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

func TestDebugRaftLog(t *testing.T) {
	ctx := context.Background()
	st := cluster.MakeClusterSettings()
	var opts []storage.ConfigOption
	opts = append(opts, storage.ReadOnly)

	const rangeID = 60 // @ 1144401
	eng, err := storage.Open(
		ctx,
		storage.Filesystem(filepath.Join(
			// os.ExpandEnv("$HOME"), "go", "src", "github.com", "cockroachdb", "cockroach", "data",
			os.ExpandEnv("$HOME"), "local", "2", "data",
		)), st, opts...)
	require.NoError(t, err)
	var lastSeq roachpb.LeaseSequence
	var lastCT hlc.Timestamp
	m := map[kvserverbase.CmdIDKey]struct{}{}
	require.NoError(t, raftlog.Visit(eng, rangeID, 0, math.MaxUint64, func(entry raftpb.Entry) error {
		e, err := raftlog.NewEntry(entry)
		if err != nil {
			return err
		}
		seq := e.Cmd.ProposerLeaseSequence
		t.Logf("idx %d: %s: LAI %d, CT %s, Lease %d", e.Index, e.ID, e.Cmd.MaxLeaseIndex, e.Cmd.ClosedTimestamp, seq)
		if lastSeq > 0 && seq != lastSeq {
			t.Logf("^----- lease seq change")
		}
		if e.Cmd.ClosedTimestamp != nil {
			if lastCT.IsSet() && e.Cmd.ClosedTimestamp.Less(lastCT) {
				t.Logf("^----- closedts regression")
			}
			lastCT = *e.Cmd.ClosedTimestamp
		}
		if _, ok := m[e.ID]; ok {
			t.Logf("^----- reproposal")
		}
		m[e.ID] = struct{}{}
		return nil
	}))
}
