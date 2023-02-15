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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

func TestDebugRaftLog(t *testing.T) {
	const rangeID = 64
	ctx := context.Background()
	st := cluster.MakeClusterSettings()
	var opts []storage.ConfigOption
	opts = append(opts, storage.ReadOnly)
	eng, err := storage.Open(
		ctx,
		storage.Filesystem(filepath.Join(
			//os.ExpandEnv("$HOME"), "go", "src", "github.com", "cockroachdb", "cockroach", "cockroach-data"),
			os.ExpandEnv("$HOME"), "local", "2", "data",
		)), st, opts...)
	require.NoError(t, err)
	var lastCT hlc.Timestamp
	require.NoError(t, raftlog.Visit(eng, rangeID, 0, math.MaxUint64, func(entry raftpb.Entry) error {
		e, err := raftlog.NewEntry(entry)
		if err != nil {
			return err
		}
		t.Logf("idx %d: %s: LAI %d, CT %s", e.Index, e.ID, e.Cmd.MaxLeaseIndex, e.Cmd.ClosedTimestamp)
		if e.Cmd.ClosedTimestamp != nil {
			if lastCT.IsSet() && e.Cmd.ClosedTimestamp.Less(lastCT) {
				t.Errorf("^----- closedts regression")
			}
			lastCT = *e.Cmd.ClosedTimestamp
		}
		return nil
	}))
}
