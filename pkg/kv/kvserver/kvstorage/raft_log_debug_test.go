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
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

func TestDebugRaftLog(t *testing.T) {
	const rangeID = 2
	ctx := context.Background()
	st := cluster.MakeClusterSettings()
	var opts []storage.ConfigOption
	opts = append(opts, storage.ReadOnly)
	eng, err := storage.Open(
		ctx,
		storage.Filesystem(filepath.Join(
			//os.ExpandEnv("$HOME"), "go", "src", "github.com", "cockroachdb", "cockroach", "cockroach-data"),
			os.ExpandEnv("$HOME"), "local", "1", "data",
		)), st, opts...)
	require.NoError(t, err)
	require.NoError(t, raftlog.Visit(eng, rangeID, 0, math.MaxUint64, func(entry raftpb.Entry) error {
		t.Log(entry)
		return nil
	}))
}
