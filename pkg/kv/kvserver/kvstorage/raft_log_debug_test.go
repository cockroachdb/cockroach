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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"go.etcd.io/raft/v3/raftpb"
)

func TestDebugRaftLog(t *testing.T) {
	const rangeID = 2
	ctx := context.Background()
	st := cluster.MakeClusterSettings()
	var opts []storage.ConfigOption
	opts = append(opts, storage.ReadOnly)
	eng, err := storage.Open(ctx, storage.Filesystem("cockroach-data"), st, opts...)
	raftlog.Visit(eng, rangeID, 0, math.MaxUint64, func(entry raftpb.Entry) error {

	})
}
