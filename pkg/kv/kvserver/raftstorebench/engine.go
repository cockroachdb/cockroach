// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstorebench

import (
	"context"
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/disk"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func makeEngines(t T, cfg Config) (*storage.Pebble, *storage.Pebble) {
	smEng := newStateEngine(t, cfg)
	growMemtable(t, smEng, cfg.SMMemtableBytes)
	raftEng := smEng

	if !cfg.SingleEngine {
		raftEng = newRaftEngine(t, cfg)
		growMemtable(t, raftEng, cfg.RaftMemtableBytes)
	}

	return smEng, raftEng
}

func makeFSEnv(t T, fisy vfs.FS, path string) *fs.Env {
	require.NoError(t, fisy.RemoveAll(path))
	require.NoError(t, fisy.MkdirAll(path, os.ModePerm))

	statsMgr := disk.NewWriteStatsManager(vfs.Default)
	env, err := fs.InitEnv(context.Background(), fisy, path, fs.EnvConfig{RW: fs.ReadWrite}, statsMgr)
	require.NoError(t, err)

	return env
}

func newStateEngine(t T, cfg Config) *storage.Pebble {
	var path string
	if !cfg.SingleEngine {
		path = filepath.Join(cfg.Dir, "state")
	} else {
		path = filepath.Join(cfg.Dir, "state_and_raft")
	}
	env := makeFSEnv(t, vfs.Default, path)

	maxCompactions := 4
	if cfg.SingleEngine {
		maxCompactions = 8
	}

	smEng, err := storage.Open(context.Background(), env, cluster.MakeTestingClusterSettings(),
		storage.MaxConcurrentCompactions(maxCompactions),
		storage.MemtableSize(uint64(cfg.SMMemtableBytes)),
		storage.If(cfg.SMSyncWALBytes > 0, storage.WALBytesPerSync(cfg.SMSyncWALBytes)),
		storage.If(cfg.SMDisableWAL, storage.DisableWAL()),
	)
	require.NoError(t, err)
	t.Cleanup(smEng.Close)

	return smEng
}

func newRaftEngine(t T, cfg Config) *storage.Pebble {
	raftPath := filepath.Join(cfg.Dir, "raft")
	env := makeFSEnv(t, vfs.Default, raftPath)

	raftEng, err := storage.Open(context.Background(), env, cluster.MakeTestingClusterSettings(),
		storage.MaxConcurrentCompactions(4),
		storage.MemtableSize(uint64(cfg.RaftMemtableBytes)),
		storage.L0CompactionThreshold(int(cfg.RaftL0Threshold)),
	)
	require.NoError(t, err)
	t.Cleanup(raftEng.Close)
	return raftEng
}

func growMemtable(t T, eng storage.Engine, target int64) {
	// Warm up the memtable size so that results are easier to understand,
	// especially when we do a run with small num-steps. 256KB is the initial
	// memtable size, and doubles with each flush. We'll have reached any
	// reasonable memtable size in well under 20 iterations.
	for memtableSize := int64(256 * 1024); memtableSize < target; memtableSize *= 2 {
		b := eng.NewWriteBatch()
		require.NoError(t, b.PutUnversioned(roachpb.Key("zoo"), nil))
		require.NoError(t, b.Commit(false)) // this works even when WAL is off

		// Each flush doubles the next memtable size.
		require.NoError(t, eng.Flush())
	}

	// Delete the key.
	b := eng.NewWriteBatch()
	require.NoError(t, b.ClearUnversioned(roachpb.Key("zoo"), storage.ClearOptions{}))
	require.NoError(t, b.Commit(false))

	require.NoError(t, eng.Flush())
}
