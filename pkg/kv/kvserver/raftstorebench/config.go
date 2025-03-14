// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstorebench

import (
	"os"
	"time"
)

const (
	keyLen   = 50
	valueLen = 1000
	batchLen = 1
)

type Config struct {
	Dir          string `yaml:",omitempty"` // directory to create engines in
	SingleEngine bool   // false to use separate raft and state engines

	NumReplicas int           // replica count to simulate (ops randomly distributed)
	NumWrites   int64         // total number of writes
	NumWorkers  int           // parallelism for writes
	DelayDur    time.Duration `yaml:",omitempty"` // worker sleep between ops

	SMMemtableBytes int64 // state (or combined) engine memtable size
	SMSyncWALBytes  int   `yaml:",omitempty"` // force state machine WAL sync every N bytes
	SMDisableWAL    bool  `yaml:",omitempty"` // disable state machine WAL

	RaftMemtableBytes   int64         // raft engine memtable size
	RaftL0Threshold     int64         // #files in L0 for raft engine triggering compaction
	RaftNoSync          bool          `yaml:",omitempty"` // don't fsync raft WAL
	RaftMetricsInterval time.Duration `yaml:",omitempty"`

	LooseTrunc          bool  // whether to use loosely coupled (state-durability-triggered) truncations
	TruncThresholdBytes int64 // raft log size that triggers truncation
	SingleDel           bool  // use SingleDel for truncations

	WALMetrics bool `yaml:",omitempty"` // print WAL metrics (for any engine that has a WAL)
}

func MakeDefaultConfig() Config {
	cfg := Config{}
	cfg.Dir = os.ExpandEnv("$TMPDIR")
	cfg.SingleEngine = true

	cfg.NumReplicas = 1
	cfg.NumWrites = 0 // don't run anything
	cfg.NumWorkers = 1
	cfg.DelayDur = 0

	cfg.SMMemtableBytes = 64 * 1 << 20
	cfg.SMSyncWALBytes = 0
	cfg.SMDisableWAL = false

	cfg.RaftMemtableBytes = 64 * 1 << 20
	cfg.RaftL0Threshold = 2
	cfg.RaftNoSync = false
	cfg.RaftMetricsInterval = 5 * time.Minute

	cfg.LooseTrunc = false
	cfg.TruncThresholdBytes = 64 * 1024
	cfg.SingleDel = false

	cfg.WALMetrics = true
	return cfg
}
