// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsql

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/storage/diskmap"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// SettingUseTempStorageJoins is a cluster setting that configures whether
// joins are allowed to spill to disk.
// TODO(yuzefovich): remove this setting.
var SettingUseTempStorageJoins = settings.RegisterBoolSetting(
	"sql.distsql.temp_storage.joins",
	"set to true to enable use of disk for distributed sql joins",
	true,
)

// SettingWorkMemBytes is a cluster setting that determines the maximum amount
// of RAM that a processor can use.
var SettingWorkMemBytes = settings.RegisterByteSizeSetting(
	"sql.distsql.temp_storage.workmem",
	"maximum amount of memory in bytes a processor can use before falling back to temp storage",
	64*1024*1024, /* 64MB */
)

// ServerConfig encompasses the configuration required to create a
// DistSQLServer.
type ServerConfig struct {
	log.AmbientContext

	Settings     *cluster.Settings
	RuntimeStats RuntimeStats

	// DB is a handle to the cluster.
	DB *client.DB
	// Executor can be used to run "internal queries". Note that Flows also have
	// access to an executor in the EvalContext. That one is "session bound"
	// whereas this one isn't.
	Executor sqlutil.InternalExecutor

	// FlowDB is the DB that flows should use for interacting with the database.
	// This DB has to be set such that it bypasses the local TxnCoordSender. We
	// want only the TxnCoordSender on the gateway to be involved with requests
	// performed by DistSQL.
	FlowDB       *client.DB
	RPCContext   *rpc.Context
	Stopper      *stop.Stopper
	TestingKnobs TestingKnobs

	// ParentMemoryMonitor is normally the root SQL monitor. It should only be
	// used when setting up a server, or in tests.
	ParentMemoryMonitor *mon.BytesMonitor

	// TempStorage is used by some DistSQL processors to store rows when the
	// working set is larger than can be stored in memory.
	TempStorage diskmap.Factory

	// BulkAdder is used by some processors to bulk-ingest data as SSTs.
	BulkAdder storagebase.BulkAdderFactory

	// DiskMonitor is used to monitor temporary storage disk usage. Actual disk
	// space used will be a small multiple (~1.1) of this because of RocksDB
	// space amplification.
	DiskMonitor *mon.BytesMonitor

	Metrics *Metrics

	// NodeID is the id of the node on which this Server is running.
	NodeID      *base.NodeIDContainer
	ClusterID   *base.ClusterIDContainer
	ClusterName string

	// JobRegistry manages jobs being used by this Server.
	JobRegistry *jobs.Registry

	// LeaseManager is a *sql.LeaseManager. It's stored as an `interface{}` due
	// to package dependency cycles
	LeaseManager interface{}

	// A handle to gossip used to broadcast the node's DistSQL version and
	// draining state.
	Gossip *gossip.Gossip

	NodeDialer *nodedialer.Dialer

	// SessionBoundInternalExecutorFactory is used to construct session-bound
	// executors. The idea is that a higher-layer binds some of the arguments
	// required, so that users of ServerConfig don't have to care about them.
	SessionBoundInternalExecutorFactory sqlutil.SessionBoundInternalExecutorFactory
}

// RuntimeStats is an interface through which the distsqlrun layer can get
// information about runtime statistics.
type RuntimeStats interface {
	// GetCPUCombinedPercentNorm returns the recent user+system cpu usage,
	// normalized to 0-1 by number of cores.
	GetCPUCombinedPercentNorm() float64
}

// TestingKnobs are the testing knobs.
type TestingKnobs struct {
	// RunBeforeBackfillChunk is called before executing each chunk of a
	// backfill during a schema change operation. It is called with the
	// current span and returns an error which eventually is returned to the
	// caller of SchemaChanger.exec(). It is called at the start of the
	// backfill function passed into the transaction executing the chunk.
	RunBeforeBackfillChunk func(sp roachpb.Span) error

	// RunAfterBackfillChunk is called after executing each chunk of a
	// backfill during a schema change operation. It is called just before
	// returning from the backfill function passed into the transaction
	// executing the chunk. It is always called even when the backfill
	// function returns an error, or if the table has already been dropped.
	RunAfterBackfillChunk func()

	// MemoryLimitBytes specifies a maximum amount of working memory that a
	// processor that supports falling back to disk can use. Must be >= 1 to
	// enable. Once this limit is hit, processors employ their on-disk
	// implementation regardless of applicable cluster settings.
	MemoryLimitBytes int64

	// DrainFast, if enabled, causes the server to not wait for any currently
	// running flows to complete or give a grace period of minFlowDrainWait
	// to incoming flows to register.
	DrainFast bool

	// MetadataTestLevel controls whether or not additional metadata test
	// processors are planned, which send additional "RowNum" metadata that is
	// checked by a test receiver on the gateway.
	MetadataTestLevel MetadataTestLevel

	// DeterministicStats overrides stats which don't have reliable values, like
	// stall time and bytes sent. It replaces them with a zero value.
	DeterministicStats bool

	// Changefeed contains testing knobs specific to the changefeed system.
	Changefeed base.ModuleTestingKnobs
}

// MetadataTestLevel represents the types of queries where metadata test
// processors are planned.
type MetadataTestLevel int

const (
	// Off represents that no metadata test processors are planned.
	Off MetadataTestLevel = iota
	// NoExplain represents that metadata test processors are planned for all
	// queries except EXPLAIN (DISTSQL) statements.
	NoExplain
	// On represents that metadata test processors are planned for all queries.
	On
)

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}
