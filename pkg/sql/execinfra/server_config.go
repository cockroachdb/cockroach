// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This file lives here instead of sql/distsql to avoid an import cycle.

package execinfra

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/diskmap"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/hydratedtables"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/marusama/semaphore"
)

// SettingWorkMemBytes is a cluster setting that determines the maximum amount
// of RAM that a processor can use.
var SettingWorkMemBytes = settings.RegisterByteSizeSetting(
	"sql.distsql.temp_storage.workmem",
	"maximum amount of memory in bytes a processor can use before falling back to temp storage",
	64*1024*1024, /* 64MB */
).WithPublic()

// ServerConfig encompasses the configuration required to create a
// DistSQLServer.
type ServerConfig struct {
	log.AmbientContext

	Settings     *cluster.Settings
	RuntimeStats RuntimeStats

	ClusterID   *base.ClusterIDContainer
	ClusterName string

	// NodeID is the id of the node on which this Server is running.
	NodeID *base.SQLIDContainer

	// Codec is capable of encoding and decoding sql table keys.
	Codec keys.SQLCodec

	// DB is a handle to the cluster.
	DB *kv.DB
	// Executor can be used to run "internal queries". Note that Flows also have
	// access to an executor in the EvalContext. That one is "session bound"
	// whereas this one isn't.
	Executor sqlutil.InternalExecutor

	RPCContext   *rpc.Context
	Stopper      *stop.Stopper
	TestingKnobs TestingKnobs

	// ParentMemoryMonitor is normally the root SQL monitor. It should only be
	// used when setting up a server, or in tests.
	ParentMemoryMonitor *mon.BytesMonitor

	// TempStorage is used by some DistSQL processors to store rows when the
	// working set is larger than can be stored in memory.
	TempStorage diskmap.Factory

	// TempStoragePath is the path where the vectorized execution engine should
	// create files using TempFS.
	TempStoragePath string

	// TempFS is used by the vectorized execution engine to store columns when the
	// working set is larger than can be stored in memory.
	TempFS fs.FS

	// VecFDSemaphore is a weighted semaphore that restricts the number of open
	// file descriptors in the vectorized engine.
	VecFDSemaphore semaphore.Semaphore

	// BulkAdder is used by some processors to bulk-ingest data as SSTs.
	BulkAdder kvserverbase.BulkAdderFactory

	// Child monitor of the bulk monitor which will be used to monitor the memory
	// used by the column and index backfillers.
	BackfillerMonitor *mon.BytesMonitor

	// ParentDiskMonitor is normally the root disk monitor. It should only be used
	// when setting up a server, a child monitor (usually belonging to a sql
	// execution flow), or in tests. It is used to monitor temporary storage disk
	// usage. Actual disk space used will be a small multiple (~1.1) of this
	// because of RocksDB space amplification.
	ParentDiskMonitor *mon.BytesMonitor

	Metrics *DistSQLMetrics

	// SQLLivenessReader provides access to reading the liveness of sessions.
	SQLLivenessReader sqlliveness.Reader

	// JobRegistry manages jobs being used by this Server.
	JobRegistry *jobs.Registry

	// LeaseManager is a *lease.Manager. It's stored as an `interface{}` due
	// to package dependency cycles
	LeaseManager interface{}

	// A handle to gossip used to broadcast the node's DistSQL version and
	// draining state.
	Gossip gossip.OptionalGossip

	NodeDialer *nodedialer.Dialer

	// SessionBoundInternalExecutorFactory is used to construct session-bound
	// executors. The idea is that a higher-layer binds some of the arguments
	// required, so that users of ServerConfig don't have to care about them.
	SessionBoundInternalExecutorFactory sqlutil.SessionBoundInternalExecutorFactory

	ExternalStorage        cloud.ExternalStorageFactory
	ExternalStorageFromURI cloud.ExternalStorageFromURIFactory

	// ProtectedTimestampProvider maintains the state of the protected timestamp
	// subsystem. It is queried during the GC process and in the handling of
	// AdminVerifyProtectedTimestampRequest.
	ProtectedTimestampProvider protectedts.Provider

	// RangeCache is used by processors that were supposed to have been planned on
	// the leaseholders of the data ranges that they're consuming. These
	// processors query the cache to see if they should communicate updates to the
	// gateway.
	RangeCache *rangecache.RangeCache

	// HydratedTables is a node-level cache of table descriptors which utilize
	// user-defined types.
	HydratedTables *hydratedtables.Cache

	// SQLStatsResetter is an interface used to reset SQL stats without the need to
	// introduce dependency on the sql package.
	SQLStatsResetter tree.SQLStatsResetter
}

// RuntimeStats is an interface through which the rowexec layer can get
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
	// caller of SchemaChanger.exec(). In the case of a column backfill, it is
	// called at the start of the backfill function passed into the transaction
	// executing the chunk.
	RunBeforeBackfillChunk func(sp roachpb.Span) error

	// RunAfterBackfillChunk is called after executing each chunk of a backfill
	// during a schema change operation. In the case of a column backfill, it is
	// called just before returning from the backfill function passed into the
	// transaction executing the chunk. It is always called even when the backfill
	// function returns an error, or if the table has already been dropped.
	RunAfterBackfillChunk func()

	// SerializeIndexBackfillCreationAndIngestion ensures that every index batch
	// created during an index backfill is also ingested before moving on to the
	// next batch or returning.
	// Ingesting does not mean that the index entries are necessarily written to
	// storage but instead that they are buffered in the index backfillers' bulk
	// adder.
	SerializeIndexBackfillCreationAndIngestion chan struct{}

	// IndexBackfillProgressReportInterval is the periodic interval at which the
	// processor pushes the spans for which it has successfully backfilled the
	// indexes.
	IndexBackfillProgressReportInterval time.Duration

	// ForceDiskSpill forces any processors/operators that can fall back to disk
	// to fall back to disk immediately.
	//
	// Cannot be set together with MemoryLimitBytes.
	ForceDiskSpill bool

	// MemoryLimitBytes specifies a maximum amount of working memory that a
	// processor that supports falling back to disk can use. Must be >= 1 to
	// enable. This is a more fine-grained knob than ForceDiskSpill when the
	// available memory needs to be controlled. Once this limit is hit,
	// processors employ their on-disk implementation regardless of applicable
	// cluster settings.
	//
	// Cannot be set together with ForceDiskSpill.
	MemoryLimitBytes int64

	// DrainFast, if enabled, causes the server to not wait for any currently
	// running flows to complete or give a grace period of minFlowDrainWait
	// to incoming flows to register.
	DrainFast bool

	// MetadataTestLevel controls whether or not additional metadata test
	// processors are planned, which send additional "RowNum" metadata that is
	// checked by a test receiver on the gateway.
	MetadataTestLevel MetadataTestLevel

	// CheckVectorizedFlowIsClosedCorrectly checks that all components in a flow
	// were closed explicitly in flow.Cleanup.
	CheckVectorizedFlowIsClosedCorrectly bool

	// Changefeed contains testing knobs specific to the changefeed system.
	Changefeed base.ModuleTestingKnobs

	// Flowinfra contains testing knobs specific to the flowinfra system
	Flowinfra base.ModuleTestingKnobs

	// Forces bulk adder flush every time a KV batch is processed.
	BulkAdderFlushesEveryBatch bool

	// JobsTestingKnobs is jobs infra specific testing knobs.
	JobsTestingKnobs base.ModuleTestingKnobs

	// BackupRestoreTestingKnobs are backup and restore specific testing knobs.
	BackupRestoreTestingKnobs base.ModuleTestingKnobs
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

// GetWorkMemLimit returns the number of bytes determining the amount of RAM
// available to a single processor or operator.
func GetWorkMemLimit(config *ServerConfig) int64 {
	if config.TestingKnobs.ForceDiskSpill && config.TestingKnobs.MemoryLimitBytes != 0 {
		panic(errors.AssertionFailedf("both ForceDiskSpill and MemoryLimitBytes set"))
	}
	if config.TestingKnobs.ForceDiskSpill {
		return 1
	} else if config.TestingKnobs.MemoryLimitBytes != 0 {
		return config.TestingKnobs.MemoryLimitBytes
	}
	return SettingWorkMemBytes.Get(&config.Settings.SV)
}
