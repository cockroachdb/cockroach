// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This file lives here instead of sql/distsql to avoid an import cycle.

package execinfra

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvstreamer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/diskmap"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/marusama/semaphore"
)

// ServerConfig encompasses the configuration required to create a
// DistSQLServer.
type ServerConfig struct {
	log.AmbientContext

	Settings     *cluster.Settings
	RuntimeStats RuntimeStats

	// LogicalClusterID is the logical cluster ID for this tenant.
	LogicalClusterID *base.ClusterIDContainer

	// ClusterName is the security string used to protect the RPC layer
	// against connections to the wrong cluster.
	ClusterName string

	// NodeID is either the KV node ID or the SQL instance ID, depending
	// on circumstances.
	// TODO(knz,radu): Split this into different fields.
	NodeID *base.SQLIDContainer

	// Locality is the locality of the node on which this Server is running.
	Locality roachpb.Locality

	// Codec is capable of encoding and decoding sql table keys.
	Codec keys.SQLCodec

	// DB is a handle to the cluster.
	DB descs.DB

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
	TempFS vfs.FS

	// VecFDSemaphore is a weighted semaphore that restricts the number of open
	// file descriptors in the vectorized engine.
	VecFDSemaphore semaphore.Semaphore

	// BulkAdder is used by some processors to bulk-ingest data as SSTs.
	BulkAdder kvserverbase.BulkAdderFactory

	// Child monitor of the bulk monitor which will be used to monitor the memory
	// used by the column and index backfillers.
	BackfillerMonitor *mon.BytesMonitor

	// Child monitor of the bulk monitor which will be used to monitor the memory
	// used during backup.
	BackupMonitor *mon.BytesMonitor

	// Child monitor of the bulk monitor which will be used to monitor the memory
	// used during restore.
	RestoreMonitor *mon.BytesMonitor

	// ChangefeedMonitor is the parent monitor for all CDC DistSQL flows.
	ChangefeedMonitor *mon.BytesMonitor

	// BulkSenderLimiter is the concurrency limiter that is shared across all of
	// the processes in a given sql server when sending bulk ingest (AddSST) reqs.
	BulkSenderLimiter limit.ConcurrentRequestLimiter

	// ParentDiskMonitor is normally the root disk monitor. It should only be used
	// when setting up a server, a child monitor (usually belonging to a sql
	// execution flow), or in tests. It is used to monitor temporary storage disk
	// usage. Actual disk space used will be a small multiple (~1.1) of this
	// because of RocksDB space amplification.
	ParentDiskMonitor *mon.BytesMonitor

	Metrics            *DistSQLMetrics
	RowMetrics         *rowinfra.Metrics
	InternalRowMetrics *rowinfra.Metrics
	KVStreamerMetrics  *kvstreamer.Metrics

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

	// Dialer for communication between SQL instances.
	SQLInstanceDialer *nodedialer.Dialer

	ExternalStorage        cloud.ExternalStorageFactory
	ExternalStorageFromURI cloud.ExternalStorageFromURIFactory

	// ProtectedTimestampProvider maintains the state of the protected timestamp
	// subsystem. It is queried during the GC process and in the handling of
	// AdminVerifyProtectedTimestampRequest.
	ProtectedTimestampProvider protectedts.Provider

	DistSender *kvcoord.DistSender

	// RangeCache is used by processors that were supposed to have been planned on
	// the leaseholders of the data ranges that they're consuming. These
	// processors query the cache to see if they should communicate updates to the
	// gateway.
	RangeCache *rangecache.RangeCache

	// SQLStatsController is an interface used to reset SQL stats without the need to
	// introduce dependency on the sql package.
	SQLStatsController eval.SQLStatsController

	// SchemaTelemetryController is an interface used by the builtins to create a
	// job schedule for schema telemetry jobs.
	SchemaTelemetryController eval.SchemaTelemetryController

	// IndexUsageStatsController is an interface used to reset index usage stats without
	// the need to introduce dependency on the sql package.
	IndexUsageStatsController eval.IndexUsageStatsController

	// SQLSQLResponseAdmissionQ is the admission queue to use for
	// SQLSQLResponseWork.
	SQLSQLResponseAdmissionQ *admission.WorkQueue

	// CollectionFactory is used to construct descs.Collections.
	CollectionFactory *descs.CollectionFactory

	// ExternalIORecorder is used to record reads and writes from
	// external services (such as external storage)
	ExternalIORecorder multitenant.TenantSideExternalIORecorder

	// TenantCostController is used to measure and record RU consumption.
	TenantCostController multitenant.TenantSideCostController

	// RangeStatsFetcher is used to fetch range stats for keys.
	RangeStatsFetcher eval.RangeStatsFetcher

	// AdmissionPacerFactory is used to integrate CPU-intensive work
	// with elastic CPU control.
	AdmissionPacerFactory admission.PacerFactory

	// Allow mutation operations to trigger stats refresh.
	StatsRefresher *stats.Refresher

	// *sql.ExecutorConfig exposed as an interface (due to dependency cycles).
	ExecutorConfig interface{}

	// RootSQLMemoryPoolSize is the size in bytes of the root SQL memory
	// monitor.
	RootSQLMemoryPoolSize int64

	// VecIndexManager allows SQL processors to access the vecindex.VectorIndex
	// for operations on a vector index. It's stored as an `interface{}` due to
	// package dependency cycles
	VecIndexManager interface{}
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

	// VecFDsToAcquire, if positive, indicates the number of file descriptors
	// that should be acquired by a single disk-spilling operator in the
	// vectorized engine.
	VecFDsToAcquire int

	// TableReaderBatchBytesLimit, if not 0, overrides the limit that the
	// TableReader will set on the size of results it wants to get for individual
	// requests.
	TableReaderBatchBytesLimit int64
	// JoinReaderBatchBytesLimit, if not 0, overrides the limit that the
	// joinReader will set on the size of results it wants to get for individual
	// lookup requests.
	JoinReaderBatchBytesLimit int64

	// DrainFast, if enabled, causes the server to not wait for any currently
	// running flows to complete or give a grace period of minFlowDrainWait
	// to incoming flows to register.
	DrainFast bool

	// Changefeed contains testing knobs specific to the changefeed system.
	Changefeed base.ModuleTestingKnobs

	// Export contains testing knobs for `EXPORT INTO ...`.
	Export base.ModuleTestingKnobs

	// Flowinfra contains testing knobs specific to the flowinfra system
	Flowinfra base.ModuleTestingKnobs

	// Forces bulk adder flush every time a KV batch is processed.
	BulkAdderFlushesEveryBatch bool

	// JobsTestingKnobs is jobs infra specific testing knobs.
	JobsTestingKnobs base.ModuleTestingKnobs

	// BackupRestoreTestingKnobs are backup and restore specific testing knobs.
	BackupRestoreTestingKnobs base.ModuleTestingKnobs

	// StreamingTestingKnobs are backup and restore specific testing knobs.
	StreamingTestingKnobs base.ModuleTestingKnobs

	// IndexBackfillMergerTestingKnobs are the index backfill merger specific
	// testing knobs.
	IndexBackfillMergerTestingKnobs base.ModuleTestingKnobs

	// ProcessorNoTracingSpan is used to disable the creation of a tracing span
	// in ProcessorBase.StartInternal if the tracing is enabled.
	ProcessorNoTracingSpan bool

	// SetupFlowCb, when non-nil, is called by the execinfrapb.DistSQLServer
	// when responding to SetupFlow RPCs, after the flow is set up but before it
	// is started.
	SetupFlowCb func(context.Context, base.SQLInstanceID, *execinfrapb.SetupFlowRequest) error

	// RunBeforeCascadeAndChecks is run before any cascade or check queries are
	// run. The associated transaction ID of the statement performing the cascade
	// or check query is passed in as an argument.
	RunBeforeCascadesAndChecks func(txnID uuid.UUID)

	// MinimumNumberOfGatewayPartitions is the minimum number of partitions that
	// will be assigned to the gateway before we start assigning partitions to
	// other nodes.
	MinimumNumberOfGatewayPartitions int
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}

// DefaultMemoryLimit is the default value of
// sql.distsql.temp_storage.workmem cluster setting.
const DefaultMemoryLimit = 64 << 20 /* 64 MiB */

// GetWorkMemLimit returns the number of bytes determining the amount of RAM
// available to a single processor or operator.
func GetWorkMemLimit(flowCtx *FlowCtx) int64 {
	if flowCtx.Cfg.TestingKnobs.ForceDiskSpill && flowCtx.Cfg.TestingKnobs.MemoryLimitBytes != 0 {
		panic(errors.AssertionFailedf("both ForceDiskSpill and MemoryLimitBytes set"))
	}
	if flowCtx.Cfg.TestingKnobs.ForceDiskSpill {
		return 1
	}
	if flowCtx.Cfg.TestingKnobs.MemoryLimitBytes != 0 {
		return flowCtx.Cfg.TestingKnobs.MemoryLimitBytes
	}
	if flowCtx.EvalCtx.SessionData().WorkMemLimit <= 0 {
		// If for some reason workmem limit is not set, use the default value.
		return DefaultMemoryLimit
	}
	return flowCtx.EvalCtx.SessionData().WorkMemLimit
}

// GetRowMetrics returns the proper rowinfra.Metrics for either internal or user
// queries.
func (flowCtx *FlowCtx) GetRowMetrics() *rowinfra.Metrics {
	if flowCtx.EvalCtx.SessionData().Internal {
		return flowCtx.Cfg.InternalRowMetrics
	}
	return flowCtx.Cfg.RowMetrics
}
