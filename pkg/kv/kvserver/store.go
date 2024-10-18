// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"bytes"
	"context"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/load"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/sidetransport"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/idalloc"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/intentresolver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvadmission"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowhandle"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/node_rac2"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/rac2"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/replica_rac2"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/multiqueue"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftentry"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness"
	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/tenantrate"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/tscache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnrecovery"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnwait"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiesauthorizer"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftlogger"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftstoreliveness"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigstore"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/disk"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/shuffle"
	"github.com/cockroachdb/cockroach/pkg/util/slidingwindow"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"github.com/prometheus/client_golang/prometheus"
	prometheusgo "github.com/prometheus/client_model/go"
	"golang.org/x/time/rate"
)

const (
	// rangeIDAllocCount is the number of Range IDs to allocate per allocation.
	rangeIDAllocCount = 10

	// replicaQueueExtraSize is the number of requests that a replica's incoming
	// message queue can keep over RaftConfig.RaftMaxInflightMsgs. When the leader
	// maxes out RaftMaxInflightMsgs, we want the receiving replica to still have
	// some buffer for other messages, primarily heartbeats.
	replicaQueueExtraSize = 10
)

// defaultRaftSchedulerConcurrency specifies the default number of Raft
// scheduler worker goroutines. These are evenly distributed across stores,
// rounded up such that all stores have the same number of workers (10 workers
// across 3 stores yields 4 workers per store or 12 in total).
//
// For small machines, we scale the scheduler concurrency by the number of CPUs.
// 8*NumCPU was determined in 9a68241 (April 2017) as the optimal concurrency
// level on 8 CPU machines. For larger machines, we've seen (#56851) that this
// scaling curve can be too aggressive and lead to too much contention in the
// Raft scheduler, so we cap the concurrency level at 128. This was revisited in
// #99063 (June 2023) with Raft scheduler sharding and multi-store worker
// distribution, and both the scaling and cap were found to still be reasonable,
// but the cap was increased from 96 to 128 to reduce chance of starvation
// within shards or on multi-store nodes.
var defaultRaftSchedulerConcurrency = envutil.EnvOrDefaultInt(
	"COCKROACH_SCHEDULER_CONCURRENCY", min(8*runtime.GOMAXPROCS(0), 128))

// defaultRaftSchedulerMinConcurrencyPerStore specifies the minimum number of
// Raft scheduler worker goroutines for each store. The configuration prevents
// defaultRaftSchedulerConcurrency from being spread so thin across stores in a
// many-store system that any single store's worker pool cannot keep up with
// imbalanced load.
var defaultRaftSchedulerMinConcurrencyPerStore = envutil.EnvOrDefaultInt(
	"COCKROACH_SCHEDULER_MIN_CONCURRENCY_PER_STORE", min(runtime.GOMAXPROCS(0), defaultRaftSchedulerConcurrency))

// defaultRaftSchedulerShardSize specifies the default maximum number of
// scheduler worker goroutines per mutex shard. By default, we spin up 8 workers
// per CPU core, capped at 128, so 16 is equivalent to 2 CPUs per shard, or a
// maximum of 8 shards. This significantly relieves contention at high core
// counts, while also avoiding starvation by excessive sharding.
var defaultRaftSchedulerShardSize = envutil.EnvOrDefaultInt("COCKROACH_SCHEDULER_SHARD_SIZE", 16)

// defaultRaftEntryCacheSize is the default size in bytes for the Raft entry
// cache, divided evenly between stores. The Raft entry cache is shared by all
// Raft groups managed by each store. It is used to cache uncommitted raft log
// entries such that once those entries are committed, their application can
// avoid disk reads to retrieve them from the persistent log.
//
// It defaults to 1/256 of system memory, with minimum 32 MB, e.g.:
//
// 8 GB RAM  = 32 MB  (~2 vCPUs)
// 16 GB RAM = 64 MB  (~4 vCPUs)
// 32 GB RAM = 128 MB (~8 vCPUs)
// 64 GB RAM = 256 MB (~16 vCPUs)
//
// This is conservative, since the memory is not accounted for in memory budgets
// nor via the --cache flag. However, it should be sufficient to achieve near
// 100% cache hit rate for well-provisioned low-latency clusters with moderate
// write volume. See: https://github.com/cockroachdb/cockroach/issues/98666
var defaultRaftEntryCacheSize = func() int64 {
	var cacheSize int64 = 32 << 20 // 32 MiB
	if mem, _, err := status.GetTotalMemoryWithoutLogging(); err == nil {
		if s := mem / 256; s > cacheSize {
			cacheSize = s
		}
	}
	return envutil.EnvOrDefaultBytes("COCKROACH_RAFT_ENTRY_CACHE_SIZE", cacheSize)
}()

// defaultRaftSchedulerPriorityShardSize specifies the default size of the Raft
// scheduler priority shard, used for certain system ranges. This shard is
// always fully populated with workers that don't count towards the concurrency
// limit, and is thus effectively the number of priority workers per store.
var defaultRaftSchedulerPriorityShardSize = envutil.EnvOrDefaultInt(
	"COCKROACH_SCHEDULER_PRIORITY_SHARD_SIZE", 2)

var logSSTInfoTicks = envutil.EnvOrDefaultInt(
	"COCKROACH_LOG_SST_INFO_TICKS_INTERVAL", 60)

// By default, telemetry events are emitted once per hour, per store:
// (10s tick interval) * 6 * 60 = 3600s = 1h.
var logStoreTelemetryTicks = envutil.EnvOrDefaultInt(
	"COCKROACH_LOG_STORE_TELEMETRY_TICKS_INTERVAL",
	6*60,
)

// defaultRangefeedSchedulerConcurrency specifies how many workers rangefeed
// scheduler will use to perform rangefeed work. This number will be divided
// between stores of the node.
var defaultRangefeedSchedulerConcurrency = envutil.EnvOrDefaultInt(
	"COCKROACH_RANGEFEED_SCHEDULER_WORKERS", min(4*runtime.GOMAXPROCS(0), 64))

// defaultRangefeedSchedulerShardSize specifies the default maximum number of
// scheduler worker goroutines per mutex shard. By default, we spin up 4 workers
// per CPU core, capped at 64, so 8 is equivalent to 2 CPUs per shard, or a
// maximum of 8 shards. Since rangefeed processing is generally cheap, this
// significantly relieves contention, while also avoiding starvation by
// excessive sharding.
var defaultRangefeedSchedulerShardSize = envutil.EnvOrDefaultInt(
	"COCKROACH_RANGEFEED_SCHEDULER_SHARD_SIZE", 8)

// defaultRangefeedSchedulerPriorityShardSize specifies the default size of the
// rangefeed scheduler priority shard, used for certain system ranges. This
// shard is always fully populated with workers that don't count towards the
// concurrency limit, and is thus effectively the number of priority workers per
// store.
var defaultRangefeedSchedulerPriorityShardSize = envutil.EnvOrDefaultInt(
	"COCKROACH_RANGEFEED_SCHEDULER_PRIORITY_SHARD_SIZE", 2)

// bulkIOWriteLimit is defined here because it is used by BulkIOWriteLimiter.
var bulkIOWriteLimit = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"kv.bulk_io_write.max_rate",
	"the rate limit (bytes/sec) to use for writes to disk on behalf of bulk io ops",
	1<<40, // 1 TiB
	settings.WithPublic)

// addSSTableRequestLimit limits concurrent AddSSTable requests.
var addSSTableRequestLimit = settings.RegisterIntSetting(
	settings.SystemOnly,
	"kv.bulk_io_write.concurrent_addsstable_requests",
	"number of concurrent AddSSTable requests per store before queueing",
	math.MaxInt, // unlimited
	settings.PositiveInt,
)

// addSSTableAsWritesRequestLimit limits concurrent AddSSTable requests with
// IngestAsWrites set. These are smaller (kv.bulk_io_write.small_write_size),
// and will end up in the Pebble memtable (default 64 MB) before flushing to
// disk, so we can allow a greater amount of concurrency than regular AddSSTable
// requests. Applied independently of concurrent_addsstable_requests.
var addSSTableAsWritesRequestLimit = settings.RegisterIntSetting(
	settings.SystemOnly,
	"kv.bulk_io_write.concurrent_addsstable_as_writes_requests",
	"number of concurrent AddSSTable requests ingested as writes per store before queueing",
	math.MaxInt, // unlimited
	settings.PositiveInt,
)

// concurrentRangefeedItersLimit limits concurrent rangefeed catchup iterators.
var concurrentRangefeedItersLimit = settings.RegisterIntSetting(
	settings.SystemOnly,
	"kv.rangefeed.concurrent_catchup_iterators",
	"number of rangefeeds catchup iterators a store will allow concurrently before queueing",
	16,
	settings.PositiveInt,
)

// Minimum time interval between system config updates which will lead to
// enqueuing replicas.
var queueAdditionOnSystemConfigUpdateRate = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"kv.store.system_config_update.queue_add_rate",
	"the rate (per second) at which the store will add, all replicas to the split and merge queue due to system config gossip",
	.5,
	settings.NonNegativeFloat,
)

// Minimum time interval between system config updates which will lead to
// enqueuing replicas. The default is relatively high to deal with startup
// scenarios.
var queueAdditionOnSystemConfigUpdateBurst = settings.RegisterIntSetting(
	settings.SystemOnly,
	"kv.store.system_config_update.queue_add_burst",
	"the burst rate at which the store will add all replicas to the split and merge queue due to system config gossip",
	32,
	settings.NonNegativeInt,
)

// LeaseTransferPerIterationTimeout is the timeout for a single iteration of draining range leases.
var LeaseTransferPerIterationTimeout = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"server.shutdown.lease_transfer_wait",
	"the timeout for a single iteration of the range lease transfer phase of draining "+
		"(note that the --drain-wait parameter for cockroach node drain may need adjustment "+
		"after changing this setting)",
	5*time.Second,
	settings.WithName("server.shutdown.lease_transfer_iteration.timeout"),
	settings.NonNegativeDuration,
	settings.WithPublic,
)

// exportRequestsLimit is the number of Export requests that can run at once.
// Each extracts data from Pebble to an in-memory SST and returns it to the
// caller. In order to not exhaust the disk or memory, or saturate the network,
// limit the number of these that can be run in parallel. This number was chosen
// by a guessing - it could be improved by more measured heuristics. Exported
// here since we check it in the caller to limit generated requests as well
// to prevent excessive queuing.
var exportRequestsLimit = settings.RegisterIntSetting(
	settings.SystemVisible, // used in backup processor
	"kv.bulk_io_write.concurrent_export_requests",
	"number of export requests a store will handle concurrently before queuing",
	3,
	settings.PositiveInt,
)

// SnapshotApplyLimit is the number of concurrent snapshots a store will
// apply. The send limit is typically higher than the apply limit for a few
// reasons. One is that it keeps "pipelining" of requests in the case where
// there is only a single sender and single receiver. As soon as a receiver
// finishes a request, there will be another one to start. The performance
// impact of sending snapshots is lower than applying. Finally, snapshots are
// not sent until the receiver is ready to apply, so the cost of sending is
// low until the receiver is ready.
var snapshotApplyLimit = settings.RegisterIntSetting(
	settings.SystemVisible,
	"kv.store.raft_snapshot.apply_limit",
	"number of concurrent snapshot apply requests that can be handled by store",
	envutil.EnvOrDefaultInt64("COCKROACH_CONCURRENT_SNAPSHOT_APPLY_LIMIT", 1),
	settings.NonNegativeInt,
)

// SnapshotSendLimit specifies the maximum number of snapshots that are
// permitted to be sent concurrently.
var SnapshotSendLimit = settings.RegisterIntSetting(
	settings.SystemVisible,
	"kv.store.raft_snapshot.send_limit",
	"number of concurrent snapshot sent requests that can be generated by store",
	envutil.EnvOrDefaultInt64("COCKROACH_CONCURRENT_SNAPSHOT_SEND_LIMIT", 2),
	settings.NonNegativeInt,
)

// TestStoreConfig has some fields initialized with values relevant in tests.
func TestStoreConfig(clock *hlc.Clock) StoreConfig {
	return testStoreConfig(clock, clusterversion.Latest.Version())
}

// TestStoreConfigWithVersion is the same as TestStoreConfig but allows to pass a cluster version.
func TestStoreConfigWithVersion(clock *hlc.Clock, version roachpb.Version) StoreConfig {
	return testStoreConfig(clock, version)
}

func testStoreConfig(clock *hlc.Clock, version roachpb.Version) StoreConfig {
	if clock == nil {
		clock = hlc.NewClockForTesting(nil)
	}
	st := cluster.MakeTestingClusterSettingsWithVersions(version, version, true)
	tracer := tracing.NewTracerWithOpt(context.TODO(), tracing.WithClusterSettings(&st.SV))
	sc := StoreConfig{
		DefaultSpanConfig:           zonepb.DefaultZoneConfigRef().AsSpanConfig(),
		Settings:                    st,
		AmbientCtx:                  log.MakeTestingAmbientContext(tracer),
		Clock:                       clock,
		CoalescedHeartbeatsInterval: 50 * time.Millisecond,
		ScanInterval:                10 * time.Minute,
		HistogramWindowInterval:     metric.TestSampleInterval,
		ProtectedTimestampReader:    spanconfig.EmptyProtectedTSReader(clock),
		RangeCount:                  &atomic.Int64{},

		// Use a constant empty system config, which mirrors the previously
		// existing logic to install an empty system config in gossip.
		SystemConfigProvider: config.NewConstantSystemConfigProvider(
			config.NewSystemConfig(zonepb.DefaultZoneConfigRef()),
		),
		KVFlowAdmittedPiggybacker:    node_rac2.NewAdmittedPiggybacker(),
		KVFlowStreamTokenProvider:    rac2.NewStreamTokenCounterProvider(st, clock),
		KVFlowEvalWaitMetrics:        rac2.NewEvalWaitMetrics(),
		KVFlowRangeControllerMetrics: rac2.NewRangeControllerMetrics(),
	}
	sc.TestingKnobs.TenantRateKnobs.Authorizer = tenantcapabilitiesauthorizer.NewAllowEverythingAuthorizer()

	// Use shorter Raft tick settings in order to minimize start up and failover
	// time in tests.
	sc.RaftHeartbeatIntervalTicks = 1
	sc.RaftElectionTimeoutTicks = 3
	sc.RaftReproposalTimeoutTicks = 5
	sc.RaftTickInterval = 100 * time.Millisecond
	sc.SetDefaults(1 /* numStores */)
	return sc
}

func newRaftConfig(
	ctx context.Context,
	strg raft.Storage,
	id raftpb.PeerID,
	appliedIndex kvpb.RaftIndex,
	storeCfg StoreConfig,
	lazyReplication bool,
	logger raftlogger.Logger,
	storeLiveness raftstoreliveness.StoreLiveness,
) *raft.Config {
	return &raft.Config{
		ID:                          id,
		Applied:                     uint64(appliedIndex),
		AsyncStorageWrites:          true,
		ElectionTick:                storeCfg.RaftElectionTimeoutTicks,
		HeartbeatTick:               storeCfg.RaftHeartbeatIntervalTicks,
		MaxUncommittedEntriesSize:   storeCfg.RaftMaxUncommittedEntriesSize,
		MaxCommittedSizePerReady:    storeCfg.RaftMaxCommittedSizePerReady,
		DisableConfChangeValidation: true, // see https://github.com/cockroachdb/cockroach/issues/105797
		LazyReplication:             lazyReplication,
		MaxSizePerMsg:               storeCfg.RaftMaxSizePerMsg,
		MaxInflightMsgs:             storeCfg.RaftMaxInflightMsgs,
		MaxInflightBytes:            storeCfg.RaftMaxInflightBytes,
		Storage:                     strg,
		Logger:                      logger,
		StoreLiveness:               storeLiveness,
		PreVote:                     true,
		CheckQuorum:                 storeCfg.RaftEnableCheckQuorum,
		CRDBVersion:                 storeCfg.Settings.Version,
	}
}

// verifyKeys verifies keys. If checkEndKey is true, then the end key
// is verified to be non-nil and greater than start key. If
// checkEndKey is false, end key is verified to be nil. Additionally,
// verifies that start key is less than KeyMax and end key is less
// than or equal to KeyMax. It also verifies that a key range that
// contains range-local keys is completely range-local.
func verifyKeys(start, end roachpb.Key, checkEndKey bool) error {
	if bytes.Compare(start, roachpb.KeyMax) >= 0 {
		return errors.Errorf("start key %q must be less than KeyMax", start)
	}
	if !checkEndKey {
		if len(end) != 0 {
			return errors.Errorf("end key %q should not be specified for this operation", end)
		}
		return nil
	}
	if end == nil {
		return errors.Errorf("end key must be specified")
	}
	if bytes.Compare(roachpb.KeyMax, end) < 0 {
		return errors.Errorf("end key %q must be less than or equal to KeyMax", end)
	}
	{
		sAddr, err := keys.Addr(start)
		if err != nil {
			return err
		}
		eAddr, err := keys.Addr(end)
		if err != nil {
			return err
		}
		if !sAddr.Less(eAddr) {
			return errors.Errorf("end key %q must be greater than start %q", end, start)
		}
		if !bytes.Equal(sAddr, start) {
			if bytes.Equal(eAddr, end) {
				return errors.Errorf("start key is range-local, but end key is not")
			}
		} else if bytes.Compare(start, keys.LocalMax) < 0 {
			// It's a range op, not local but somehow plows through local data -
			// not cool.
			return errors.Errorf("start key in [%q,%q) must be greater than LocalMax", start, end)
		}
	}

	return nil
}

// A storeReplicaVisitor calls a visitor function for each of a store's
// initialized Replicas (in unspecified order). It provides an option
// to visit replicas in increasing RangeID order.
type storeReplicaVisitor struct {
	store   *Store
	repls   []*Replica // Replicas to be visited
	visited int        // Number of visited ranges, -1 before first call to Visit()
	order   storeReplicaVisitorOrder
}

type storeReplicaVisitorOrder byte

const (
	// visitRandom shuffles the order of the replicas. It is the default.
	visitRandom storeReplicaVisitorOrder = iota
	// visitSorted sorts the replicas by their range ID.
	visitSorted
	// visitUndefined does not touch the ordering of the replicas, and thus
	// leaves it undefined
	visitUndefined
)

// Len implements sort.Interface.
func (rs storeReplicaVisitor) Len() int { return len(rs.repls) }

// Less implements sort.Interface.
func (rs storeReplicaVisitor) Less(i, j int) bool { return rs.repls[i].RangeID < rs.repls[j].RangeID }

// Swap implements sort.Interface.
func (rs storeReplicaVisitor) Swap(i, j int) { rs.repls[i], rs.repls[j] = rs.repls[j], rs.repls[i] }

// newStoreReplicaVisitor constructs a storeReplicaVisitor.
func newStoreReplicaVisitor(store *Store) *storeReplicaVisitor {
	return &storeReplicaVisitor{
		store:   store,
		visited: -1,
	}
}

// InOrder tells the visitor to visit replicas in increasing RangeID order.
func (rs *storeReplicaVisitor) InOrder() *storeReplicaVisitor {
	rs.order = visitSorted
	return rs
}

// UndefinedOrder tells the visitor to visit replicas in any order.
func (rs *storeReplicaVisitor) UndefinedOrder() *storeReplicaVisitor {
	rs.order = visitUndefined
	return rs
}

// Visit calls the visitor with each Replica until false is returned.
func (rs *storeReplicaVisitor) Visit(visitor func(*Replica) bool) {
	// Copy the range IDs to a slice so that we iterate over some (possibly
	// stale) view of all Replicas without holding the Store lock. In particular,
	// no locks are acquired during the copy process.
	rs.repls = nil
	rs.store.mu.replicasByRangeID.Range(func(_ roachpb.RangeID, repl *Replica) bool {
		rs.repls = append(rs.repls, repl)
		return true
	})

	switch rs.order {
	case visitRandom:
		// The Replicas are already in "unspecified order" due to map iteration,
		// but we want to make sure it's completely random to prevent issues in
		// tests where stores are scanning replicas in lock-step and one store is
		// winning the race and getting a first crack at processing the replicas on
		// its queues.
		//
		// TODO(peter): Re-evaluate whether this is necessary after we allow
		// rebalancing away from the leaseholder. See TestRebalance_3To5Small.
		shuffle.Shuffle(rs)
	case visitSorted:
		// If the replicas were requested in sorted order, perform the sort.
		sort.Sort(rs)
	case visitUndefined:
		// Don't touch the ordering.

	default:
		panic(errors.AssertionFailedf("invalid visit order %v", rs.order))
	}

	rs.visited = 0
	for _, repl := range rs.repls {
		// TODO(tschottdorf): let the visitor figure out if something's been
		// destroyed once we return errors from mutexes (#9190). After all, it
		// can still happen with this code.
		rs.visited++
		repl.mu.RLock()
		destroyed := repl.mu.destroyStatus
		initialized := repl.IsInitialized()
		repl.mu.RUnlock()
		if initialized && destroyed.IsAlive() && !visitor(repl) {
			break
		}
	}
	rs.visited = 0
}

// EstimatedCount returns an estimated count of the underlying store's
// replicas.
//
// TODO(tschottdorf): this method has highly doubtful semantics.
func (rs *storeReplicaVisitor) EstimatedCount() int {
	if rs.visited <= 0 {
		return rs.store.ReplicaCount()
	}
	return len(rs.repls) - rs.visited
}

// internalEngines contains the engines that support the operations of
// this Store. At the time of writing, all three fields will be populated
// with the same Engine. As work on CRDB-220 (separate raft log) proceeds,
// we will be able to experimentally run with a separate log engine, and
// ultimately allow doing so in production deployments.
type internalEngines struct {
	// stateEngine is the engine that materializes the raft logs on the system.
	stateEngine storage.Engine
	// todoEngine is a placeholder while we work on CRDB-220, used in cases where
	// - the code does not yet cleanly separate between state and log engine
	// - it is still unclear which of the two engines is the better choice for a
	//   particular write, or there is a candidate, but it needs to be verified.
	todoEngine storage.Engine
	// logEngine is the engine holding the raft state.
	logEngine storage.Engine
}

/*
A Store maintains a set of Replicas whose data is stored on a storage.Engine
usually corresponding to a dedicated storage medium. It also houses a collection
of subsystems that, in broad terms, perform maintenance of each Replica on the
Store when required. In particular, this includes various queues such as the
split, merge, rebalance, GC, and replicaGC queues (to name just a few).

INVARIANT: the set of all Ranges (as determined by, e.g. a transactionally
consistent scan of the meta index ranges) always exactly covers the addressable
keyspace roachpb.KeyMin (inclusive) to roachpb.KeyMax (exclusive).

# Ranges

Each Replica is part of a Range, i.e. corresponds to what other systems would
call a shard. A Range is a consensus group backed by Raft, i.e. each Replica is
a state machine backed by a replicated log of commands. In CockroachDB, Ranges
own a contiguous chunk of addressable keyspace, where the word "addressable" is
a fine-print that interested readers can learn about in keys.Addr; in short the
Replica data is logically contiguous, but not contiguous when viewed as Engine
kv pairs.

Considerable complexity is incurred by the features of dynamic re-sharding and
relocation, i.e. range splits, range merges, and range relocation (also called
replication changes, or, in the case of lateral movement, rebalancing). Each of
these interact heavily with the Range as a consensus group (of which each
Replica is a member). All of these intricacies are described at a high level in
this comment.

# RangeDescriptor

A roachpb.RangeDescriptor is the configuration of a Range. It is an
MVCC-backed key-value pair (where the key is derived from the StartKey via
keys.RangeDescriptorKey, which in particular resides on the Range itself)
that is accessible to the transactional KV API much like any other, but is
for internal use only (and in particular plays no role at the SQL layer).

Splits, Merges, and Rebalances are all carried out as distributed
transactions. They are all complex in their own right but they share the
basic approach of transactionally acting on the RangeDescriptor. Each of
these operations at some point will

- start a transaction

- update the RangeDescriptor (for example, to reflect a split, or a change
to the Replicas comprising the members of the Range)

  - update the meta ranges (which form a search index used for request routing, see
    kv.RangeLookup and updateRangeAddressing for details)

- commit with a roachpb.InternalCommitTrigger.

In particular, note that the RangeDescriptor is the first write issued in the
transaction, and so the transaction record will be created on the affected
range. This allows us to establish a helpful invariant:

INVARIANT: an intent on keys.RangeDescriptorKey is resolved atomically with
the (application of the) kvpb.EndTxnRequest committing the transaction.

A Replica's active configuration is dictated by its visible version of the
RangeDescriptor, and the above invariant simplifies this. Without the invariant,
the new RangeDescriptor would come into effect when the intent is resolved,
which is a) later (so requests might get routed to this Replica without it being
ready to handle them appropriately) and b) requires special-casing around
ResolveIntent acting on the RangeDescriptor.

INVARIANT: A Store never contains two Replicas from the same Range, nor do the
key ranges for any two of its Replicas overlap. (Note that there is no
requirement that these Replicas come from a consistent set of Ranges; the
RangeDescriptor.Generation orders overlapping descriptors).

To illustrate this last invariant, consider a split of a Replica [a-z) into two
Replicas, [a-c) and [c,z). The Store will never contain both; it has to swap
directly from [a-z) to [a,c)+[c,z). For a similar example, consider accepting a
new Replica [b,d) when a Replica [a,c) is already present, which is similarly
not allowed. To understand how such situations could arise, consider that a
series of changes to the RangeDescriptor is observed asynchronously by
followers. This means that a follower may have Replica [a-z) while any number of
splits and replication changes are already known to the leaseholder, and the new
Ranges created in the process may be attempting to add a Replica to the slow
follower.

With the invariant, we effectively allow looking up a unique (if it exists)
Replica for any given key, and ensure that no two Replicas on a Store operate on
shared keyspace (as seen by the storage.Engine). Refer to the Replica Lifecycle
diagram below for details on how this invariant is upheld.

# Replica Lifecycle

A Replica should be thought of primarily as a State Machine applying commands
from a replicated log (the log being replicated across the members of the
Range). The Store's RaftTransport receives Raft messages from Replicas residing
on other Stores and routes them to the appropriate Replicas via
Store.HandleRaftRequest (which is part of the IncomingRaftMessageHandler
interface), ultimately resulting in a call to
Replica.handleRaftReadyRaftMuLocked, which houses the integration with the
etcd/raft library (raft.RawNode). This may generate Raft messages to be sent to
other Stores; these are handed to Replica.sendRaftMessages which ultimately
hands them to the Store's RaftTransport.SendAsync method. Raft uses message
passing (not request-response), and outgoing messages will use a gRPC stream
that differs from that used for incoming messages (which makes asymmetric
partitions more likely in case of stream-specific problems). The steady state is
relatively straightforward but when Ranges are being reconfigured, an
understanding the Replica Lifecycle becomes important and upholding the Store's
invariants becomes more complex.

A first phenomenon to understand is that of uninitialized Replicas, which is the
State Machine at applied index zero, i.e. has an empty state. In CockroachDB, an
uninitialized Replica can only advance to a nonzero log position ("become
initialized") via a Raft snapshot (this is because we initialize all Ranges in
the system at log index RaftInitialLogIndex which allows us to write arbitrary
amounts of data into the initial state without having to worry about the size
of individual log entries; see WriteInitialReplicaState).

An uninitialized Replica has no notion of its active RangeDescriptor yet, has no
data, and should be thought of as a pure raft peer that can react to incoming
messages (it can't send messages, as it is unaware of who the other members of
the group are; its configuration is zero, consistent with the configuration
represented by "no RangeDescriptor"); in particular it may be asked to cast a
vote to determine Raft leadership. (This should not be required in CockroachDB
today since we always add an uninitialized Replica as a roachpb.LEARNER first
and only promote it after it has successfully received a Raft snapshot; we don't
rely on this fact today)

Uninitialized Replicas should be viewed as an implementation detail that is (or
should be!) invisible to most access to a Store in the context of processing
requests coming from the KV API, as uninitialized Replicas cannot serve such
requests. At the time of writing, uninitialized Replicas need to be handled
in many code paths that should never encounter them in the first place. We
will be addressing this with #72374.

Raft snapshots can also be directed at initialized Replicas. For practical
reasons, we cannot preserve the entire committed replicated log forever and
periodically purge a prefix that is known to be durably applied on a quorum of
peers. Under certain circumstances (see newTruncateDecision) this may cut a
follower off from the log, and this follower will require a Raft snapshot before
being able to resume replication. Another (rare) source of snapshots occurs
around Range splits. During a split, the involved Replicas shrink and
simultaneously instantiate the right-hand side Replica, but since Replicas carry
out this operation at different times, a "faster" initialized right-hand side
might contact a "slow" store on which the right-hand side has not yet been
instantiated, leading to the creation of an uninitialized Replica that may
request a snapshot. See maybeDelaySplitToAvoidSnapshot.

The diagram is a lot to take in. The various transitions are discussed in
prose below, and the source .dot file is in store_doc_replica_lifecycle.dot.

	                              +---------------------+
	          +------------------ |       Absent        | ---------------------------------------------------------------------------------------------------+
	          |                   +---------------------+                                                                                                    |
	          |                     |                        Subsume              Crash          applySnapshot                                               |
	          |                     | Store.Start          +---------------+    +---------+    +---------------+                                             |
	          |                     v                      v               |    v         |    v               |                                             |
	          |                   +-----------------------------------------------------------------------------------------------------------------------+  |
	+---------+------------------ |                                                                                                                       |  |
	|         |                   |                                                      Initialized                                                      |  |
	|         |                   |                                                                                                                       |  |
	|    +----+------------------ |                                                                                                                       | -+----+
	|    |    |                   +-----------------------------------------------------------------------------------------------------------------------+  |    |
	|    |    |                     |                      ^                    ^                                   |              |                    |    |    |
	|    |    | Raft msg            | Crash                | applySnapshot      | post-split                        |              |                    |    |    |
	|    |    |                     v                      |                    |                                   |              |                    |    |    |
	|    |    |                   +---------------------------------------------------------+  pre-split            |              |                    |    |    |
	|    |    +-----------------> |                                                         | <---------------------+--------------+--------------------+----+    |
	|    |                        |                                                         |                       |              |                    |         |
	|    |                        |                      Uninitialized                      |   Raft msg            |              |                    |         |
	|    |                        |                                                         | -----------------+    |              |                    |         |
	|    |                        |                                                         |                  |    |              |                    |         |
	|    |                        |                                                         | <----------------+    |              |                    |         |
	|    |                        +---------------------------------------------------------+                       |              | apply removal      |         |
	|    |                          |                      |                                                        |              |                    |         |
	|    |                          | ReplicaTooOldError   | higher ReplicaID                                       | Replica GC   |                    |         |
	|    |                          v                      v                                                        v              |                    |         |
	|    |   Merged (snapshot)    +---------------------------------------------------------------------------------------------+  |                    |         |
	|    +----------------------> |                                                                                             | <+                    |         |
	|                             |                                                                                             |                       |         |
	|        apply Merge          |                                                                                             |  ReplicaTooOld        |         |
	+---------------------------> |                                           Removed                                           | <---------------------+         |
	                              |                                                                                             |                                 |
	                              |                                                                                             |  higher ReplicaID               |
	                              |                                                                                             | <-------------------------------+
	                              +---------------------------------------------------------------------------------------------+

When a Store starts, it iterates through all RangeDescriptors it can find on its
Engine. Finding a RangeDescriptor by definition implies that the Replica is
initialized. Raft state (a raftpb.HardState) for uninitialized Replicas may
exist, however it would be ignored until a message arrives addressing that
Replica, or a split trigger applies that instantiates and then initializes it.

Uninitialized Replicas principally arise when a replication change occurs and a
new member is added to a Range. This new member will be contacted by the Raft
leader, creating an uninitialized Replica on the recipient which will then
negotiate a snapshot to become initialized and to receive the replicated log. A
split can be understood as a special case of that, except that all members of
the Range are created at around the same time (though in practice with arbitrary
delays due to the usual distributed systems reasons), and the uninitialized
Replica creation is triggered by the split trigger executing on the left-hand
side of the split, or a Replica of the right-hand side (already initialized by
the split trigger on another Store) reaching out, whichever occurs first.

An uninitialized Replica requires a snapshot to become initialized. The case in
which the Replica is the right-hand side of a split can be understood as the
application of a snapshot as well (though this is not reflected in code at the
time of writing) where the left-hand side applies the split by (logically)
moving any data past the split point to the right-hand side Replica, thus
initializing it. In principle, since writes to the state machine do not need to
be made durable, it is conceivable that a split or snapshot could "unapply" due
to an ill-timed crash (though snapshots currently use SST ingestion, which the
storage engine performs durably). Similarly, entry application (which only
occurs on initialized Replicas) is not synced and so a suffix of applied entries
may need to be re-applied following a crash.

If an uninitialized Replica receives a Raft message from a peer informing it
that it is no longer part of a more up-to-date Range configuration (via a
ReplicaTooOldError) or that it has been removed and re-added under a higher
ReplicaID, the uninitialized Replica is removed. There is currently no
general-purpose mechanism to determine whether an uninitialized Replica is
outdated; an uninitialized Replica could in principle leak "forever" if the
Range quickly changes its members such that the triggers mentioned here don't
apply. A full scan of meta2 would be required as there is no RangeID-keyed index
on the RangeDescriptors (the meta ranges are keyed on the EndKey, which allows
routing requests based on the key ranges they touch). The fact that
uninitialized Replicas can be removed has to be taken into account by splits as
well; the split trigger may find that the right-hand side uninitialized Replica
has already been removed, in which case the right half of the split has to be
discarded (see acquireSplitLock and splitPostApply).

Initialized Replicas represent the common case. They can apply snapshots
(required if they get cut off from the raft log via log truncation) which will
always move the applied log position forward, however this is rare. A Replica
typically spends most of its life applying log entries and/or serving reads. The
mechanisms that can lead to removal of uninitialized Replicas apply to
initialized Replicas in the exact same way, but there are additional ways for an
initialized Replica to be removed. The most general such mechanism is the
replica GC queue, which periodically checks the meta2 copy of the Range's
descriptor (using a consistent read, i.e. reading the latest version). If this
indicates that the Replica on the local Store should no longer exist, the
Replica is removed. Additionally, the Replica may directly witness a change that
indicates a need for a removal. For example, it may apply a change to the range
descriptor that removes it, or it may be destroyed by the application of a merge
on its left neighboring Replica, which may also occur through a snapshot. Merges
are the single most complex reconfiguration operation and can only be touched
upon here. At their core, they will at some point "freeze" the right-hand side
Replicas (via kvpb.SubsumeRequest) to prevent additional read or write
activity, and also ensure that the two sets of Ranges to be merged are
co-located on the same Stores as well as are all initialized.

INVARIANT: An initialized Replica's RangeDescriptor always includes it as a member.

INVARIANT: RangeDescriptor.StartKey is immutable (splits and merges mutate the
EndKey only). In particular, an initialized Replica's StartKey is immutable.

INVARIANT: A Replica's ReplicaID is constant.
NOTE: the way to read this is that a Replica object will not be re-used for
multiple ReplicaIDs. Instead, the existing Replica will be destroyed and a new
Replica instantiated.

These invariants significantly reduce complexity since we do not need to handle
the excluded situations. Particularly a changing replicaID is bug-inducing since
at the Replication layer a change in replica ID is a complete change of
identity, and re-use of in-memory structures poses the threat of erroneously
re-using cached information.

INVARIANT: for each key `k` in the replicated key space, the Generation of the
RangeDescriptor containing is strictly increasing over time (see
RangeDescriptor.Generation).
NOTE: we rely on this invariant for cache coherency in `kvcoord`; it currently
plays no role in `kvserver` though we could use it to improve the handling of
snapshot overlaps; see Store.checkSnapshotOverlapLocked.
NOTE: a key may be owned by different RangeIDs at different points in time due
to Range splits and merges.

INVARIANT: on each Store and for each RangeID, the ReplicaID is strictly
increasing over time (see Replica.setTombstoneKey).
NOTE: to the best of our knowledge, we don't rely on this invariant.
*/
type Store struct {
	Ident                *roachpb.StoreIdent // pointer to catch access before Start() is called
	cfg                  StoreConfig
	internalEngines      internalEngines
	db                   *kv.DB
	tsCache              tscache.Cache           // Most recent timestamps for keys / key ranges
	allocator            allocatorimpl.Allocator // Makes allocation decisions
	replRankings         *ReplicaRankings
	replRankingsByTenant *ReplicaRankingMap
	storeRebalancer      *StoreRebalancer
	rangeIDAlloc         *idalloc.Allocator // Range ID allocator
	leaseQueue           *leaseQueue        // Lease queue
	mvccGCQueue          *mvccGCQueue       // MVCC GC queue
	mergeQueue           *mergeQueue        // Range merging queue
	splitQueue           *splitQueue        // Range splitting queue
	replicateQueue       *replicateQueue    // Replication queue
	replicaGCQueue       *replicaGCQueue    // Replica GC queue
	raftLogQueue         *raftLogQueue      // Raft log truncation queue
	// Carries out truncations proposed by the raft log queue, and "replicated"
	// via raft, when they are safe. Created in Store.Start.
	raftTruncator       *raftLogTruncator
	raftSnapshotQueue   *raftSnapshotQueue          // Raft repair queue
	tsMaintenanceQueue  *timeSeriesMaintenanceQueue // Time series maintenance queue
	scanner             *replicaScanner             // Replica scanner
	consistencyQueue    *consistencyQueue           // Replica consistency check queue
	consistencyLimiter  *quotapool.RateLimiter      // Rate limits consistency checks
	metrics             *StoreMetrics
	intentResolver      *intentresolver.IntentResolver
	recoveryMgr         txnrecovery.Manager
	storeLiveness       storeliveness.Fabric
	syncWaiters         []*logstore.SyncWaiterLoop
	raftEntryCache      *raftentry.Cache
	limiters            batcheval.Limiters
	txnWaitMetrics      *txnwait.Metrics
	sstSnapshotStorage  SSTSnapshotStorage
	protectedtsReader   spanconfig.ProtectedTSReader
	ctSender            *sidetransport.Sender
	storeGossip         *StoreGossip
	rebalanceObjManager *RebalanceObjectiveManager
	// raftTransportForFlowControl exposes the set of (remote) stores the raft
	// transport is connected to, and is used by the canonical
	// replicaFlowControlIntegration implementation.
	raftTransportForFlowControl raftTransportForFlowControl

	// kvflowRangeControllerFactory is used for replication AC (flow control) V2
	// to create new range controllers which mediate the flow of requests to
	// replicas.
	kvflowRangeControllerFactory replica_rac2.RangeControllerFactory

	// metricsMu protects the collection and update of engine metrics.
	metricsMu syncutil.Mutex

	coalescedMu struct {
		syncutil.Mutex
		heartbeats         map[roachpb.StoreIdent][]kvserverpb.RaftHeartbeat
		heartbeatResponses map[roachpb.StoreIdent][]kvserverpb.RaftHeartbeat
	}
	// 1 if the store was started, 0 if it wasn't. To be accessed using atomic
	// ops.
	started int32
	stopper *stop.Stopper
	// The time when the store was Start()ed, in nanos.
	startedAt    int64
	nodeDesc     *roachpb.NodeDescriptor
	initComplete sync.WaitGroup // Signaled by async init tasks

	// Queue to limit concurrent non-empty snapshot application.
	snapshotApplyQueue *multiqueue.MultiQueue

	// Queue to limit concurrent non-empty snapshot sending.
	snapshotSendQueue *multiqueue.MultiQueue

	// draining holds a bool which indicates whether this store is draining. See
	// SetDraining() for a more detailed explanation of behavior changes.
	//
	// TODO(bdarnell,tschottdorf): Would look better inside of `mu`, which at
	// the time of its creation was riddled with deadlock (but that situation
	// has likely improved).
	draining atomic.Bool

	// concurrentRaftTraces is the number of concurrent raft trace requests that
	// are currently registered. This limit is used to prevent extensive raft
	// tracing from inadvertently impacting performance.
	concurrentRaftTraces atomic.Int64

	// Locking notes: To avoid deadlocks, the following lock order must be
	// obeyed: baseQueue.mu < Replica.raftMu < Replica.readOnlyCmdMu < Store.mu
	// < Replica.mu < Replica.unreachablesMu < Store.coalescedMu < Store.scheduler.mu.
	// (It is not required to acquire every lock in sequence, but when multiple
	// locks are held at the same time, it is incorrect to acquire a lock with
	// "lesser" value in this sequence after one with "greater" value).
	//
	// Methods of Store with a "Locked" suffix require that
	// Store.mu.Mutex be held. Other locking requirements are indicated
	// in comments.
	//
	// The locking structure here is complex because A) Store is a
	// container of Replicas, so it must generally be consulted before
	// doing anything with any Replica, B) some Replica operations
	// (including splits) modify the Store. Therefore we generally lock
	// Store.mu to find a Replica, release it, then call a method on the
	// Replica. These short-lived locks of Store.mu and Replica.mu are
	// often surrounded by a long-lived lock of Replica.raftMu as
	// described below.
	//
	// There are two major entry points to this stack of locks:
	// Store.Send (which handles incoming RPCs) and raft-related message
	// processing (including handleRaftReady on the processRaft
	// goroutine and HandleRaftRequest on GRPC goroutines). Reads are
	// processed solely through Store.Send; writes start out on
	// Store.Send until they propose their raft command and then they
	// finish on the raft goroutines.
	//
	// TODO(bdarnell): a Replica could be destroyed immediately after
	// Store.Send finds the Replica and releases the lock. We need
	// another RWMutex to be held by anything using a Replica to ensure
	// that everything is finished before releasing it. #7169
	//
	// Detailed description of the locks:
	//
	// * Replica.raftMu: Held while any raft messages are being processed
	//   (including handleRaftReady and HandleRaftRequest) or while the set of
	//   Replicas in the Store is being changed (which may happen outside of raft
	//   via the replica GC queue).
	//
	//   If holding raftMus for multiple different replicas simultaneously,
	//   acquire the locks in the order that the replicas appear in replicasByKey.
	//
	// * Replica.readOnlyCmdMu (RWMutex): Held in read mode while any
	//   read-only command is in progress on the replica; held in write
	//   mode while executing a commit trigger. This is necessary
	//   because read-only commands mutate the Replica's timestamp cache
	//   (while holding Replica.mu in addition to readOnlyCmdMu). The
	//   RWMutex ensures that no reads are being executed during a split
	//   (which copies the timestamp cache) while still allowing
	//   multiple reads in parallel (#3148). TODO(bdarnell): this lock
	//   only needs to be held during splitTrigger, not all triggers.
	//
	// * baseQueue.mu: The mutex contained in each of the store's queues (such
	//   as the replicate queue, replica GC queue, MVCC GC queue, ...). The mutex is
	//   typically acquired when deciding whether to add a replica to the respective
	//   queue.
	//
	// * Store.mu: Protects the Store's map of its Replicas. Acquired and
	//   released briefly at the start of each request; metadata operations like
	//   splits acquire it again to update the map. Even though these lock
	//   acquisitions do not make up a single critical section, it is safe thanks
	//   to Replica.raftMu which prevents any concurrent modifications.
	//
	// * Replica.mu: Protects the Replica's in-memory state. Acquired
	//   and released briefly as needed (note that while the lock is
	//   held "briefly" in that it is not held for an entire request, we
	//   do sometimes do I/O while holding the lock, as in
	//   Replica.Entries). This lock should be held when calling any
	//   methods on the raft group. Raft may call back into the Replica
	//   via the methods of the raft.Storage interface, which assume the
	//   lock is held even though they do not follow our convention of
	//   the "Locked" suffix.
	//
	// * Store.scheduler.mu: Protects the Raft scheduler internal
	//   state. Callbacks from the scheduler are performed while not holding this
	//   mutex in order to observe the above ordering constraints.
	//
	// Splits and merges deserve special consideration: they operate on two
	// ranges. For splits, this might seem fine because the right-hand range is
	// brand new, but an uninitialized version may have been created by a raft
	// message before we process the split (see commentary on
	// Replica.splitTrigger). We make this safe, for both splits and merges, by
	// locking the right-hand range for the duration of the Raft command
	// containing the split/merge trigger.
	//
	// Note that because we acquire and release Store.mu and Replica.mu
	// repeatedly rather than holding a lock for an entire request, we are
	// actually relying on higher-level locks to ensure that things don't change
	// out from under us. In particular, handleRaftReady accesses the replicaID
	// more than once, and we rely on Replica.raftMu to ensure that this is not
	// modified by a concurrent HandleRaftRequest. (#4476)

	mu struct {
		syncutil.RWMutex
		// Map of replicas by Range ID (map[roachpb.RangeID]*Replica).
		// May be read without holding Store.mu.
		replicasByRangeID syncutil.Map[roachpb.RangeID, Replica]
		// A btree key containing objects of type *Replica or *ReplicaPlaceholder.
		// Both types have an associated key range; the btree is keyed on their
		// start keys.
		//
		// INVARIANT: Any ReplicaPlaceholder in this map is also in replicaPlaceholders.
		// INVARIANT: Any Replica with Replica.IsInitialized()==true is also in replicasByRangeID.
		replicasByKey *storeReplicaBTree
		// creatingReplicas stores IDs of all ranges for which there is an ongoing
		// attempt to create a replica.
		creatingReplicas map[roachpb.RangeID]struct{}
		// All *Replica objects for which Replica.IsInitialized is false. Replicas
		// are added to and removed from this map with Replica.raftMu locked.
		//
		// INVARIANT: any entry in this map is also in replicasByRangeID.
		uninitReplicas map[roachpb.RangeID]*Replica // Map of uninitialized replicas by Range ID
		// replicaPlaceholders is a map to access all placeholders, so they can
		// be directly accessed and cleared after stepping all raft groups.
		//
		// INVARIANT: any entry in this map is also in replicasByKey.
		replicaPlaceholders map[roachpb.RangeID]*ReplicaPlaceholder
	}

	// The unquiesced subset of replicas.
	unquiescedReplicas struct {
		syncutil.Mutex
		m map[roachpb.RangeID]struct{}
	}

	// The subset of replicas with active rangefeeds.
	rangefeedReplicas struct {
		syncutil.Mutex
		// m contains mapping from rangeID that could be used to retrieve replicas
		// with associated rangefeed processor scheduler IDs that allow enqueueing
		// periodic events directly.
		m map[roachpb.RangeID]int64
	}
	rangefeedScheduler *rangefeed.Scheduler

	// raftRecvQueues is a map of per-Replica incoming request queues. These
	// queues might more naturally belong in Replica, but are kept separate to
	// avoid reworking the locking in getOrCreateReplica which requires
	// Replica.raftMu to be held while a replica is being inserted into
	// Store.mu.replicas.
	raftRecvQueues raftReceiveQueues

	scheduler *raftScheduler

	// livenessMap is a map from nodeID to a bool indicating
	// liveness. It is updated periodically in raftTickLoop()
	// and reactively in nodeIsLiveCallback() on liveness updates.
	livenessMap atomic.Value
	// ioThresholds is analogous to livenessMap, but stores the *IOThresholds for
	// the stores in the cluster . It is gossip-backed but is not updated
	// reactively, i.e. will refresh on each tick loop iteration only.
	ioThresholds *ioThresholds

	ioThreshold struct {
		syncutil.Mutex
		t                 *admissionpb.IOThreshold // never nil
		maxL0NumSubLevels *slidingwindow.Swag
		maxL0NumFiles     *slidingwindow.Swag
		maxL0Size         *slidingwindow.Swag
	}

	// lastIOOverloadLeaseShed tracks the last time the store attempted to shed
	// all range leases it held due to becoming IO overloaded.
	lastIOOverloadLeaseShed atomic.Value

	counts struct {
		// Number of placeholders removed due to error. Not a good fit for meaningful
		// metrics, as snapshots to initialized ranges don't get a placeholder.
		failedPlaceholders int32
		// Number of placeholders successfully filled by a snapshot. Not a good fit
		// for meaningful metrics, as snapshots to initialized ranges don't get a
		// placeholder.
		filledPlaceholders int32
		// Number of placeholders removed due to a snapshot that was dropped by
		// raft. Not a good fit for meaningful metrics, as snapshots to initialized
		// ranges don't get a placeholder.
		droppedPlaceholders int32
	}

	// tenantRateLimiters manages tenantrate.Limiters
	tenantRateLimiters *tenantrate.LimiterFactory

	// eagerLeaseAcquisitionLimiter limits the number of concurrent eager lease
	// acquisitions made during Raft ticks.
	eagerLeaseAcquisitionLimiter *quotapool.IntPool

	computeInitialMetrics              sync.Once
	systemConfigUpdateQueueRateLimiter *quotapool.RateLimiter

	rangeFeedSlowClosedTimestampNudge *singleflight.Group

	// diskMonitor provides metrics for the disk associated with this store.
	diskMonitor *disk.Monitor
}

var _ kv.Sender = &Store{}
var _ IncomingRaftMessageHandler = &Store{}
var _ OutgoingRaftMessageHandler = &Store{}

// A StoreConfig encompasses the auxiliary objects and configuration
// required to create a store.
// All fields holding a pointer or an interface are required to create
// a store; the rest will have sane defaults set if omitted.
// TODO(baptist): Split into StoreConfig (immutable) and NodeState (mutable).
type StoreConfig struct {
	AmbientCtx log.AmbientContext
	base.RaftConfig

	DefaultSpanConfig      roachpb.SpanConfig
	Settings               *cluster.Settings
	Clock                  *hlc.Clock
	Gossip                 *gossip.Gossip
	DB                     *kv.DB
	NodeLiveness           *liveness.NodeLiveness
	StorePool              *storepool.StorePool
	Transport              *RaftTransport
	StoreLivenessTransport *storeliveness.Transport
	NodeDialer             *nodedialer.Dialer
	RPCContext             *rpc.Context
	RangeDescriptorCache   *rangecache.RangeCache

	ClosedTimestampSender   *sidetransport.Sender
	ClosedTimestampReceiver sidetransportReceiver

	// TimeSeriesDataStore is an interface used by the store's time series
	// maintenance queue to dispatch individual maintenance tasks.
	TimeSeriesDataStore TimeSeriesDataStore

	// CoalescedHeartbeatsInterval is the interval for which heartbeat messages
	// are queued and then sent as a single coalesced heartbeat; it is a
	// fraction of the RaftTickInterval so that heartbeats don't get delayed by
	// an entire tick. Delaying coalescing heartbeat responses has a bad
	// interaction with quiescence because the coalesced (delayed) heartbeat
	// response can unquiesce the leader. Consider:
	//
	// T+0: leader queues MsgHeartbeat
	// T+1: leader sends MsgHeartbeat
	//                                        follower receives MsgHeartbeat
	//                                        follower queues MsgHeartbeatResp
	// T+2: leader queues quiesce message
	//                                        follower sends MsgHeartbeatResp
	//      leader receives MsgHeartbeatResp
	// T+3: leader sends quiesce message
	//
	// Thus we want to make sure that heartbeats are responded to faster than
	// the quiesce cadence.
	CoalescedHeartbeatsInterval time.Duration

	// ScanInterval is the default value for the scan interval
	ScanInterval time.Duration

	// ScanMinIdleTime is the minimum time the scanner will be idle between ranges.
	// If enabled (> 0), the scanner may complete in more than ScanInterval for
	// stores with many ranges.
	ScanMinIdleTime time.Duration

	// ScanMaxIdleTime is the maximum time the scanner will be idle between ranges.
	// If enabled (> 0), the scanner may complete in less than ScanInterval for small
	// stores.
	ScanMaxIdleTime time.Duration

	// If LogRangeAndNodeEvents is true, major changes to ranges will be logged into
	// the range event log (system.rangelog table) and node join and restart
	// events will be logged into the event log (system.eventlog table).
	// Note that node Decommissioning events are always logged.
	LogRangeAndNodeEvents bool

	// RaftSchedulerConcurrency specifies the number of Raft scheduler workers
	// for this store. Values < 1 imply 1.
	RaftSchedulerConcurrency int

	// RaftSchedulerConcurrentPriority specifies the number of Raft scheduler
	// workers for this store's dedicated priority shard. Values < 1 imply 1.
	RaftSchedulerConcurrencyPriority int

	// RaftSchedulerShardSize specifies the maximum number of Raft scheduler
	// workers per mutex shard. Values < 1 imply 1.
	RaftSchedulerShardSize int

	// RaftEntryCacheSize is the size in bytes of the Raft log entry cache
	// shared by all Raft groups managed by the store.
	RaftEntryCacheSize uint64

	// IntentResolverTaskLimit is the maximum number of asynchronous tasks that
	// may be started by the intent resolver. -1 indicates no asynchronous tasks
	// are allowed. 0 uses the default value (defaultIntentResolverTaskLimit)
	// which is non-zero.
	IntentResolverTaskLimit int

	TestingKnobs StoreTestingKnobs

	// EagerLeaseAcquisitionLimiter is used to limit the number of concurrent
	// eager lease extensions. Normally shared between all stores on a node.
	// Can be nil, which disables the limit.
	EagerLeaseAcquisitionLimiter *quotapool.IntPool

	// HistogramWindowInterval is (server.Config).HistogramWindowInterval
	HistogramWindowInterval time.Duration

	// ProtectedTimestampReader provides a read-only view into the protected
	// timestamp subsystem. It is queried during the GC process.
	ProtectedTimestampReader spanconfig.ProtectedTSReader

	// KV Memory Monitor. Must be non-nil for production, and can be nil in some
	// tests.
	KVMemoryMonitor        *mon.BytesMonitor
	RangefeedBudgetFactory *rangefeed.BudgetFactory
	RaftEntriesMonitor     *logstore.SoftLimit // tracks memory used by raft entries

	// SpanConfigsDisabled determines whether we're able to use the span configs
	// infrastructure or not.
	//
	// TODO(baptist): Don't add any future uses of this. Will be removed soon.
	SpanConfigsDisabled bool

	// Used to subscribe to span configuration changes, keeping up-to-date a
	// data structure useful for retrieving span configs.
	SpanConfigSubscriber spanconfig.KVSubscriber
	// SharedStorageEnabled stores whether this store is configured with a
	// shared.Storage instance and can accept shared snapshots.
	SharedStorageEnabled bool

	// KVAdmissionController is used for admission control.
	KVAdmissionController kvadmission.Controller
	// KVFlowController is used for replication admission control.
	KVFlowController kvflowcontrol.Controller
	// KVFlowHandles is used for replication admission control.
	KVFlowHandles kvflowcontrol.Handles
	// KVFlowHandleMetrics is a shared metrics struct for all
	// kvflowcontrol.Handles.
	KVFlowHandleMetrics *kvflowhandle.Metrics
	// KVFlowAdmittedPiggybacker is used for replication AC (flow control) v2.
	KVFlowAdmittedPiggybacker replica_rac2.AdmittedPiggybacker
	// KVFlowStreamTokenProvider is used for replication AC (flow control) v2 to
	// provide token counters for replication streams.
	KVFlowStreamTokenProvider *rac2.StreamTokenCounterProvider
	// KVFlowSendTokenWatcher is used for replication AC (flow control) v2 to
	// watch for elastic send tokens.
	KVFlowSendTokenWatcher *rac2.SendTokenWatcher
	// KVFlowWaitForEvalConfig is used for configuring WaitForEval for
	// replication AC (flow control) v2.
	KVFlowWaitForEvalConfig *rac2.WaitForEvalConfig
	// KVFlowEvalWaitMetrics is used for replication AC (flow control) v2 to
	// track requests waiting for evaluation.
	KVFlowEvalWaitMetrics *rac2.EvalWaitMetrics
	// KVFlowRangeControllerMetrics is used for replication AC (flow control) v2
	// to track various range controller metrics.
	KVFlowRangeControllerMetrics *rac2.RangeControllerMetrics

	// SchedulerLatencyListener listens in on scheduling latencies, information
	// that's then used to adjust various admission control components (like how
	// many CPU tokens are granted to elastic work like backups).
	SchedulerLatencyListener admission.SchedulerLatencyListener

	// SystemConfigProvider is used to drive replication decision-making in the
	// mixed-version state, before the span configuration infrastructure has been
	// bootstrapped.
	//
	// TODO(ajwerner): Remove in 22.2.
	SystemConfigProvider config.SystemConfigProvider

	// RangeLogWriter is used to write entries to the system.rangelog table.
	RangeLogWriter RangeLogWriter

	// RangeFeedSchedulerConcurrency specifies number of rangefeed scheduler
	// workers for the store.
	RangeFeedSchedulerConcurrency int

	// RangeFeedSchedulerConcurrentPriority specifies the number of rangefeed
	// scheduler workers for this store's dedicated priority shard. Values < 1
	// imply 1.
	RangeFeedSchedulerConcurrencyPriority int

	// RangeFeedSchedulerShardSize specifies the maximum number of workers per
	// scheduler shard.
	RangeFeedSchedulerShardSize int

	// RangeCount is populated by the node and represents the total number of
	// ranges this node has.
	RangeCount *atomic.Int64
}

// logRangeAndNodeEventsEnabled is used to enable or disable logging range events
// (e.g., split, merge, add/remove voter/non-voter) into the system.rangelog
// table and node join and restart events into system.eventolog table.
// Decommissioning events are not controlled by this setting.
var logRangeAndNodeEventsEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.log_range_and_node_events.enabled",
	"set to true to transactionally log range events"+
		" (e.g., split, merge, add/remove voter/non-voter) into system.rangelog"+
		"and node join and restart events into system.eventolog",
	true,
	settings.WithPublic)

// ConsistencyTestingKnobs is a BatchEvalTestingKnobs struct used to control the
// behavior of the consistency checker for tests.
type ConsistencyTestingKnobs struct {
	// If non-nil, OnBadChecksumFatal is called on a replica with a mismatching
	// checksum, instead of log.Fatal.
	OnBadChecksumFatal func(roachpb.StoreIdent)

	ConsistencyQueueResultHook func(response kvpb.CheckConsistencyResponse)
}

// Valid returns true if the StoreConfig is populated correctly.
// We don't check for Gossip and DB since some of our tests pass
// that as nil.
func (sc *StoreConfig) Valid() bool {
	return sc.Clock != nil && sc.Transport != nil &&
		sc.RaftTickInterval != 0 && sc.RaftHeartbeatIntervalTicks > 0 &&
		sc.RaftElectionTimeoutTicks > 0 && sc.RaftReproposalTimeoutTicks > 0 &&
		sc.RaftSchedulerConcurrency > 0 && sc.RaftSchedulerConcurrencyPriority > 0 &&
		sc.RaftSchedulerShardSize > 0 && sc.ScanInterval >= 0 && sc.AmbientCtx.Tracer != nil &&
		sc.RangeFeedSchedulerConcurrency > 0 && sc.RangeFeedSchedulerShardSize > 0
}

// SetDefaults initializes unset fields in StoreConfig to values
// suitable for use on a local network.
// TODO(tschottdorf): see if this ought to be configurable via flags.
func (sc *StoreConfig) SetDefaults(numStores int) {
	sc.RaftConfig.SetDefaults()

	if sc.CoalescedHeartbeatsInterval == 0 {
		sc.CoalescedHeartbeatsInterval = sc.RaftTickInterval / 2
	}
	if sc.RaftSchedulerConcurrency == 0 {
		sc.RaftSchedulerConcurrency = defaultRaftSchedulerConcurrency
		// If we have more than one store, evenly divide the default workers across
		// stores, since the default value is a function of CPU count and should not
		// scale with the number of stores. However, we place a floor on the number
		// of workers for each store to ensure that small imbalances in load do not
		// starve a single store's Raft scheduler.
		if numStores > 1 && sc.RaftSchedulerConcurrency > 1 {
			sc.RaftSchedulerConcurrency = (sc.RaftSchedulerConcurrency-1)/numStores + 1 // ceil division
			sc.RaftSchedulerConcurrency = max(sc.RaftSchedulerConcurrency, defaultRaftSchedulerMinConcurrencyPerStore)
		}
	}
	if sc.RaftSchedulerConcurrencyPriority == 0 {
		sc.RaftSchedulerConcurrencyPriority = defaultRaftSchedulerPriorityShardSize
	}
	if sc.RaftSchedulerShardSize == 0 {
		sc.RaftSchedulerShardSize = defaultRaftSchedulerShardSize
	}
	if sc.RaftEntryCacheSize == 0 {
		sc.RaftEntryCacheSize = uint64(defaultRaftEntryCacheSize)
		if numStores > 1 { // guard against zero division
			sc.RaftEntryCacheSize /= uint64(numStores)
		}
	}
	if raftDisableLeaderFollowsLeaseholder {
		sc.TestingKnobs.DisableLeaderFollowsLeaseholder = true
		sc.TestingKnobs.AllowLeaseRequestProposalsWhenNotLeader = true // otherwise lease requests fail
		// The allocator must skip snapshot checks, since these only work when the
		// leader and leaseholder are colocated.
		if sc.TestingKnobs.AllocatorKnobs == nil {
			sc.TestingKnobs.AllocatorKnobs = &allocator.TestingKnobs{}
		}
		sc.TestingKnobs.AllocatorKnobs.AllowLeaseTransfersToReplicasNeedingSnapshots = true
		sc.TestingKnobs.ReplicaPlannerKnobs.AllowVoterRemovalWhenNotLeader = true // downreplication
	}
	if raftDisableQuiescence {
		sc.TestingKnobs.DisableQuiescence = true
	}
	if sc.RangeFeedSchedulerConcurrency == 0 {
		sc.RangeFeedSchedulerConcurrency = defaultRangefeedSchedulerConcurrency
		if numStores > 1 && sc.RangeFeedSchedulerConcurrency > 1 {
			// We want at least two workers per store to avoid any blocking.
			sc.RangeFeedSchedulerConcurrency = min(
				(sc.RangeFeedSchedulerConcurrency-1)/numStores+1, // ceil division
				2)
		}
	}
	if sc.RangeFeedSchedulerShardSize == 0 {
		sc.RangeFeedSchedulerShardSize = defaultRangefeedSchedulerShardSize
	}
	if sc.RangeFeedSchedulerConcurrencyPriority == 0 {
		sc.RangeFeedSchedulerConcurrencyPriority = defaultRangefeedSchedulerPriorityShardSize
	}
}

// GetStoreConfig exposes the config used for this store.
func (s *Store) GetStoreConfig() *StoreConfig {
	return &s.cfg
}

// LeaseExpiration returns an int64 to increment a manual clock with to
// make sure that all active range leases expire.
func (sc *StoreConfig) LeaseExpiration() int64 {
	// Due to lease extensions, the remaining interval can be longer than just
	// the sum of the offset (=length of stasis period) and the active
	// duration, but definitely not by 2x.
	maxOffset := sc.Clock.MaxOffset()
	return 2 * (sc.RangeLeaseDuration + maxOffset).Nanoseconds()
}

// Tracer returns the tracer embedded within StoreConfig
func (sc *StoreConfig) Tracer() *tracing.Tracer {
	return sc.AmbientCtx.Tracer
}

// NewStore returns a new instance of a store.
func NewStore(
	ctx context.Context, cfg StoreConfig, eng storage.Engine, nodeDesc *roachpb.NodeDescriptor,
) *Store {
	if !cfg.Valid() {
		log.Fatalf(ctx, "invalid store configuration: %+v", &cfg)
	}
	iot := ioThresholds{}
	iot.Replace(nil, 1.0) // init as empty
	s := &Store{
		// NB: do not access these fields directly. Instead, use
		// the StateEngine, TODOEngine, LogEngine methods.
		// This simplifies going through references to these
		// engines.
		internalEngines: internalEngines{
			stateEngine: eng,
			todoEngine:  eng,
			logEngine:   eng,
		},
		cfg:                               cfg,
		db:                                cfg.DB, // TODO(tschottdorf): remove redundancy.
		nodeDesc:                          nodeDesc,
		metrics:                           newStoreMetrics(cfg.HistogramWindowInterval),
		ctSender:                          cfg.ClosedTimestampSender,
		ioThresholds:                      &iot,
		rangeFeedSlowClosedTimestampNudge: singleflight.NewGroup("rangfeed-ct-nudge", "range"),
	}
	s.ioThreshold.t = &admissionpb.IOThreshold{}
	// Track the maxScore over the last 5 minutes, in one minute windows.
	now := cfg.Clock.Now().GoTime()
	s.ioThreshold.maxL0NumSubLevels = slidingwindow.NewMaxSwag(now, time.Minute, 5)
	s.ioThreshold.maxL0NumFiles = slidingwindow.NewMaxSwag(now, time.Minute, 5)
	s.ioThreshold.maxL0Size = slidingwindow.NewMaxSwag(now, time.Minute, 5)
	var allocatorStorePool storepool.AllocatorStorePool
	var storePoolIsDeterministic bool
	if cfg.StorePool != nil {
		// There are number of test cases that make a test store but don't add
		// gossip or a store pool. So we can't rely on the existence of the
		// store pool in those cases.
		allocatorStorePool = cfg.StorePool
		storePoolIsDeterministic = allocatorStorePool.IsDeterministic()
		allocatorStorePool.SetOnCapacityChange(s.makeIOOverloadCapacityChangeFn())

		s.rebalanceObjManager = newRebalanceObjectiveManager(
			ctx,
			s.cfg.AmbientCtx,
			s.cfg.Settings,
			func(ctx context.Context, obj LBRebalancingObjective) {
				s.VisitReplicas(func(r *Replica) (wantMore bool) {
					r.loadBasedSplitter.SetSplitObjective(
						s.Clock().PhysicalTime(),
						obj.ToSplitObjective(),
					)
					return true
				})
			},
			allocatorStorePool, /* storeDescProvider */
			allocatorStorePool, /* capacityChangeNotifier */
		)
	}
	if cfg.RPCContext != nil {
		s.allocator = allocatorimpl.MakeAllocator(
			cfg.Settings,
			storePoolIsDeterministic,
			cfg.RPCContext.RemoteClocks.Latency,
			cfg.TestingKnobs.AllocatorKnobs,
		)
	} else {
		s.allocator = allocatorimpl.MakeAllocator(
			cfg.Settings,
			storePoolIsDeterministic,
			func(id roachpb.NodeID) (time.Duration, bool) {
				return 0, false
			}, cfg.TestingKnobs.AllocatorKnobs,
		)
	}
	if s.metrics != nil {
		s.metrics.registry.AddMetricStruct(s.allocator.Metrics.LoadBasedLeaseTransferMetrics)
		s.metrics.registry.AddMetricStruct(s.allocator.Metrics.LoadBasedReplicaRebalanceMetrics)
	}

	s.replRankings = NewReplicaRankings()
	s.replRankingsByTenant = NewReplicaRankingsMap()

	s.raftRecvQueues.mon = mon.NewUnlimitedMonitor(ctx, mon.Options{
		Name:     "raft-receive-queue",
		CurCount: s.metrics.RaftRcvdQueuedBytes,
		Settings: cfg.Settings,
	})

	s.cfg.RangeLogWriter = newWrappedRangeLogWriter(
		s.metrics.getCounterForRangeLogEventType,
		func() bool {
			return cfg.LogRangeAndNodeEvents &&
				logRangeAndNodeEventsEnabled.Get(&cfg.Settings.SV)
		},
		cfg.RangeLogWriter,
	)

	// NB: buffer up to RaftElectionTimeoutTicks in Raft scheduler to avoid
	// unnecessary elections when ticks are temporarily delayed and piled up.
	s.scheduler = newRaftScheduler(cfg.AmbientCtx, s.metrics, s,
		cfg.RaftSchedulerConcurrency, cfg.RaftSchedulerShardSize, cfg.RaftSchedulerConcurrencyPriority,
		cfg.RaftElectionTimeoutTicks)

	// kvflowRangeControllerFactory depends on the raft scheduler, so it must be
	// created per-store rather than per-node like other replication admission
	// control (flow control) v2 components.
	s.kvflowRangeControllerFactory = replica_rac2.NewRangeControllerFactoryImpl(
		s.Clock(),
		s.cfg.KVFlowEvalWaitMetrics,
		s.cfg.KVFlowRangeControllerMetrics,
		s.cfg.KVFlowStreamTokenProvider,
		replica_rac2.NewStreamCloseScheduler(
			s.stopper, timeutil.DefaultTimeSource{}, s.scheduler),
		(*racV2Scheduler)(s.scheduler),
		s.cfg.KVFlowSendTokenWatcher,
		s.cfg.KVFlowWaitForEvalConfig,
		s.TestingKnobs().FlowControlTestingKnobs,
	)

	// Run a log SyncWaiter loop for every 32 raft scheduler goroutines.
	// Experiments on c5d.12xlarge instances (48 vCPUs, the largest single-socket
	// instance AWS offers) show that with fewer SyncWaiters, raft log callback
	// processing can become a bottleneck for write heavy workloads, which can
	// drive about 100k raft log appends per second, per store.
	numSyncWaiters := (cfg.RaftSchedulerConcurrency-1)/32 + 1 // ceil division
	s.syncWaiters = make([]*logstore.SyncWaiterLoop, numSyncWaiters)
	for i := range s.syncWaiters {
		s.syncWaiters[i] = logstore.NewSyncWaiterLoop()
	}

	s.raftEntryCache = raftentry.NewCache(cfg.RaftEntryCacheSize)
	s.metrics.registry.AddMetricStruct(s.raftEntryCache.Metrics())

	s.coalescedMu.Lock()
	s.coalescedMu.heartbeats = map[roachpb.StoreIdent][]kvserverpb.RaftHeartbeat{}
	s.coalescedMu.heartbeatResponses = map[roachpb.StoreIdent][]kvserverpb.RaftHeartbeat{}
	s.coalescedMu.Unlock()

	s.mu.Lock()
	s.mu.replicaPlaceholders = map[roachpb.RangeID]*ReplicaPlaceholder{}
	s.mu.replicasByKey = newStoreReplicaBTree()
	s.mu.creatingReplicas = map[roachpb.RangeID]struct{}{}
	s.mu.uninitReplicas = map[roachpb.RangeID]*Replica{}
	s.mu.Unlock()

	s.unquiescedReplicas.Lock()
	s.unquiescedReplicas.m = map[roachpb.RangeID]struct{}{}
	s.unquiescedReplicas.Unlock()

	s.rangefeedReplicas.Lock()
	s.rangefeedReplicas.m = map[roachpb.RangeID]int64{}
	s.rangefeedReplicas.Unlock()

	s.tsCache = tscache.New(cfg.Clock)
	s.metrics.registry.AddMetricStruct(s.tsCache.Metrics())

	s.txnWaitMetrics = txnwait.NewMetrics(cfg.HistogramWindowInterval)
	s.metrics.registry.AddMetricStruct(s.txnWaitMetrics)
	s.snapshotApplyQueue = multiqueue.NewMultiQueue(int(snapshotApplyLimit.Get(&cfg.Settings.SV)))
	snapshotApplyLimit.SetOnChange(&cfg.Settings.SV, func(ctx context.Context) {
		s.snapshotApplyQueue.UpdateConcurrencyLimit(int(snapshotApplyLimit.Get(&cfg.Settings.SV)))
	})
	s.snapshotSendQueue = multiqueue.NewMultiQueue(int(SnapshotSendLimit.Get(&cfg.Settings.SV)))
	SnapshotSendLimit.SetOnChange(&cfg.Settings.SV, func(ctx context.Context) {
		s.snapshotSendQueue.UpdateConcurrencyLimit(int(SnapshotSendLimit.Get(&cfg.Settings.SV)))
	})

	s.consistencyLimiter = quotapool.NewRateLimiter(
		"ConsistencyQueue",
		quotapool.Limit(consistencyCheckRate.Get(&cfg.Settings.SV)),
		consistencyCheckRate.Get(&cfg.Settings.SV)*consistencyCheckRateBurstFactor,
		quotapool.WithMinimumWait(consistencyCheckRateMinWait))

	consistencyCheckRate.SetOnChange(&cfg.Settings.SV, func(ctx context.Context) {
		rate := consistencyCheckRate.Get(&cfg.Settings.SV)
		s.consistencyLimiter.UpdateLimit(quotapool.Limit(rate), rate*consistencyCheckRateBurstFactor)
	})

	s.limiters.BulkIOWriteRate = rate.NewLimiter(rate.Limit(bulkIOWriteLimit.Get(&cfg.Settings.SV)), kvserverbase.BulkIOWriteBurst)
	bulkIOWriteLimit.SetOnChange(&cfg.Settings.SV, func(ctx context.Context) {
		s.limiters.BulkIOWriteRate.SetLimit(rate.Limit(bulkIOWriteLimit.Get(&cfg.Settings.SV)))
	})
	s.limiters.ConcurrentExportRequests = limit.MakeConcurrentRequestLimiter(
		"exportRequestLimiter", int(exportRequestsLimit.Get(&cfg.Settings.SV)),
	)
	s.eagerLeaseAcquisitionLimiter = cfg.EagerLeaseAcquisitionLimiter

	// The snapshot storage is usually empty at this point since it is cleared
	// after each snapshot application, except when the node crashed right before
	// it can clean it up. If this fails it's not a correctness issue since the
	// storage is also cleared before receiving a snapshot.
	//
	// TODO(sep-raft-log): need a snapshot storage per engine since we'll need to split
	// the SSTs. Or probably we don't need snapshots on the raft SST at all - the reason
	// we use them now is because we want snapshot apply to be completely atomic but that
	// is out the window with two engines, so we may as well break the atomicity in the
	// common case and do something more effective.
	s.sstSnapshotStorage = NewSSTSnapshotStorage(s.TODOEngine(), s.limiters.BulkIOWriteRate)
	if err := s.sstSnapshotStorage.Clear(); err != nil {
		log.Warningf(ctx, "failed to clear snapshot storage: %v", err)
	}
	s.protectedtsReader = cfg.ProtectedTimestampReader
	s.raftTransportForFlowControl = cfg.Transport

	// On low-CPU instances, a default limit value may still allow ExportRequests
	// to tie up all cores so cap limiter at cores-1 when setting value is higher.
	exportCores := runtime.GOMAXPROCS(0) - 1
	if exportCores < 1 {
		exportCores = 1
	}
	exportRequestsLimit.SetOnChange(&cfg.Settings.SV, func(ctx context.Context) {
		limit := int(exportRequestsLimit.Get(&cfg.Settings.SV))
		if limit > exportCores {
			limit = exportCores
		}
		s.limiters.ConcurrentExportRequests.SetLimit(limit)
	})
	s.limiters.ConcurrentAddSSTableRequests = limit.MakeConcurrentRequestLimiter(
		"addSSTableRequestLimiter", int(addSSTableRequestLimit.Get(&cfg.Settings.SV)),
	)
	addSSTableRequestLimit.SetOnChange(&cfg.Settings.SV, func(ctx context.Context) {
		s.limiters.ConcurrentAddSSTableRequests.SetLimit(
			int(addSSTableRequestLimit.Get(&cfg.Settings.SV)))
	})
	s.limiters.ConcurrentAddSSTableAsWritesRequests = limit.MakeConcurrentRequestLimiter(
		"addSSTableAsWritesRequestLimiter", int(addSSTableAsWritesRequestLimit.Get(&cfg.Settings.SV)),
	)
	addSSTableAsWritesRequestLimit.SetOnChange(&cfg.Settings.SV, func(ctx context.Context) {
		s.limiters.ConcurrentAddSSTableAsWritesRequests.SetLimit(
			int(addSSTableAsWritesRequestLimit.Get(&cfg.Settings.SV)))
	})
	s.limiters.ConcurrentRangefeedIters = limit.MakeConcurrentRequestLimiter(
		"rangefeedIterLimiter", int(concurrentRangefeedItersLimit.Get(&cfg.Settings.SV)),
	)
	concurrentRangefeedItersLimit.SetOnChange(&cfg.Settings.SV, func(ctx context.Context) {
		s.limiters.ConcurrentRangefeedIters.SetLimit(
			int(concurrentRangefeedItersLimit.Get(&cfg.Settings.SV)))
	})

	authorizer := cfg.TestingKnobs.TenantRateKnobs.Authorizer
	if cfg.RPCContext != nil && cfg.RPCContext.TenantRPCAuthorizer != nil {
		authorizer = cfg.RPCContext.TenantRPCAuthorizer
	}
	if authorizer == nil {
		log.Fatalf(ctx, "programming error: missing authorizer from config")
	}

	s.tenantRateLimiters = tenantrate.NewLimiterFactory(&cfg.Settings.SV, &cfg.TestingKnobs.TenantRateKnobs, authorizer)
	s.metrics.registry.AddMetricStruct(s.tenantRateLimiters.Metrics())

	s.systemConfigUpdateQueueRateLimiter = quotapool.NewRateLimiter(
		"SystemConfigUpdateQueue",
		quotapool.Limit(queueAdditionOnSystemConfigUpdateRate.Get(&cfg.Settings.SV)),
		queueAdditionOnSystemConfigUpdateBurst.Get(&cfg.Settings.SV))
	updateSystemConfigUpdateQueueLimits := func(ctx context.Context) {
		s.systemConfigUpdateQueueRateLimiter.UpdateLimit(
			quotapool.Limit(queueAdditionOnSystemConfigUpdateRate.Get(&cfg.Settings.SV)),
			queueAdditionOnSystemConfigUpdateBurst.Get(&cfg.Settings.SV))
	}
	queueAdditionOnSystemConfigUpdateRate.SetOnChange(&cfg.Settings.SV,
		updateSystemConfigUpdateQueueLimits)
	queueAdditionOnSystemConfigUpdateBurst.SetOnChange(&cfg.Settings.SV,
		updateSystemConfigUpdateQueueLimits)

	if s.cfg.Gossip != nil {
		s.storeGossip = NewStoreGossip(cfg.Gossip,
			s, cfg.TestingKnobs.GossipTestingKnobs, &cfg.Settings.SV, timeutil.DefaultTimeSource{})

		// Add range scanner and configure with queues.
		s.scanner = newReplicaScanner(
			s.cfg.AmbientCtx, s.cfg.Clock, cfg.ScanInterval,
			cfg.ScanMinIdleTime, cfg.ScanMaxIdleTime, newStoreReplicaVisitor(s),
		)
		s.leaseQueue = newLeaseQueue(s, s.allocator)
		s.mvccGCQueue = newMVCCGCQueue(s)
		s.mergeQueue = newMergeQueue(s, s.db)
		s.splitQueue = newSplitQueue(s, s.db)
		s.replicateQueue = newReplicateQueue(s, s.allocator)
		s.replicaGCQueue = newReplicaGCQueue(s, s.db)
		s.raftLogQueue = newRaftLogQueue(s, s.db)
		s.raftSnapshotQueue = newRaftSnapshotQueue(s)
		s.consistencyQueue = newConsistencyQueue(s)
		// NOTE: If more queue types are added, please also add them to the list of
		// queues on the EnqueueRange debug page as defined in
		// pkg/ui/src/views/reports/containers/enqueueRange/index.tsx
		s.scanner.AddQueues(
			s.mvccGCQueue, s.mergeQueue, s.splitQueue, s.replicateQueue, s.replicaGCQueue,
			s.raftLogQueue, s.raftSnapshotQueue, s.consistencyQueue, s.leaseQueue)
		tsDS := s.cfg.TimeSeriesDataStore
		if s.cfg.TestingKnobs.TimeSeriesDataStore != nil {
			tsDS = s.cfg.TestingKnobs.TimeSeriesDataStore
		}
		if tsDS != nil {
			s.tsMaintenanceQueue = newTimeSeriesMaintenanceQueue(
				s, s.db, tsDS,
			)
			s.scanner.AddQueues(s.tsMaintenanceQueue)
		}
	}

	if cfg.TestingKnobs.DisableGCQueue {
		s.testingSetGCQueueActive(false)
	}
	if cfg.TestingKnobs.DisableLeaseQueue {
		s.TestingSetLeaseQueueActive(false)
	}
	if cfg.TestingKnobs.DisableMergeQueue {
		s.testingSetMergeQueueActive(false)
	}
	if cfg.TestingKnobs.DisableRaftLogQueue {
		s.testingSetRaftLogQueueActive(false)
	}
	if cfg.TestingKnobs.DisableReplicaGCQueue {
		s.testingSetReplicaGCQueueActive(false)
	}
	if cfg.TestingKnobs.DisableReplicateQueue {
		s.TestingSetReplicateQueueActive(false)
	}
	if cfg.TestingKnobs.DisableSplitQueue {
		s.TestingSetSplitQueueActive(false)
	}
	if cfg.TestingKnobs.DisableTimeSeriesMaintenanceQueue {
		s.testingSetTimeSeriesMaintenanceQueueActive(false)
	}
	if cfg.TestingKnobs.DisableRaftSnapshotQueue {
		s.testingSetRaftSnapshotQueueActive(false)
	}
	if cfg.TestingKnobs.DisableConsistencyQueue {
		s.testingSetConsistencyQueueActive(false)
	}
	if cfg.TestingKnobs.DisableScanner {
		s.testingSetScannerActive(false)
	}

	return s
}

// String formats a store for debug output.
func (s *Store) String() string {
	return redact.StringWithoutMarkers(s)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (s *Store) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("[n%d,s%d]", s.Ident.NodeID, s.Ident.StoreID)
}

// ClusterSettings returns the node's ClusterSettings.
func (s *Store) ClusterSettings() *cluster.Settings {
	return s.cfg.Settings
}

// AnnotateCtx is a convenience wrapper; see AmbientContext.
func (s *Store) AnnotateCtx(ctx context.Context) context.Context {
	return s.cfg.AmbientCtx.AnnotateCtx(ctx)
}

// SetDraining (when called with 'true') causes incoming lease transfers to be
// rejected, prevents all of the Store's Replicas from acquiring or extending
// range leases, and attempts to transfer away any leases owned.
// When called with 'false', returns to the normal mode of operation.
//
// Note: this code represents ONE round of draining. This code is iterated on
// indefinitely until all leases are transferred away.
// This iteration can be found here: pkg/cli/start.go, pkg/cli/quit.go.
//
// The reporter callback, if non-nil, is called on a best effort basis
// to report work that needed to be done and which may or may not have
// been done by the time this call returns. See the explanation in
// pkg/server/drain.go for details.
func (s *Store) SetDraining(drain bool, reporter func(int, redact.SafeString), verbose bool) {
	s.draining.Store(drain)
	if !drain {
		return
	}

	baseCtx := logtags.AddTag(context.Background(), "drain", nil)

	// In a running server, the code below (transferAllAway and the loop
	// that calls it) does not need to be conditional on messaging by
	// the Stopper. This is because the top level Server calls SetDrain
	// upon a graceful shutdown, and waits until the SetDrain calls
	// completes, at which point the work has terminated on its own. If
	// the top-level server is forcefully shut down, it does not matter
	// if some of the code below is still running.
	//
	// However, the situation is different in unit tests where we also
	// assert there are no leaking goroutines when a test terminates.
	// If a test terminates with a timed out lease transfer, it's
	// possible for the transferAllAway() closure to be still running
	// when the closer shuts down the test server.
	//
	// To prevent this, we add this code here which adds the missing
	// cancel + wait in the particular case where the stopper is
	// completing a shutdown while a graceful SetDrain is still ongoing.
	ctx, cancelFn := s.stopper.WithCancelOnQuiesce(baseCtx)
	defer cancelFn()

	var wg sync.WaitGroup

	transferAllAway := func(transferCtx context.Context) int {
		// Limit the number of concurrent lease transfers.
		const leaseTransferConcurrency = 100
		sem := quotapool.NewIntPool("Store.SetDraining", leaseTransferConcurrency)

		// Incremented for every lease transfer attempted. We try to send the lease
		// away, but this may not reliably work. Instead, we run the surrounding
		// retry loop until there are no leases left (ignoring single-replica
		// ranges).
		var numTransfersAttempted int32
		newStoreReplicaVisitor(s).Visit(func(r *Replica) bool {
			//
			// We need to be careful about the case where the ctx has been canceled
			// prior to the call to (*Stopper).RunAsyncTaskEx(). In that case,
			// the goroutine is not even spawned. However, we don't want to
			// mis-count the missing goroutine as the lack of transfer attempted.
			// So what we do here is immediately increase numTransfersAttempted
			// to count this replica, and then decrease it when it is known
			// below that there is nothing to transfer (not lease holder and
			// not raft leader).
			atomic.AddInt32(&numTransfersAttempted, 1)
			wg.Add(1)
			if err := s.stopper.RunAsyncTaskEx(
				r.AnnotateCtx(ctx),
				stop.TaskOpts{
					TaskName:   "storage.Store: draining replica",
					Sem:        sem,
					WaitForSem: true,
				},
				func(ctx context.Context) {
					defer wg.Done()

					select {
					case <-transferCtx.Done():
						// Context canceled: the timeout loop has decided we've
						// done enough draining
						// (server.shutdown.lease_transfer_iteration.timeout).
						//
						// We need this check here because each call of
						// transferAllAway() traverses all stores/replicas without
						// checking for the timeout otherwise.
						if verbose || log.V(1) {
							log.Infof(ctx, "lease transfer aborted due to exceeded timeout")
						}
						return
					default:
					}

					now := s.Clock().NowAsClockTimestamp()
					var drainingLeaseStatus kvserverpb.LeaseStatus
					for {
						var llHandle *leaseRequestHandle
						r.mu.Lock()
						drainingLeaseStatus = r.leaseStatusAtRLocked(ctx, now)
						_, nextLease := r.getLeaseRLocked()
						if nextLease != (roachpb.Lease{}) && nextLease.OwnedBy(s.StoreID()) {
							llHandle = r.mu.pendingLeaseRequest.JoinRequest()
						}
						r.mu.Unlock()

						if llHandle != nil {
							<-llHandle.C()
							continue
						}
						break
					}

					// Is the lease owned by this store?
					leaseLocallyOwned := drainingLeaseStatus.OwnedBy(s.StoreID())

					// Is there some other replica that we can transfer the lease to?
					// Learner replicas aren't allowed to become the leaseholder or raft
					// leader, so only consider the Voters replicas.
					transferTargetAvailable := len(r.Desc().Replicas().VoterDescriptors()) > 1

					// If so, and the lease is proscribed, we have to reacquire it so that
					// we can transfer it away. Other replicas won't know that this lease
					// is proscribed and not usable by this replica, so failing to
					// transfer the lease away could cause temporary unavailability.
					needsLeaseReacquisition := leaseLocallyOwned && transferTargetAvailable &&
						drainingLeaseStatus.State == kvserverpb.LeaseState_PROSCRIBED

					// Otherwise, if the lease is locally owned and valid, transfer it.
					needsLeaseTransfer := leaseLocallyOwned && transferTargetAvailable &&
						drainingLeaseStatus.State == kvserverpb.LeaseState_VALID

					if !needsLeaseTransfer && !needsLeaseReacquisition {
						// Skip this replica.
						atomic.AddInt32(&numTransfersAttempted, -1)
						return
					}

					if needsLeaseReacquisition {
						// Re-acquire the proscribed lease for this replica so that we can
						// transfer it away during a later iteration.
						desc := r.Desc()
						if verbose || log.V(1) {
							// This logging is useful to troubleshoot incomplete drains.
							log.Infof(ctx, "attempting to acquire proscribed lease %v for range %s",
								drainingLeaseStatus.Lease, desc)
						}

						_, pErr := r.redirectOnOrAcquireLease(ctx)
						if pErr != nil {
							const failFormat = "failed to acquire proscribed lease %s for range %s when draining: %v"
							infoArgs := []interface{}{drainingLeaseStatus.Lease, desc, pErr}
							if verbose {
								log.Dev.Infof(ctx, failFormat, infoArgs...)
							} else {
								log.VErrEventf(ctx, 1 /* level */, failFormat, infoArgs...)
							}
							// The lease reacquisition failed. Either we no longer hold the
							// lease or we will need to attempt to reacquire it again. Either
							// way, handle this on a future iteration.
							return
						}

						// The lease reacquisition succeeded. Proceed to the lease transfer.
					}

					// Note that this code doesn't deal with transferring the Raft
					// leadership. Leadership tries to follow the lease, so when leases
					// are transferred, leadership will be transferred too. For ranges
					// without leases we probably should try to move the leadership
					// manually to a non-draining replica.

					desc, conf := r.DescAndSpanConfig()

					if verbose || log.V(1) {
						// This logging is useful to troubleshoot incomplete drains.
						log.Infof(ctx, "attempting to transfer lease %v for range %s", drainingLeaseStatus.Lease, desc)
					}

					start := timeutil.Now()
					transferStatus, err := s.replicateQueue.shedLease(
						ctx,
						r,
						desc,
						conf,
						allocator.TransferLeaseOptions{ExcludeLeaseRepl: true},
					)
					duration := timeutil.Since(start).Microseconds()

					if transferStatus != allocator.TransferOK {
						const failFormat = "failed to transfer lease %s for range %s when draining: %v"
						const durationFailFormat = "blocked for %d microseconds on transfer attempt"

						infoArgs := []interface{}{
							drainingLeaseStatus.Lease,
							desc,
						}
						if err != nil {
							infoArgs = append(infoArgs, err)
						} else {
							infoArgs = append(infoArgs, transferStatus)
						}

						if verbose {
							log.Dev.Infof(ctx, failFormat, infoArgs...)
							log.Dev.Infof(ctx, durationFailFormat, duration)
						} else {
							log.VErrEventf(ctx, 1 /* level */, failFormat, infoArgs...)
							log.VErrEventf(ctx, 1 /* level */, durationFailFormat, duration)
						}
					}
				}); err != nil {
				if verbose || log.V(1) {
					log.Errorf(ctx, "error running draining task: %+v", err)
				}
				wg.Done()
				return false
			}
			return true
		})
		wg.Wait()
		return int(numTransfersAttempted)
	}

	// Give all replicas at least one chance to transfer.
	// If we don't do that, then it's possible that a configured
	// value for raftLeadershipTransferWait is too low to iterate
	// through all the replicas at least once, and the drain
	// condition on the remaining value will never be reached.
	if numRemaining := transferAllAway(ctx); numRemaining > 0 {
		// Report progress to the Drain RPC.
		if reporter != nil {
			reporter(numRemaining, "range lease iterations")
		}
	} else {
		// No more work to do.
		return
	}

	// We've seen all the replicas once. Now we're going to iterate
	// until they're all gone, up to the configured timeout.
	transferTimeout := LeaseTransferPerIterationTimeout.Get(&s.cfg.Settings.SV)

	const drainLeasesOp = "transfer range leases"
	if err := timeutil.RunWithTimeout(ctx, drainLeasesOp, transferTimeout,
		func(ctx context.Context) error {
			opts := retry.Options{
				InitialBackoff: 10 * time.Millisecond,
				MaxBackoff:     time.Second,
				Multiplier:     2,
			}
			everySecond := log.Every(time.Second)
			var err error
			// Avoid retry.ForDuration because of https://github.com/cockroachdb/cockroach/issues/25091.
			for r := retry.StartWithCtx(ctx, opts); r.Next(); {
				err = nil
				if numRemaining := transferAllAway(ctx); numRemaining > 0 {
					// Report progress to the Drain RPC.
					if reporter != nil {
						reporter(numRemaining, "range lease iterations")
					}
					err = errors.Errorf("waiting for %d replicas to transfer their lease away", numRemaining)
					if everySecond.ShouldLog() {
						log.Infof(ctx, "%v", err)
					}
				}
				if err == nil {
					// All leases transferred. We can stop retrying.
					break
				}
			}
			// If there's an error in the context but not yet detected in
			// err, take it into account here.
			return errors.CombineErrors(err, ctx.Err())
		}); err != nil {
		if tErr := (*timeutil.TimeoutError)(nil); errors.As(err, &tErr) && tErr.Operation() == drainLeasesOp {
			// You expect this message when shutting down a server in an unhealthy
			// cluster, or when draining all nodes with replicas for some range at the
			// same time. If we see it on healthy ones, there's likely something to fix.
			log.Warningf(ctx, "unable to drain cleanly within %s (cluster setting %s), "+
				"service might briefly deteriorate if the node is terminated: %s",
				transferTimeout, LeaseTransferPerIterationTimeout.Name(), tErr.Cause())
		} else {
			log.Warningf(ctx, "drain error: %+v", err)
		}
	}
}

// IsStarted returns true if the Store has been started.
func (s *Store) IsStarted() bool {
	return atomic.LoadInt32(&s.started) == 1
}

// Start the engine, set the GC and read the StoreIdent.
func (s *Store) Start(ctx context.Context, stopper *stop.Stopper) error {
	s.stopper = stopper

	// Populate the store ident. If not bootstrapped, ReadStoreIntent will
	// return an error.
	// TODO(sep-raft-log): which engine holds the ident?
	ident, err := kvstorage.ReadStoreIdent(ctx, s.TODOEngine())
	if err != nil {
		return err
	}
	s.Ident = &ident

	// Set the store ID for logging.
	s.cfg.AmbientCtx.AddLogTag("s", s.StoreID())
	ctx = s.AnnotateCtx(ctx)
	log.Event(ctx, "read store identity")

	// Communicate store ID to engine.
	// TODO(sep-raft-log): do for both engines.
	if err := s.TODOEngine().SetStoreID(ctx, int32(s.StoreID())); err != nil {
		return err
	}

	{
		m := rangefeed.NewSchedulerMetrics(s.cfg.HistogramWindowInterval)
		rfs := rangefeed.NewScheduler(rangefeed.SchedulerConfig{
			Workers:         s.cfg.RangeFeedSchedulerConcurrency,
			PriorityWorkers: s.cfg.RangeFeedSchedulerConcurrencyPriority,
			ShardSize:       s.cfg.RangeFeedSchedulerShardSize,
			Metrics:         m,
		})
		s.Registry().AddMetricStruct(m)
		if err = rfs.Start(ctx, s.stopper); err != nil {
			return err
		}
		s.rangefeedScheduler = rfs
	}

	// Add the store ID to the scanner's AmbientContext before starting it, since
	// the AmbientContext provided during construction did not include it.
	// Note that this is just a hacky way of getting around that without
	// refactoring the scanner/queue construction/start logic more broadly, and
	// depends on the scanner not having added its own log tag.
	if s.scanner != nil {
		s.scanner.AmbientContext.AddLogTag("s", s.StoreID())

		// We have to set the stopper here to avoid races, since scanner.Start() is
		// called async and the stopper is not available when the scanner is
		// created. This is a hack, the scanner/queue construction should be
		// refactored.
		s.scanner.stopper = s.stopper
	}

	// If the nodeID is 0, it has not be assigned yet.
	if s.nodeDesc.NodeID != 0 && s.Ident.NodeID != s.nodeDesc.NodeID {
		return errors.Errorf("node id:%d does not equal the one in node descriptor:%d", s.Ident.NodeID, s.nodeDesc.NodeID)
	}
	// Always set gossip NodeID before gossiping any info.
	if s.cfg.Gossip != nil {
		s.cfg.Gossip.NodeID.Set(ctx, s.Ident.NodeID)
	}

	// Create ID allocators.
	idAlloc, err := idalloc.NewAllocator(idalloc.Options{
		AmbientCtx:  s.cfg.AmbientCtx,
		Key:         keys.RangeIDGenerator,
		Incrementer: idalloc.DBIncrementer(s.db),
		BlockSize:   rangeIDAllocCount,
		Stopper:     s.stopper,
	})
	if err != nil {
		return err
	}

	// Create the intent resolver.
	var intentResolverRangeCache intentresolver.RangeCache
	rngCache := s.cfg.RangeDescriptorCache
	if s.cfg.RangeDescriptorCache != nil {
		intentResolverRangeCache = rngCache
	}

	s.intentResolver = intentresolver.New(intentresolver.Config{
		Clock:                s.cfg.Clock,
		DB:                   s.db,
		Stopper:              stopper,
		Settings:             s.cfg.Settings,
		TaskLimit:            s.cfg.IntentResolverTaskLimit,
		AmbientCtx:           s.cfg.AmbientCtx,
		TestingKnobs:         s.cfg.TestingKnobs.IntentResolverKnobs,
		RangeDescriptorCache: intentResolverRangeCache,
	})
	s.metrics.registry.AddMetricStruct(s.intentResolver.Metrics)

	// Create the raft log truncator and register the callback.
	s.raftTruncator = makeRaftLogTruncator(s.cfg.AmbientCtx, (*storeForTruncatorImpl)(s), stopper)
	{
		truncator := s.raftTruncator
		// When state machine has persisted new RaftAppliedIndex, fire callback.
		s.TODOEngine().RegisterFlushCompletedCallback(func() {
			truncator.durabilityAdvancedCallback()
		})
	}

	// Create the recovery manager.
	s.recoveryMgr = txnrecovery.NewManager(
		s.cfg.AmbientCtx, s.cfg.Clock, s.db, stopper,
	)
	s.metrics.registry.AddMetricStruct(s.recoveryMgr.Metrics())

	// Create the Store Liveness SupportManager.
	livenessInterval, heartbeatInterval := s.cfg.StoreLivenessDurations()
	supportGracePeriod := s.cfg.RPCContext.StoreLivenessWithdrawalGracePeriod()
	options := storeliveness.NewOptions(livenessInterval, heartbeatInterval, supportGracePeriod)
	sm := storeliveness.NewSupportManager(
		slpb.StoreIdent{NodeID: s.nodeDesc.NodeID, StoreID: s.StoreID()}, s.StateEngine(), options,
		s.cfg.Settings, s.stopper, s.cfg.Clock, s.cfg.StoreLivenessTransport,
	)
	s.cfg.StoreLivenessTransport.ListenMessages(s.StoreID(), sm)
	s.storeLiveness = sm
	s.metrics.registry.AddMetricStruct(sm.Metrics())
	if err = sm.Start(ctx); err != nil {
		return errors.Wrap(err, "starting store liveness")
	}

	s.rangeIDAlloc = idAlloc

	if s.cfg.KVFlowStreamTokenProvider == nil {
		// Some tests don't setup a server, just a store or similar. In these
		// cases, the KVFlow.* fields in the store config, which are references
		// to a shared object between stores on the same server, will not be set.
		// We set them here to avoid handling nil values in such tests within the
		// rac2 code.
		if buildutil.CrdbTestBuild {
			// Ensure that this is actually test build. If it is not, we should not
			// be compensating for an underlying bug.
			s.cfg.KVFlowSendTokenWatcher = rac2.NewSendTokenWatcher(s.stopper, timeutil.DefaultTimeSource{})
		} else {
			return errors.New("missing KVFlowSendTokenWatcher")
		}
	}

	now := s.cfg.Clock.Now()
	s.startedAt = now.WallTime

	// Iterate over all range descriptors, ignoring uncommitted versions
	// (consistent=false). Uncommitted intents which have been abandoned
	// due to a split crashing halfway will simply be resolved on the
	// next split attempt. They can otherwise be ignored.
	//
	// TODO(peter): While we have to iterate to find the replica descriptors
	// serially, we can perform the migrations and replica creation
	// concurrently. Note that while we can perform this initialization
	// concurrently, all initialization must be performed before we start
	// listening for Raft messages and starting the process Raft loop.
	//
	// TODO(sep-raft-log): this will need to learn to stitch and reconcile data from
	// both engines.
	repls, err := kvstorage.LoadAndReconcileReplicas(ctx, s.TODOEngine())
	if err != nil {
		return err
	}
	logEvery := log.Every(10 * time.Second)
	for i, repl := range repls {
		// Log progress regularly, but not for the first replica (we only want to
		// log when this is slow). The last replica is logged after iteration.
		if logEvery.ShouldLog() && i > 0 {
			log.Infof(ctx, "initialized %d/%d replicas", i, len(repls))
		}

		if repl.Desc == nil {
			// Uninitialized Replicas are not currently instantiated at store start.
			continue
		}
		// TODO(pavelkalinnikov): integrate into kvstorage.LoadAndReconcileReplicas.
		state, err := repl.Load(ctx, s.TODOEngine(), s.StoreID())
		if err != nil {
			return err
		}
		rep, err := newInitializedReplica(s, state)
		if err != nil {
			return err
		}

		// We can't lock s.mu across NewReplica due to the lock ordering
		// constraint (*Replica).raftMu < (*Store).mu. See the comment on
		// (Store).mu.
		s.mu.Lock()
		// TODO(pavelkalinnikov): hide these in Store's replica create functions.
		err = s.addToReplicasByRangeIDLocked(rep)
		if err == nil {
			err = s.addToReplicasByKeyLocked(rep, rep.Desc())
		}
		s.mu.Unlock()
		if err != nil {
			return err
		}

		// Add this range and its stats to our counter.
		s.metrics.ReplicaCount.Inc(1)
		// INVARIANT: each initialized Replica is associated to a tenant.
		if _, ok := rep.TenantID(); ok {
			s.metrics.addMVCCStats(ctx, rep.tenantMetricsRef, rep.GetMVCCStats())
		} else {
			return errors.AssertionFailedf("no tenantID for initialized replica %s", rep)
		}

		// Eagerly unquiesce replicas that use expiration-based leases. We don't
		// quiesce ranges with expiration leases, and we want to eagerly acquire
		// leases for them, which happens during Raft ticks. We rely on Raft
		// pre-vote to avoid disturbing established Raft leaders.
		//
		// NB: cluster settings haven't propagated yet, so we have to check the last
		// known lease instead of relying on shouldUseExpirationLeaseRLocked(). We
		// also check Sequence > 0 to omit ranges that haven't seen a lease yet.
		if l, _ := rep.GetLease(); l.Type() == roachpb.LeaseExpiration && l.Sequence > 0 {
			rep.maybeUnquiesce(ctx, true /* wakeLeader */, true /* mayCampaign */)
		}
	}
	log.Infof(ctx, "initialized %d/%d replicas", len(repls), len(repls))

	// Register a callback to unquiesce any ranges with replicas on a
	// node transitioning from non-live to live.
	if s.cfg.NodeLiveness != nil {
		s.cfg.NodeLiveness.RegisterCallback(s.nodeIsLiveCallback)
	}

	// SystemConfigProvider can be nil during some tests.
	if scp := s.cfg.SystemConfigProvider; scp != nil {
		systemCfgUpdateC, _ := scp.RegisterSystemConfigChannel()
		_ = s.stopper.RunAsyncTask(ctx, "syscfg-listener", func(context.Context) {
			for {
				select {
				case <-systemCfgUpdateC:
					cfg := scp.GetSystemConfig()
					s.systemGossipUpdate(cfg)
				case <-s.stopper.ShouldQuiesce():
					return
				}
			}
		})
	}

	// Gossip is only ever nil while bootstrapping a cluster and
	// in unittests.
	if s.cfg.Gossip != nil {
		s.storeGossip.stopper = stopper
		s.storeGossip.Ident = *s.Ident

		// Start a single goroutine in charge of periodically gossiping the
		// sentinel and first range metadata if we have a first range.
		// This may wake up ranges and requires everything to be set up and
		// running.
		s.startGossip()

		// Start the scanner. The construction here makes sure that the scanner
		// only starts after Gossip has connected, and that it does not block Start
		// from returning (as doing so might prevent Gossip from ever connecting).
		_ = s.stopper.RunAsyncTask(ctx, "scanner", func(context.Context) {
			select {
			case <-s.cfg.Gossip.Connected:
				s.scanner.Start()
			case <-s.stopper.ShouldQuiesce():
				return
			}
		})
	}

	if !s.cfg.SpanConfigsDisabled {
		s.cfg.SpanConfigSubscriber.Subscribe(func(ctx context.Context, update roachpb.Span) {
			s.onSpanConfigUpdate(ctx, update)
		})

		// We also want to do it when the fallback config setting is changed.
		spanconfigstore.FallbackConfigOverride.SetOnChange(&s.ClusterSettings().SV, func(ctx context.Context) {
			s.applyAllFromSpanConfigStore(ctx)
		})
	}

	// Start Raft processing goroutines.
	s.cfg.Transport.ListenIncomingRaftMessages(s.StoreID(), s)
	s.cfg.Transport.ListenOutgoingMessage(s.StoreID(), s)
	s.processRaft(ctx)

	// Connect rangefeeds to closed timestamp updates.
	s.startRangefeedUpdater(ctx)

	s.startRangefeedTxnPushNotifier(ctx)

	if s.replicateQueue != nil {
		s.storeRebalancer = NewStoreRebalancer(
			s.cfg.AmbientCtx, s.cfg.Settings, s.replicateQueue, s.replRankings, s.rebalanceObjManager)
		s.storeRebalancer.Start(ctx, s.stopper)
	}

	// Set the started flag (for unittests).
	atomic.StoreInt32(&s.started, 1)

	return nil
}

// WaitForInit waits for any asynchronous processes begun in Start()
// to complete their initialization. In particular, this includes
// gossiping. In some cases this may block until the range GC queue
// has completed its scan. Only for testing.
func (s *Store) WaitForInit() {
	s.initComplete.Wait()
}

// GetConfReader exposes access to a configuration reader.
func (s *Store) GetConfReader(ctx context.Context) (spanconfig.StoreReader, error) {
	if s.cfg.TestingKnobs.MakeSystemConfigSpanUnavailableToQueues {
		return nil, errSpanConfigsUnavailable
	}
	if s.cfg.TestingKnobs.ConfReaderInterceptor != nil {
		return s.cfg.TestingKnobs.ConfReaderInterceptor(), nil
	}

	if s.cfg.SpanConfigsDisabled || s.TestingKnobs().UseSystemConfigSpanForQueues {
		sysCfg := s.cfg.SystemConfigProvider.GetSystemConfig()
		if sysCfg == nil {
			return nil, errSpanConfigsUnavailable
		}
		return sysCfg, nil
	}

	if s.cfg.SpanConfigSubscriber.LastUpdated().IsEmpty() {
		// This code path is used in various internal queues. It's important to
		// surface explicitly that we don't have any span configs instead of
		// falling back to the statically configured one.
		// - enabling range merges would be extremely dangerous -- we could
		//   collapse everything into a single range.
		// - enabling the split queue would mean applying the statically
		//   configured range sizes in the fallback span config. For clusters
		//   configured with larger range sizes, this could lead to a burst of
		//   splitting post node-restart.
		// - enabling the MVCC GC queue would mean applying the statically
		//   configured default GC TTL and ignoring any set protected
		//   timestamps. The latter is best-effort protection, but for clusters
		//   configured with GC TTL greater than the default, post node-restart
		//   it could lead to a burst of MVCC GC activity and AOST queries
		//   failing to find expected data.
		// - enabling the replicate queue would mean replicating towards the
		//   statically defined 3x replication in the fallback span config.
		return nil, errSpanConfigsUnavailable
	}
	return s.cfg.SpanConfigSubscriber, nil
}

// startRangefeedUpdater periodically informs all the replicas with rangefeeds
// about closed timestamp updates.
func (s *Store) startRangefeedUpdater(ctx context.Context) {
	_ /* err */ = s.stopper.RunAsyncTaskEx(ctx, stop.TaskOpts{
		TaskName: "closedts-rangefeed-updater",
		SpanOpt:  stop.SterileRootSpan,
	}, func(ctx context.Context) {
		ctx, cancel := s.stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		// The replicas with an active rangefeed. Updated periodically below. Reuse
		// the rangeIDs slice across runs to minimize allocation.
		var rangeIDs []roachpb.RangeID
		updateRangeIDs := func() []roachpb.RangeID {
			rangeIDs = rangeIDs[:0]
			s.rangefeedReplicas.Lock()
			for rangeID := range s.rangefeedReplicas.m {
				rangeIDs = append(rangeIDs, rangeID)
			}
			s.rangefeedReplicas.Unlock()
			// Sort the range IDs so that we notify them in the same order on each
			// iteration. With the pacing below, this helps to ensure a consistent
			// closed timestamp lag for each range. The closedts-rangefeed-updater
			// refresh loop operates independently of incoming closed timestamp
			// updates, so this does not favor some replicas over others.
			slices.Sort(rangeIDs)
			return rangeIDs
		}

		var timer timeutil.Timer
		defer timer.Stop()
		errInterrupted := errors.New("waiting interrupted")
		wait := func(ctx context.Context, until time.Time, interrupt <-chan struct{}) error {
			now := timeutil.Now()
			if !now.Before(until) {
				return nil
			}
			timer.Reset(until.Sub(now))
			select {
			case <-timer.C:
				timer.Read = true
				return nil
			case <-interrupt:
				return errInterrupted
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// Repeatedly handle the closed timestamp update for each range ID.
		//
		// The `refresh` interval is the target interval between delivering closed
		// timestamp updates for a single range. This is the target time in which a
		// single pass of the loop below should complete. The `smear` interval is
		// the target interval between wake-ups within a single run.
		//
		// We smear the work across the refresh interval to avoid waking up all the
		// replicas' rangefeed processor and registration goroutines in a short
		// burst. Doing so without pacing can have a severe impact on goroutine
		// scheduling latency which impacts foreground latency, as tens of thousands
		// of goroutines can be marked as runnable in a short period of time.
		//
		// Pacing works as follows:
		// - We spread closed timestamp notifications to replicas across the refresh
		//   interval (default 200ms).
		// - We smear them at the granularity of `smear` interval (e.g. 1ms) to
		//   avoid putting this goroutine to sleep for excessively short periods of
		//   time.
		// - We prioritize staying within the target refresh interval, so we adjust
		//   the rate of processing in accordance with the time remaining until the
		//   refresh interval ends.
		conf := newRangeFeedUpdaterConf(s.cfg.Settings)
		for {
			// Configuration may have changed between runs, load it unconditionally.
			// This will block until an "active" configuration exists, i.e. a one with
			// non-zero refresh and smear intervals.
			refresh, smear, err := conf.wait(ctx)
			if err != nil {
				return // context canceled
			}
			// Aim to complete this run in exactly refresh interval.
			now := timeutil.Now()
			deadline := now.Add(refresh)
			var waitErr error
			// We're about to perform one work cycle, where we go through all replicas
			// that have an active rangefeed on them and update them with the current
			// closed timestamp for their range. While doing so, we'll keep track of
			// the earliest closed timestamp we've seen and the number of ranges whose
			// closed timestamp is lagging excessively; we'll then use these to update
			// some metrics when the work cycle completes.
			var earliestClosedTS hlc.Timestamp
			var numExcessivelyLaggingClosedTS int64

			for work, startAt := updateRangeIDs(), now; len(work) != 0; {
				if waitErr = wait(ctx, startAt, conf.changed); waitErr != nil {
					// NB: a configuration change abandons the update loop
					break
				}
				todo, by := rangeFeedUpdaterPace(timeutil.Now(), deadline, smear, len(work))
				for _, id := range work[:todo] {
					if r := s.GetReplicaIfExists(id); r != nil {
						cts := r.GetCurrentClosedTimestamp(ctx)
						if earliestClosedTS.IsEmpty() || cts.Less(earliestClosedTS) {
							earliestClosedTS = cts
						}
						if exceedsSlowLagThresh := r.handleClosedTimestampUpdate(ctx, cts); exceedsSlowLagThresh {
							numExcessivelyLaggingClosedTS++
						}
					}
				}
				work = work[todo:]
				startAt = by
			}
			if waitErr == nil {
				waitErr = wait(ctx, deadline, conf.changed) // wait out any remaining time
			}
			// NB: an errInterrupted means this run was interrupted by relevant
			// cluster setting changes. In this case we abandon the current run, and
			// start a new one immediately by looping around.
			//
			// TODO(pavelkalinnikov): honour config changes without interrupting the
			// run, as rangeFeedUpdaterPace makes this algorithm adaptive.
			if waitErr != nil && !errors.Is(waitErr, errInterrupted) {
				return // context canceled
			}
			// We've successfully finished one work cycle where we went through all
			// replicas that had an active rangefeed on them; update metrics.
			if !earliestClosedTS.IsEmpty() {
				nanos := timeutil.Since(earliestClosedTS.GoTime()).Nanoseconds()
				s.metrics.RangeFeedMetrics.RangeFeedClosedTimestampMaxBehindNanos.Update(nanos)
			}
			s.metrics.RangeFeedMetrics.RangeFeedSlowClosedTimestampRanges.Update(numExcessivelyLaggingClosedTS)
		}
	})
}

// startRangefeedTxnPushNotifier starts a worker that would periodically
// enqueue txn push event for rangefeed processors to let them push lagging
// transactions.
func (s *Store) startRangefeedTxnPushNotifier(ctx context.Context) {
	interval := rangefeed.DefaultPushTxnsInterval
	if i := s.TestingKnobs().RangeFeedPushTxnsInterval; i > 0 {
		interval = i
	}

	_ /* err */ = s.stopper.RunAsyncTaskEx(ctx, stop.TaskOpts{
		TaskName: "transaction-rangefeed-push-notifier",
		SpanOpt:  stop.SterileRootSpan,
	}, func(ctx context.Context) {
		ctx, cancel := s.stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		makeSchedulerBatch := func() *rangefeed.SchedulerBatch {
			batch := s.rangefeedScheduler.NewEnqueueBatch()
			s.rangefeedReplicas.Lock()
			for _, id := range s.rangefeedReplicas.m {
				if id != 0 {
					// Only process ranges that use scheduler.
					batch.Add(id)
				}
			}
			s.rangefeedReplicas.Unlock()
			return batch
		}

		ticker := time.NewTicker(interval)
		for {
			select {
			case <-ticker.C:
				if !rangefeed.PushTxnsEnabled.Get(&s.ClusterSettings().SV) {
					continue
				}
				batch := makeSchedulerBatch()
				s.rangefeedScheduler.EnqueueBatch(batch, rangefeed.PushTxnQueued)
				batch.Close()
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	})
}

func (s *Store) addReplicaWithRangefeed(rangeID roachpb.RangeID, schedulerID int64) {
	s.rangefeedReplicas.Lock()
	s.rangefeedReplicas.m[rangeID] = schedulerID
	s.rangefeedReplicas.Unlock()
}

func (s *Store) removeReplicaWithRangefeed(rangeID roachpb.RangeID) {
	s.rangefeedReplicas.Lock()
	delete(s.rangefeedReplicas.m, rangeID)
	s.rangefeedReplicas.Unlock()
}

// onSpanConfigUpdate is the callback invoked whenever this store learns of a
// span config update.
func (s *Store) onSpanConfigUpdate(ctx context.Context, updated roachpb.Span) {
	sp, err := keys.SpanAddr(updated)
	if err != nil {
		log.Errorf(ctx, "skipped applying update (%s), unexpected error resolving span address: %v",
			updated, err)
		return
	}

	now := s.cfg.Clock.NowAsClockTimestamp()

	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.mu.replicasByKey.VisitKeyRange(ctx, sp.Key, sp.EndKey, AscendingKeyOrder,
		func(ctx context.Context, it replicaOrPlaceholder) error {
			repl := it.repl
			if repl == nil {
				return nil // placeholder; ignore
			}

			replCtx := repl.AnnotateCtx(ctx)
			startKey := repl.Desc().StartKey
			changed := true
			if sp.ContainsKey(startKey) {
				// It's possible that the update we're receiving here implies a split.
				// If the update corresponds to what would be the config for the
				// right-hand side after the split, we avoid clobbering the pre-split
				// range's embedded span config by checking if the start key is part of
				// the update.
				//
				// Even if we're dealing with what would be the right-hand side after
				// the split is processed, we still want to nudge the split queue
				// below -- we can't instead rely on there being an update for the
				// left-hand side of the split. Concretely, consider the case when a
				// new table is added with a different configuration to its (left)
				// adjacent table. This results in a single update, corresponding to the
				// new table's span, which forms the right-hand side post split.

				conf, sp, err := s.cfg.SpanConfigSubscriber.GetSpanConfigForKey(replCtx, startKey)
				if err != nil {
					log.Errorf(replCtx, "skipped applying update, unexpected error reading from subscriber: %v", err)
					return err
				}
				changed = repl.SetSpanConfig(conf, sp)
			}
			if changed {
				repl.MaybeQueue(ctx, now)
			}
			return nil // more
		},
	); err != nil {
		// Errors here should not be possible, but if there is one, log loudly.
		log.Errorf(ctx, "unexpected error visiting replicas: %v", err)
	}
}

// applyAllFromSpanConfigStore applies, on each replica, span configs from the
// embedded span config store.
func (s *Store) applyAllFromSpanConfigStore(ctx context.Context) {
	now := s.cfg.Clock.NowAsClockTimestamp()
	newStoreReplicaVisitor(s).Visit(func(repl *Replica) bool {
		replCtx := repl.AnnotateCtx(ctx)
		key := repl.Desc().StartKey
		conf, confSpan, err := s.cfg.SpanConfigSubscriber.GetSpanConfigForKey(replCtx, key)
		if err != nil {
			log.Errorf(ctx, "skipped applying config update, unexpected error reading from subscriber: %v", err)
			return true // more
		}

		changed := repl.SetSpanConfig(conf, confSpan)
		if changed {
			repl.MaybeQueue(replCtx, now)
		}
		return true // more
	})
}

// GossipStore broadcasts the store on the gossip network.
func (s *Store) GossipStore(ctx context.Context, useCached bool) error {
	return s.storeGossip.GossipStore(ctx, useCached)
}

// UpdateIOThreshold updates the IOThreshold and IOThresholdMax reported in the
// StoreDescriptor.
func (s *Store) UpdateIOThreshold(ioThreshold *admissionpb.IOThreshold) {
	now := s.Clock().Now().GoTime()

	s.ioThreshold.Lock()
	s.ioThreshold.t = ioThreshold
	s.ioThreshold.maxL0NumSubLevels.Record(now, float64(ioThreshold.L0NumSubLevels))
	s.ioThreshold.maxL0NumFiles.Record(now, float64(ioThreshold.L0NumFiles))
	s.ioThreshold.maxL0Size.Record(now, float64(ioThreshold.L0Size))
	maxL0NumSubLevels, _ := s.ioThreshold.maxL0NumSubLevels.Query(now)
	maxL0NumFiles, _ := s.ioThreshold.maxL0NumFiles.Query(now)
	maxL0Size, _ := s.ioThreshold.maxL0Size.Query(now)
	s.ioThreshold.Unlock()

	ioThresholdMax := protoutil.Clone(ioThreshold).(*admissionpb.IOThreshold)
	ioThresholdMax.L0NumSubLevels = int64(maxL0NumSubLevels)
	ioThresholdMax.L0NumFiles = int64(maxL0NumFiles)
	ioThresholdMax.L0Size = int64(maxL0Size)

	// Update the store's cached capacity and potentially gossip the updated
	// capacity async if the IO threshold has increased.
	s.storeGossip.RecordNewIOThreshold(*ioThreshold, *ioThresholdMax)
}

// existingLeaseCheckIOOverload checks whether the store is IO overloaded
// enough that it would fail the allocator checks for holding leases, returning
// true when okay and false otherwise. When false is returned, it indicates
// that a replicas' lease will be transferred away when encountered by the
// lease queue.
func (s *Store) existingLeaseCheckIOOverload(ctx context.Context) bool {
	storeList, _, _ := s.cfg.StorePool.GetStoreList(storepool.StoreFilterNone)
	storeDescriptor, ok := s.cfg.StorePool.GetStoreDescriptor(s.StoreID())
	if !ok {
		return false
	}
	return s.allocator.IOOverloadOptions().ExistingLeaseCheck(
		ctx, storeDescriptor, storeList)
}

// makeIOOverloadCapacityChangeFn returns a capacity change callback which will
// enqueue all leaseholder replicas into the lease queue when: (1) the capacity
// change is for the local store and (2) the store's IO is considered too
// overloaded to hold onto its existing leases. The leases will be processed
// via the lease queue and shed to another replica, if available.
func (s *Store) makeIOOverloadCapacityChangeFn() storepool.CapacityChangeFn {
	return func(storeID roachpb.StoreID, old, cur roachpb.StoreCapacity) {
		// There's nothing to do when there are no leases on the store.
		if cur.LeaseCount == 0 {
			return
		}

		// Don't react to other stores capacity changes, only IO overload change on
		// the local store descriptor is relevant.
		if !s.IsStarted() || s.StoreID() != storeID {
			return
		}

		// Avoid shedding leases too frequently by checking the last time a shed
		// was attempted.
		if lastShed := s.lastIOOverloadLeaseShed.Load(); lastShed != nil {
			minInterval := MinIOOverloadLeaseShedInterval.Get(&s.cfg.Settings.SV)
			if timeutil.Since(lastShed.(time.Time)) < minInterval {
				return
			}
		}

		// Lastly, check whether the store is considered IO overloaded relative to
		// the configured threshold and the cluster average.
		ctx := context.Background()
		s.AnnotateCtx(ctx)
		if s.existingLeaseCheckIOOverload(ctx) {
			return
		}

		// NB: Update the last shed time prior to trying to shed leases, this
		// should limit the window of concurrent shedding activity (in the case
		// where multiple capacity changes are called within a short window). This
		// could be removed entirely with a mutex but hardly seems necessary.
		s.lastIOOverloadLeaseShed.Store(s.Clock().Now().GoTime())
		log.KvDistribution.Infof(
			ctx, "IO overload detected, will shed leases %v", cur.LeaseCount)

		// This callback is on the gossip goroutine, once we know we wish to shed
		// leases, split off the actual enqueuing work to a separate async task
		// goroutine.
		if err := s.stopper.RunTask(ctx, "io-overload: shed leases", func(ctx context.Context) {
			newStoreReplicaVisitor(s).Visit(func(repl *Replica) bool {
				s.leaseQueue.maybeAdd(ctx, repl, repl.Clock().NowAsClockTimestamp())
				return true /* wantMore */
			})
		}); err != nil {
			log.KvDistribution.Infof(ctx,
				"unable to shed leases due to IO overload: %v", err)
			// An error should only be encountered when the server is quiescing, as
			// such we don't reset the timer on a failed attempt.
		}
	}
}

// VisitReplicasOption optionally modifies store.VisitReplicas.
type VisitReplicasOption func(*storeReplicaVisitor)

// WithReplicasInOrder is a VisitReplicasOption that ensures replicas are
// visited in increasing RangeID order.
func WithReplicasInOrder() VisitReplicasOption {
	return func(visitor *storeReplicaVisitor) {
		visitor.InOrder()
	}
}

// VisitReplicas invokes the visitor on the Store's Replicas until the visitor returns false.
// Replicas which are added to the Store after iteration begins may or may not be observed.
func (s *Store) VisitReplicas(visitor func(*Replica) (wantMore bool), opts ...VisitReplicasOption) {
	v := newStoreReplicaVisitor(s)
	for _, opt := range opts {
		opt(v)
	}
	v.Visit(visitor)
}

// visitReplicasByKey invokes the visitor on all the replicas for ranges that
// overlap [startKey, endKey), or until the visitor returns false. Replicas are
// visited in key order, with `s.mu` held. Placeholders are not visited.
//
// Visited replicas might be IsDestroyed(); if the visitor cares, it needs to
// protect against it itself.
func (s *Store) visitReplicasByKey(
	ctx context.Context,
	startKey, endKey roachpb.RKey,
	order IterationOrder,
	visitor func(context.Context, *Replica) error, // can return iterutil.StopIteration()
) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.replicasByKey.VisitKeyRange(ctx, startKey, endKey, order, func(ctx context.Context, it replicaOrPlaceholder) error {
		if it.repl != nil {
			return visitor(ctx, it.repl)
		}
		return nil
	})
}

// WriteLastUpTimestamp records the supplied timestamp into the "last up" key
// on this store. This value should be refreshed whenever this store's node
// updates its own liveness record; it is used by a restarting store to
// determine the approximate time that it stopped.
func (s *Store) WriteLastUpTimestamp(ctx context.Context, time hlc.Timestamp) error {
	ctx = s.AnnotateCtx(ctx)
	return storage.MVCCPutProto(
		ctx,
		s.TODOEngine(), // TODO(sep-raft-log): probably state engine
		keys.StoreLastUpKey(),
		hlc.Timestamp{},
		&time,
		storage.MVCCWriteOptions{},
	)
}

// ReadLastUpTimestamp returns the "last up" timestamp recorded in this store.
// This value can be used to approximate the last time the engine was was being
// served as a store by a running node. If the store does not contain a "last
// up" timestamp (for example, on a newly bootstrapped store), the zero
// timestamp is returned instead.
func (s *Store) ReadLastUpTimestamp(ctx context.Context) (hlc.Timestamp, error) {
	var timestamp hlc.Timestamp
	ok, err := storage.MVCCGetProto(ctx, s.TODOEngine(), keys.StoreLastUpKey(), hlc.Timestamp{},
		&timestamp, storage.MVCCGetOptions{})
	if err != nil {
		return hlc.Timestamp{}, err
	} else if !ok {
		return hlc.Timestamp{}, nil
	}
	return timestamp, nil
}

// WriteHLCUpperBound records an upper bound to the wall time of the HLC
func (s *Store) WriteHLCUpperBound(ctx context.Context, time int64) error {
	ctx = s.AnnotateCtx(ctx)
	ts := hlc.Timestamp{WallTime: time}
	batch := s.TODOEngine().NewBatch() // TODO(sep-raft-log): state engine might be useful here due to need to sync
	// Write has to sync to disk to ensure HLC monotonicity across restarts
	defer batch.Close()
	if err := storage.MVCCPutProto(
		ctx,
		batch,
		keys.StoreHLCUpperBoundKey(),
		hlc.Timestamp{},
		&ts,
		storage.MVCCWriteOptions{},
	); err != nil {
		return err
	}

	if err := batch.Commit(true /* sync */); err != nil {
		return err
	}
	return nil
}

// ReadHLCUpperBound returns the upper bound to the wall time of the HLC
// If this value does not exist 0 is returned
func ReadHLCUpperBound(ctx context.Context, e storage.Engine) (int64, error) {
	var timestamp hlc.Timestamp
	ok, err := storage.MVCCGetProto(ctx, e, keys.StoreHLCUpperBoundKey(), hlc.Timestamp{},
		&timestamp, storage.MVCCGetOptions{})
	if err != nil {
		return 0, err
	} else if !ok {
		return 0, nil
	}
	return timestamp.WallTime, nil
}

// ReadMaxHLCUpperBound returns the maximum of the stored hlc upper bounds
// among all the engines. This value is optionally persisted by the server and
// it is guaranteed to be higher than any wall time used by the HLC. If this
// value is persisted, HLC wall clock monotonicity is guaranteed across server
// restarts
func ReadMaxHLCUpperBound(ctx context.Context, engines []storage.Engine) (int64, error) {
	var hlcUpperBound int64
	for _, e := range engines {
		engineHLCUpperBound, err := ReadHLCUpperBound(ctx, e)
		if err != nil {
			return 0, err
		}
		if engineHLCUpperBound > hlcUpperBound {
			hlcUpperBound = engineHLCUpperBound
		}
	}
	return hlcUpperBound, nil
}

// GetReplica fetches a replica by Range ID. Returns an error if no replica is found.
//
// See also GetReplicaIfExists for a more performant version.
func (s *Store) GetReplica(rangeID roachpb.RangeID) (*Replica, error) {
	if r := s.GetReplicaIfExists(rangeID); r != nil {
		return r, nil
	}
	return nil, kvpb.NewRangeNotFoundError(rangeID, s.StoreID())
}

// GetReplicaIfExists returns the replica with the given RangeID or nil.
func (s *Store) GetReplicaIfExists(rangeID roachpb.RangeID) *Replica {
	if repl, ok := s.mu.replicasByRangeID.Load(rangeID); ok {
		return repl
	}
	return nil
}

// LookupReplica looks up the replica that contains the specified key. It
// returns nil if no such replica exists.
func (s *Store) LookupReplica(key roachpb.RKey) *Replica {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.replicasByKey.LookupReplica(context.Background(), key)
}

// lookupPrecedingReplica finds the replica in this store that immediately
// precedes the specified key without containing it. It returns nil if no such
// replica exists. It ignores replica placeholders.
//
// Concretely, when key represents a key within replica R,
// lookupPrecedingReplica returns the replica that immediately precedes R in
// replicasByKey.
func (s *Store) lookupPrecedingReplica(key roachpb.RKey) *Replica {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.replicasByKey.LookupPrecedingReplica(context.Background(), key)
}

// getOverlappingKeyRangeLocked returns an replicaOrPlaceholder from Store.mu.replicasByKey
// overlapping the given descriptor (or nil if no such replicaOrPlaceholder exists).
func (s *Store) getOverlappingKeyRangeLocked(
	rngDesc *roachpb.RangeDescriptor,
) replicaOrPlaceholder {
	var it replicaOrPlaceholder
	if err := s.mu.replicasByKey.VisitKeyRange(
		context.Background(), rngDesc.StartKey, rngDesc.EndKey, AscendingKeyOrder,
		func(ctx context.Context, iit replicaOrPlaceholder) error {
			it = iit
			return iterutil.StopIteration()
		}); err != nil {
		log.Fatalf(context.Background(), "%v", err)
	}

	return it
}

// RaftStatus returns the current raft status of the local replica of
// the given range.
func (s *Store) RaftStatus(rangeID roachpb.RangeID) *raft.Status {
	if repl, ok := s.mu.replicasByRangeID.Load(rangeID); ok {
		return repl.RaftStatus()
	}
	return nil
}

// ClusterID accessor.
func (s *Store) ClusterID() uuid.UUID { return s.Ident.ClusterID }

// NodeID accessor.
func (s *Store) NodeID() roachpb.NodeID { return s.Ident.NodeID }

// StoreID accessor.
func (s *Store) StoreID() roachpb.StoreID { return s.Ident.StoreID }

// Clock accessor.
func (s *Store) Clock() *hlc.Clock { return s.cfg.Clock }

// StateEngine returns the statemachine engine.
func (s *Store) StateEngine() storage.Engine {
	return s.internalEngines.stateEngine
}

// TODOEngine is a placeholder for cases in which
// the caller needs to be updated in order to use
// only one engine, or a closer check is still
// pending.
func (s *Store) TODOEngine() storage.Engine {
	return s.internalEngines.todoEngine
}

// LogEngine returns the log engine.
func (s *Store) LogEngine() storage.Engine {
	return s.internalEngines.logEngine
}

// DB accessor.
func (s *Store) DB() *kv.DB { return s.cfg.DB }

// Gossip accessor.
func (s *Store) Gossip() *gossip.Gossip { return s.cfg.Gossip }

// Stopper accessor.
func (s *Store) Stopper() *stop.Stopper { return s.stopper }

// TestingKnobs accessor.
func (s *Store) TestingKnobs() *StoreTestingKnobs { return &s.cfg.TestingKnobs }

// IsDraining accessor.
func (s *Store) IsDraining() bool { return s.draining.Load() }

// AllocateRangeID allocates a new RangeID from the cluster-wide RangeID allocator.
func (s *Store) AllocateRangeID(ctx context.Context) (roachpb.RangeID, error) {
	id, err := s.rangeIDAlloc.Allocate(ctx)
	if err != nil {
		return 0, err
	}
	return roachpb.RangeID(id), nil
}

// Attrs returns the attributes of the underlying store.
func (s *Store) Attrs() roachpb.Attributes {
	return s.TODOEngine().Attrs()
}

// Properties returns the properties of the underlying store.
func (s *Store) Properties() roachpb.StoreProperties {
	// TODO(sep-raft-log): see if this needs to exist for the logEngine too.
	return s.TODOEngine().Properties()
}

// Capacity returns the capacity of the underlying storage engine. Note that
// this does not include reservations.
// Note that Capacity() has the side effect of updating some of the store's
// internal statistics about its replicas.
func (s *Store) Capacity(ctx context.Context, useCached bool) (roachpb.StoreCapacity, error) {
	if useCached {
		capacity := s.storeGossip.CachedCapacity()
		if capacity != (roachpb.StoreCapacity{}) {
			return capacity, nil
		}
	}

	capacity, err := s.TODOEngine().Capacity()
	if err != nil {
		return roachpb.StoreCapacity{}, err
	}

	now := s.cfg.Clock.NowAsClockTimestamp()
	var leaseCount int32
	var rangeCount int32
	var logicalBytes int64
	var totalQueriesPerSecond float64
	var totalWritesPerSecond float64
	var totalStoreCPUTimePerSecond float64
	replicaCount := s.metrics.ReplicaCount.Value()
	bytesPerReplica := make([]float64, 0, replicaCount)
	writesPerReplica := make([]float64, 0, replicaCount)
	// We wish to track both CPU and QPS, due to different usecases between UI
	// and rebalancing. By default rebalancing uses CPU whilst the UI will use
	// QPS.
	rankingsAccumulator := NewReplicaAccumulator(load.CPU, load.Queries)
	// rankingsByTenantAccumulator collects top replicas by QPS only as far as it is
	// used in Db Console only.
	rankingsByTenantAccumulator := NewTenantReplicaAccumulator(load.Queries)

	newStoreReplicaVisitor(s).Visit(func(r *Replica) bool {
		rangeCount++
		if r.OwnsValidLease(ctx, now) {
			leaseCount++
		}
		usage := r.RangeUsageInfo()
		logicalBytes += usage.LogicalBytes
		bytesPerReplica = append(bytesPerReplica, float64(usage.LogicalBytes))
		// TODO(a-robinson): How dangerous is it that these numbers will be
		// incorrectly low the first time or two it gets gossiped when a store
		// starts? We can't easily have a countdown as its value changes like for
		// leases/replicas.
		// TODO(a-robinson): Calculate percentiles for qps? Get rid of other percentiles?
		totalStoreCPUTimePerSecond += usage.RequestCPUNanosPerSecond + usage.RaftCPUNanosPerSecond
		totalQueriesPerSecond += usage.QueriesPerSecond
		totalWritesPerSecond += usage.WritesPerSecond
		writesPerReplica = append(writesPerReplica, usage.WritesPerSecond)
		cr := candidateReplica{
			Replica: r,
			usage:   usage,
		}
		rankingsAccumulator.AddReplica(cr)
		rankingsByTenantAccumulator.AddReplica(cr)
		return true
	})

	// It is possible that the cputime utility isn't supported on this node's
	// architecture. If that is the case, we publish the cpu per second as -1
	// which is special cased on the receiving end and controls whether the cpu
	// balancing objective is permitted. If this is not updated, the cpu per
	// second will be zero and other stores will likely begin rebalancing
	// towards this store as it will appear underfull.
	if !grunning.Supported() {
		totalStoreCPUTimePerSecond = -1
	} else {
		totalStoreCPUTimePerSecond = math.Max(totalStoreCPUTimePerSecond, 0)
	}

	capacity.RangeCount = rangeCount
	capacity.LeaseCount = leaseCount
	capacity.LogicalBytes = logicalBytes
	capacity.CPUPerSecond = totalStoreCPUTimePerSecond
	capacity.QueriesPerSecond = totalQueriesPerSecond
	capacity.WritesPerSecond = totalWritesPerSecond
	goNow := now.ToTimestamp().GoTime()
	{
		s.ioThreshold.Lock()
		capacity.IOThreshold = *s.ioThreshold.t
		capacity.IOThresholdMax = *s.ioThreshold.t
		maxL0NumSubLevels, _ := s.ioThreshold.maxL0NumSubLevels.Query(goNow)
		maxL0NumFiles, _ := s.ioThreshold.maxL0NumFiles.Query(goNow)
		maxL0Size, _ := s.ioThreshold.maxL0Size.Query(goNow)
		capacity.IOThresholdMax.L0NumSubLevels = int64(maxL0NumSubLevels)
		capacity.IOThresholdMax.L0NumFiles = int64(maxL0NumFiles)
		capacity.IOThresholdMax.L0Size = int64(maxL0Size)
		s.ioThreshold.Unlock()
	}
	capacity.BytesPerReplica = roachpb.PercentilesFromData(bytesPerReplica)
	capacity.WritesPerReplica = roachpb.PercentilesFromData(writesPerReplica)
	s.storeGossip.RecordNewPerSecondStats(totalQueriesPerSecond, totalWritesPerSecond)
	s.replRankings.Update(rankingsAccumulator)
	s.replRankingsByTenant.Update(rankingsByTenantAccumulator)

	s.storeGossip.UpdateCachedCapacity(capacity)

	return capacity, nil
}

// ReplicaCount returns the number of replicas contained by this store. This
// method is O(n) in the number of replicas and should not be called from
// performance critical code.
func (s *Store) ReplicaCount() int {
	var count int
	s.mu.replicasByRangeID.Range(func(_ roachpb.RangeID, _ *Replica) bool {
		count++
		return true
	})
	return count
}

// Registry returns the store registry.
func (s *Store) Registry() *metric.Registry {
	return s.metrics.registry
}

// Metrics returns the store's metric struct.
func (s *Store) Metrics() *StoreMetrics {
	return s.metrics
}

// ReplicateQueueMetrics returns the store's replicateQueue metric struct.
func (s *Store) ReplicateQueueMetrics() ReplicateQueueMetrics {
	return s.replicateQueue.metrics
}

// Descriptor returns a StoreDescriptor including current store
// capacity information.
func (s *Store) Descriptor(ctx context.Context, useCached bool) (*roachpb.StoreDescriptor, error) {
	capacity, err := s.Capacity(ctx, useCached)
	if err != nil {
		return nil, err
	}

	// Initialize the store descriptor.
	return &roachpb.StoreDescriptor{
		StoreID:    s.Ident.StoreID,
		Attrs:      s.Attrs(),
		Node:       *s.nodeDesc,
		Capacity:   capacity,
		Properties: s.Properties(),
	}, nil
}

// RangeFeed registers a rangefeed over the specified span. It sends updates to
// the provided stream and returns a future with an optional error when the rangefeed is
// complete.
func (s *Store) RangeFeed(args *kvpb.RangeFeedRequest, stream rangefeed.Stream) error {
	if filter := s.TestingKnobs().TestingRangefeedFilter; filter != nil {
		if pErr := filter(args, stream); pErr != nil {
			return pErr.GoError()
		}
	}

	if err := verifyKeys(args.Span.Key, args.Span.EndKey, true); err != nil {
		return err
	}

	// Get range and add command to the range for execution.
	repl, err := s.GetReplica(args.RangeID)
	if err != nil {
		return err
	}
	if !repl.IsInitialized() {
		// (*Store).Send has an optimization for uninitialized replicas to send back
		// a NotLeaseHolderError with a hint of where an initialized replica might
		// be found. RangeFeeds can always be served from followers and so don't
		// otherwise return NotLeaseHolderError. For simplicity we also don't return
		// one here.
		return kvpb.NewRangeNotFoundError(args.RangeID, s.StoreID())
	}

	tenID, _ := repl.TenantID()
	pacer := s.cfg.KVAdmissionController.AdmitRangefeedRequest(tenID, args)
	return repl.RangeFeed(args, stream, pacer)
}

// updateReplicationGauges counts a number of simple replication statistics for
// the ranges in this store.
// TODO(bram): #4564 It may be appropriate to compute these statistics while
// scanning ranges. An ideal solution would be to create incremental events
// whenever availability changes.
func (s *Store) updateReplicationGauges(ctx context.Context) error {
	var (
		raftLeaderCount                int64
		leaseHolderCount               int64
		leaseExpirationCount           int64
		leaseEpochCount                int64
		leaseLeaderCount               int64
		leaseLivenessCount             int64
		leaseViolatingPreferencesCount int64
		leaseLessPreferredCount        int64
		raftLeaderNotLeaseHolderCount  int64
		raftLeaderInvalidLeaseCount    int64
		quiescentCount                 int64
		uninitializedCount             int64
		averageQueriesPerSecond        float64
		averageRequestsPerSecond       float64
		averageReadsPerSecond          float64
		averageWritesPerSecond         float64
		averageReadBytesPerSecond      float64
		averageWriteBytesPerSecond     float64
		averageCPUNanosPerSecond       float64

		rangeCount                int64
		unavailableRangeCount     int64
		underreplicatedRangeCount int64
		overreplicatedRangeCount  int64
		decommissioningRangeCount int64
		behindCount               int64
		pausedFollowerCount       int64
		ioOverload                float64
		pendingRaftProposalCount  int64
		slowRaftProposalCount     int64

		locks                          int64
		totalLockHoldDurationNanos     int64
		maxLockHoldDurationNanos       int64
		locksWithWaitQueues            int64
		lockWaitQueueWaiters           int64
		totalLockWaitDurationNanos     int64
		maxLockWaitDurationNanos       int64
		maxLockWaitQueueWaitersForLock int64

		kvflowSendQueueSizeCount int64
		kvflowSendQueueSizeBytes int64

		minMaxClosedTS hlc.Timestamp
	)

	now := s.cfg.Clock.NowAsClockTimestamp()
	goNow := now.ToTimestamp().GoTime()
	clusterNodes := s.ClusterNodeCount()

	s.mu.RLock()
	uninitializedCount = int64(len(s.mu.uninitReplicas))
	s.mu.RUnlock()

	// TODO(kaisun314,kvoli): move this to a per-store admission control metrics
	// struct when available. See pkg/util/admission/granter.go.
	s.ioThreshold.Lock()
	ioOverload, _ = s.ioThreshold.t.Score()
	s.ioThreshold.Unlock()

	// We want to avoid having to read this multiple times during the replica
	// visiting, so load it once up front for all nodes.
	livenessMap := s.cfg.NodeLiveness.ScanNodeVitalityFromCache()
	kvflowSendStats := rac2.RangeSendStreamStats{}
	newStoreReplicaVisitor(s).Visit(func(rep *Replica) bool {
		metrics := rep.Metrics(ctx, now, livenessMap, clusterNodes)
		if metrics.Leader {
			raftLeaderCount++
			if metrics.LeaseValid && !metrics.Leaseholder {
				raftLeaderNotLeaseHolderCount++
			}
			if !metrics.LeaseValid {
				raftLeaderInvalidLeaseCount++
			}
			kvflowSendStats.Clear()
			rep.SendStreamStats(&kvflowSendStats)
			sizeCount, sizeBytes := kvflowSendStats.SumSendQueues()
			kvflowSendQueueSizeCount += sizeCount
			kvflowSendQueueSizeBytes += sizeBytes
		}
		if metrics.Leaseholder {
			s.metrics.RaftQuotaPoolPercentUsed.RecordValue(metrics.QuotaPoolPercentUsed)
			leaseHolderCount++
			switch metrics.LeaseType {
			case roachpb.LeaseNone:
			case roachpb.LeaseExpiration:
				leaseExpirationCount++
			case roachpb.LeaseEpoch:
				leaseEpochCount++
			case roachpb.LeaseLeader:
				leaseLeaderCount++
			}
			if metrics.LivenessLease {
				leaseLivenessCount++
			}
			// NB: Can't be satisfying a less preferred preference, and also
			// satisfying no preferences.
			if metrics.ViolatingLeasePreferences {
				leaseViolatingPreferencesCount++
			} else if metrics.LessPreferredLease {
				leaseLessPreferredCount++
			}
		}
		if metrics.Quiescent {
			quiescentCount++
		}
		if metrics.RangeCounter {
			rangeCount++
			if metrics.Unavailable {
				unavailableRangeCount++
			}
			if metrics.Underreplicated {
				underreplicatedRangeCount++
			}
			if metrics.Overreplicated {
				overreplicatedRangeCount++
			}
			if metrics.Decommissioning {
				// NB: Enqueue is disabled by default from here and throttled async if
				// enabled.
				rep.maybeEnqueueProblemRange(ctx, goNow, metrics.LeaseValid, metrics.Leaseholder)
				decommissioningRangeCount++
			}
		}
		pausedFollowerCount += metrics.PausedFollowerCount
		pendingRaftProposalCount += metrics.PendingRaftProposalCount
		slowRaftProposalCount += metrics.SlowRaftProposalCount
		behindCount += metrics.BehindCount
		loadStats := rep.loadStats.Stats()
		averageQueriesPerSecond += loadStats.QueriesPerSecond
		averageRequestsPerSecond += loadStats.RequestsPerSecond
		averageWritesPerSecond += loadStats.WriteKeysPerSecond
		averageReadsPerSecond += loadStats.ReadKeysPerSecond
		averageReadBytesPerSecond += loadStats.ReadBytesPerSecond
		averageWriteBytesPerSecond += loadStats.WriteBytesPerSecond
		replicaCPUNanosPerSecond := loadStats.RaftCPUNanosPerSecond + loadStats.RequestCPUNanosPerSecond
		averageCPUNanosPerSecond += replicaCPUNanosPerSecond
		s.metrics.RecentReplicaCPUNanosPerSecond.RecordValue(replicaCPUNanosPerSecond)
		s.metrics.RecentReplicaQueriesPerSecond.RecordValue(loadStats.QueriesPerSecond)

		locks += metrics.LockTableMetrics.Locks
		totalLockHoldDurationNanos += metrics.LockTableMetrics.TotalLockHoldDurationNanos
		locksWithWaitQueues += metrics.LockTableMetrics.LocksWithWaitQueues
		lockWaitQueueWaiters += metrics.LockTableMetrics.Waiters
		totalLockWaitDurationNanos += metrics.LockTableMetrics.TotalWaitDurationNanos
		if w := metrics.LockTableMetrics.TopKLocksByWaiters[0].Waiters; w > maxLockWaitQueueWaitersForLock {
			maxLockWaitQueueWaitersForLock = w
		}
		if w := metrics.LockTableMetrics.TopKLocksByHoldDuration[0].HoldDurationNanos; w > maxLockHoldDurationNanos {
			maxLockHoldDurationNanos = w
		}
		if w := metrics.LockTableMetrics.TopKLocksByWaitDuration[0].MaxWaitDurationNanos; w > maxLockWaitDurationNanos {
			maxLockWaitDurationNanos = w
		}
		mc := rep.GetCurrentClosedTimestamp(ctx)
		if minMaxClosedTS.IsEmpty() || mc.Less(minMaxClosedTS) {
			minMaxClosedTS = mc
		}
		return true // more
	})

	s.metrics.RaftLeaderCount.Update(raftLeaderCount)
	s.metrics.RaftLeaderNotLeaseHolderCount.Update(raftLeaderNotLeaseHolderCount)
	s.metrics.RaftLeaderInvalidLeaseCount.Update(raftLeaderInvalidLeaseCount)
	s.metrics.LeaseHolderCount.Update(leaseHolderCount)
	s.metrics.LeaseExpirationCount.Update(leaseExpirationCount)
	s.metrics.LeaseEpochCount.Update(leaseEpochCount)
	s.metrics.LeaseLeaderCount.Update(leaseLeaderCount)
	s.metrics.LeaseViolatingPreferencesCount.Update(leaseViolatingPreferencesCount)
	s.metrics.LeaseLessPreferredCount.Update(leaseLessPreferredCount)
	s.metrics.LeaseLivenessCount.Update(leaseLivenessCount)
	s.metrics.QuiescentCount.Update(quiescentCount)
	s.metrics.UninitializedCount.Update(uninitializedCount)
	s.metrics.AverageQueriesPerSecond.Update(averageQueriesPerSecond)
	s.metrics.AverageRequestsPerSecond.Update(averageRequestsPerSecond)
	s.metrics.AverageWritesPerSecond.Update(averageWritesPerSecond)
	s.metrics.AverageReadsPerSecond.Update(averageReadsPerSecond)
	s.metrics.AverageReadBytesPerSecond.Update(averageReadBytesPerSecond)
	s.metrics.AverageWriteBytesPerSecond.Update(averageWriteBytesPerSecond)
	s.metrics.AverageCPUNanosPerSecond.Update(averageCPUNanosPerSecond)
	s.storeGossip.RecordNewPerSecondStats(averageQueriesPerSecond, averageWritesPerSecond)

	s.metrics.RangeCount.Update(rangeCount)
	s.metrics.UnavailableRangeCount.Update(unavailableRangeCount)
	s.metrics.UnderReplicatedRangeCount.Update(underreplicatedRangeCount)
	s.metrics.OverReplicatedRangeCount.Update(overreplicatedRangeCount)
	s.metrics.DecommissioningRangeCount.Update(decommissioningRangeCount)
	s.metrics.RaftLogFollowerBehindCount.Update(behindCount)
	s.metrics.RaftPausedFollowerCount.Update(pausedFollowerCount)
	s.metrics.IOOverload.Update(ioOverload)
	s.metrics.RaftCommandsPending.Update(pendingRaftProposalCount)
	s.metrics.SlowRaftRequests.Update(slowRaftProposalCount)

	var averageLockHoldDurationNanos int64
	var averageLockWaitDurationNanos int64
	if locks > 0 {
		averageLockHoldDurationNanos = totalLockHoldDurationNanos / locks
	}
	if lockWaitQueueWaiters > 0 {
		averageLockWaitDurationNanos = totalLockWaitDurationNanos / lockWaitQueueWaiters
	}

	s.metrics.Locks.Update(locks)
	s.metrics.AverageLockHoldDurationNanos.Update(averageLockHoldDurationNanos)
	s.metrics.MaxLockHoldDurationNanos.Update(maxLockHoldDurationNanos)
	s.metrics.LocksWithWaitQueues.Update(locksWithWaitQueues)
	s.metrics.LockWaitQueueWaiters.Update(lockWaitQueueWaiters)
	s.metrics.AverageLockWaitDurationNanos.Update(averageLockWaitDurationNanos)
	s.metrics.MaxLockWaitDurationNanos.Update(maxLockWaitDurationNanos)
	s.metrics.MaxLockWaitQueueWaitersForLock.Update(maxLockWaitQueueWaitersForLock)

	if !minMaxClosedTS.IsEmpty() {
		nanos := timeutil.Since(minMaxClosedTS.GoTime()).Nanoseconds()
		s.metrics.ClosedTimestampMaxBehindNanos.Update(nanos)
	}
	s.cfg.KVFlowRangeControllerMetrics.SendQueue.SizeCount.Update(kvflowSendQueueSizeCount)
	s.cfg.KVFlowRangeControllerMetrics.SendQueue.SizeBytes.Update(kvflowSendQueueSizeBytes)

	if err := s.metrics.RecentReplicaCPUNanosPerSecond.Rotate(); err != nil {
		return err
	}
	if err := s.metrics.RecentReplicaQueriesPerSecond.Rotate(); err != nil {
		return err
	}

	return nil
}

func (s *Store) checkpointsDir() string {
	return filepath.Join(s.TODOEngine().GetAuxiliaryDir(), "checkpoints")
}

// checkpointSpans returns key spans containing the given range. The spans may
// be wider, and contain a few extra ranges that surround the given range. The
// extension of the spans gives more information for debugging consistency or
// storage issues, e.g. in situations when a recent reconfiguration like split
// or merge occurred.
func (s *Store) checkpointSpans(desc *roachpb.RangeDescriptor) []roachpb.Span {
	_ = s.TODOEngine() // this method needs to return two sets of spans, one for each engine
	// Find immediate left and right neighbours by range ID.
	var prevID, nextID roachpb.RangeID
	s.mu.replicasByRangeID.Range(func(rangeID roachpb.RangeID, _ *Replica) bool {
		if id, our := rangeID, desc.RangeID; id < our && id > prevID {
			prevID = id
		} else if id > our && (nextID == 0 || id < nextID) {
			nextID = id
		}
		return true
	})
	if prevID == 0 {
		prevID = desc.RangeID
	}
	if nextID == 0 {
		nextID = desc.RangeID
	}

	// Find immediate left and right neighbours by user key.
	s.mu.RLock()
	left := s.mu.replicasByKey.LookupPrecedingReplica(context.Background(), desc.StartKey)
	right := s.mu.replicasByKey.LookupNextReplica(context.Background(), desc.EndKey)
	s.mu.RUnlock()

	// Cover all range IDs (prevID, desc.RangeID, nextID) using a continuous span.
	spanRangeIDs := func(first, last roachpb.RangeID) roachpb.Span {
		return roachpb.Span{
			Key:    keys.MakeRangeIDPrefix(first),
			EndKey: keys.MakeRangeIDPrefix(last).PrefixEnd(),
		}
	}
	spans := []roachpb.Span{spanRangeIDs(prevID, nextID)}

	userKeys := desc.RSpan()
	// Include the rangeID-local data comprising ranges left, desc, and right.
	if left != nil {
		userKeys.Key = left.Desc().StartKey
		// Skip this range ID if it was already covered by [prevID, nextID].
		if id := left.RangeID; id < prevID || id > nextID {
			spans = append(spans, spanRangeIDs(id, id))
		}
	}
	if right != nil {
		userKeys.EndKey = right.Desc().EndKey
		// Skip this range ID if it was already covered by [prevID, nextID].
		if id := right.RangeID; id < prevID || id > nextID {
			spans = append(spans, spanRangeIDs(id, id))
		}
	}
	// Include replicated user key span containing ranges left, desc, and right.
	// TODO(tbg): rangeID is ignored here, make a rangeID-agnostic helper.
	spans = append(spans, rditer.Select(0, rditer.SelectOpts{ReplicatedBySpan: userKeys})...)

	return spans
}

// checkpoint creates a Pebble checkpoint in the auxiliary directory with the
// provided tag used in the filepath. Returns the path to the created checkpoint
// directory. The checkpoint includes only files that intersect with either of
// the provided key spans. If spans is empty, it includes the entire store.
func (s *Store) checkpoint(tag string, spans []roachpb.Span) (string, error) {
	checkpointBase := s.checkpointsDir()
	_ = s.TODOEngine().Env().MkdirAll(checkpointBase, os.ModePerm)
	// Create the checkpoint in a "pending" directory first. If we fail midway, it
	// should be clear that the directory contains an incomplete checkpoint.
	pendingDir := filepath.Join(checkpointBase, tag+"_pending")
	if err := s.TODOEngine().CreateCheckpoint(pendingDir, spans); err != nil {
		return "", err
	}
	// Atomically rename the directory when it represents a complete checkpoint.
	checkpointDir := filepath.Join(checkpointBase, tag)
	if err := s.TODOEngine().Env().Rename(pendingDir, checkpointDir); err != nil {
		return "", err
	}
	return checkpointDir, nil
}

// computeMetrics is a common metric computation that is used by
// ComputeMetricsPeriodically and ComputeMetrics to compute metrics.
func (s *Store) computeMetrics(ctx context.Context) (m storage.Metrics, err error) {
	s.metricsMu.Lock()
	defer s.metricsMu.Unlock()
	return s.computeMetricsLocked(ctx)
}

// computeMetricsLocked should only be used while holding store.metricsMu.
func (s *Store) computeMetricsLocked(ctx context.Context) (m storage.Metrics, err error) {
	ctx = s.AnnotateCtx(ctx)
	if err = s.updateCapacityGauges(ctx); err != nil {
		return m, err
	}
	if err = s.updateReplicationGauges(ctx); err != nil {
		return m, err
	}

	// Get the latest engine metrics.
	m = s.TODOEngine().GetMetrics()
	_ = s.TODOEngine() // TODO(sep-raft-log): log engine should also have metrics
	s.metrics.updateEngineMetrics(m)

	// Get engine Env stats.
	envStats, err := s.TODOEngine().GetEnvStats()
	if err != nil {
		return m, err
	}
	s.metrics.updateEnvStats(*envStats)

	{
		dirs, err := s.TODOEngine().Env().List(s.checkpointsDir())
		if err != nil { // skip NotFound or any other error
			dirs = nil
		}
		s.metrics.RdbCheckpoints.Update(int64(len(dirs)))
	}

	return m, nil
}

// ComputeMetricsPeriodically computes metrics that need to be computed
// periodically along with the regular metrics.
func (s *Store) ComputeMetricsPeriodically(
	ctx context.Context, prevMetrics *storage.MetricsForInterval, tick int,
) (m storage.Metrics, err error) {
	m, err = s.computeMetrics(ctx)
	if err != nil {
		return m, err
	}

	// Get disk stats for the disk associated with this store.
	if s.diskMonitor != nil {
		// IncrementalStats returns the stats captured in the tracer since the
		// last call to incremental stats. These are useful for computing
		// functions like max over an interval. We could use rollingStats.Latest()
		// for cumulative stats, but there is no guarantee that slowness in
		// capturing traces or other timing issues will not cause rollingStats to
		// be empty, in which case rollingStats.Latest() will be empty. For the
		// real cumulative stats we use Monitor.CumulativeStats which is robust to
		// such slowness (since it will return the latest value even if it is the
		// same as the last call).
		rollingStats := s.diskMonitor.IncrementalStats()
		cumulativeStats, cumulativeStatsErr := s.diskMonitor.CumulativeStats()
		s.metrics.updateDiskStats(ctx, rollingStats, cumulativeStats, cumulativeStatsErr)
	}

	wt := m.Flush.WriteThroughput

	updateWindowedHistogram := func(
		prevCum prometheusgo.Metric, curCum prometheus.Histogram, wh *metric.ManualWindowHistogram) error {
		// The current cumulative latency histogram is subtracted from the
		// previous cumulative value producing a delta that represent the change
		// between the two points in time. Since the prometheus.Histogram does not
		// expose any of the data it collects, the current metrics need to be
		// written to an intermediate struct to facilitate the calculation.
		windowedHist := &prometheusgo.Metric{}
		err := curCum.Write(windowedHist)
		if err != nil {
			return err
		}
		metric.SubtractPrometheusHistograms(windowedHist.GetHistogram(), prevCum.GetHistogram())
		wh.Update(curCum, windowedHist.Histogram)
		return nil
	}
	if prevMetrics != nil {
		// The following code is subtracting previous and current metrics from the
		// storage engine in order to expose windowed metrics. This calculation is
		// done on each tick and is required since the metrics exposed by the
		// storage engine are cumulative. However, the metrics returned from this
		// function are intended to be over an interval.

		// Subtract the cumulative Flush WriteThroughput from the previous
		// cumulative value producing a delta that represent the change between the
		// two points in time.
		wt.Subtract(prevMetrics.FlushWriteThroughput)

		if err := updateWindowedHistogram(
			prevMetrics.WALFsyncLatency, m.LogWriter.FsyncLatency, s.metrics.FsyncLatency); err != nil {
			return m, err
		}
		if m.WAL.Failover.FailoverWriteAndSyncLatency != nil {
			if err := updateWindowedHistogram(prevMetrics.WALFailoverWriteAndSyncLatency,
				m.WAL.Failover.FailoverWriteAndSyncLatency, s.metrics.WALFailoverWriteAndSyncLatency); err != nil {
				return m, err
			}
		}
	}

	flushUtil := 0.0
	if wt.WorkDuration > 0 {
		flushUtil = float64(wt.WorkDuration) / float64(wt.WorkDuration+wt.IdleDuration)
	}
	s.metrics.FlushUtilization.Update(flushUtil)

	// Log this metric infrequently (with current configurations,
	// every 10 minutes). Trigger on tick 1 instead of tick 0 so that
	// non-periodic callers of this method don't trigger expensive
	// stats.
	if tick%logSSTInfoTicks == 1 /* every 10m */ {
		// NB: The initial blank line ensures that compaction stats display
		// will not contain the log prefix.
		log.Storage.Infof(ctx, "\n%s", m.Metrics)
	}
	// Periodically emit a store stats structured event to the TELEMETRY channel,
	// if reporting is enabled. These events are intended to be emitted at low
	// frequency. Trigger on every (N-1)-th tick to avoid spamming the telemetry
	// channel if crash-looping.
	if logcrash.DiagnosticsReportingEnabled.Get(&s.ClusterSettings().SV) &&
		tick%logStoreTelemetryTicks == logStoreTelemetryTicks-1 {
		// The stats event is populated from a subset of the Metrics.
		e := m.AsStoreStatsEvent()
		e.NodeId = int32(s.NodeID())
		e.StoreId = int32(s.StoreID())
		log.StructuredEvent(ctx, severity.INFO, &e)
	}
	return m, nil
}

// ComputeMetrics immediately computes the current value of store metrics which
// cannot be computed incrementally. This method should be invoked periodically
// by a higher-level system which records store metrics.
func (s *Store) ComputeMetrics(ctx context.Context) error {
	_, err := s.computeMetrics(ctx)
	return err
}

// ClusterNodeCount returns this store's view of the number of nodes in the
// cluster. This is the metric used for adapative zone configs; ranges will not
// be reported as underreplicated if it is low. Tests that wait for full
// replication by tracking the underreplicated metric must also check for the
// expected ClusterNodeCount to avoid catching the cluster while the first node
// is initialized but the other nodes are not.
func (s *Store) ClusterNodeCount() int {
	return s.cfg.StorePool.ClusterNodeCount()
}

// HotReplicaInfo contains a range descriptor and its QPS.
type HotReplicaInfo struct {
	Desc                *roachpb.RangeDescriptor
	QPS                 float64
	RequestsPerSecond   float64
	ReadKeysPerSecond   float64
	WriteKeysPerSecond  float64
	WriteBytesPerSecond float64
	ReadBytesPerSecond  float64
	CPUTimePerSecond    float64
}

// HottestReplicas returns the hottest replicas on a store, sorted by their
// QPS. Only contains ranges for which this store is the leaseholder.
//
// Note that this uses cached information, so it's cheap but may be slightly
// out of date.
func (s *Store) HottestReplicas() []HotReplicaInfo {
	topLoad := s.replRankings.TopLoad(load.Queries)
	return mapToHotReplicasInfo(topLoad)
}

// HottestReplicasByTenant returns the hottest replicas on a store for specified
// tenant ID. It works identically as HottestReplicas func with only exception that
// hottest replicas are grouped by tenant ID.
func (s *Store) HottestReplicasByTenant(tenantID roachpb.TenantID) []HotReplicaInfo {
	topQPS := s.replRankingsByTenant.TopLoad(tenantID, load.Queries)
	return mapToHotReplicasInfo(topQPS)
}

func mapToHotReplicasInfo(repls []CandidateReplica) []HotReplicaInfo {
	hotRepls := make([]HotReplicaInfo, len(repls))
	for i := range repls {
		ri := repls[i].RangeUsageInfo()
		hotRepls[i].Desc = repls[i].Desc()
		hotRepls[i].QPS = ri.QueriesPerSecond
		hotRepls[i].RequestsPerSecond = ri.RequestsPerSecond
		hotRepls[i].WriteKeysPerSecond = ri.WritesPerSecond
		hotRepls[i].ReadKeysPerSecond = ri.ReadsPerSecond
		hotRepls[i].WriteBytesPerSecond = ri.WriteBytesPerSecond
		hotRepls[i].ReadBytesPerSecond = ri.ReadBytesPerSecond
		hotRepls[i].CPUTimePerSecond = ri.RaftCPUNanosPerSecond + ri.RequestCPUNanosPerSecond
	}
	return hotRepls
}

// ReplicateQueueDryRun runs the given replica through the replicate queue
// (using the allocator) without actually carrying out any changes, returning
// all trace messages collected along the way.
// Intended to help power a debug endpoint.
func (s *Store) ReplicateQueueDryRun(
	ctx context.Context, repl *Replica,
) (tracingpb.Recording, error) {
	ctx, collectAndFinish := tracing.ContextWithRecordingSpan(ctx,
		s.cfg.AmbientCtx.Tracer, "replicate queue dry run",
	)
	defer collectAndFinish()
	desc := repl.Desc()
	conf, err := repl.LoadSpanConfig(ctx)
	if err != nil {
		log.Eventf(ctx, "error simulating allocator unable to load span config %s: %s", repl, err)
		return collectAndFinish(), nil
	}
	_, err = s.replicateQueue.processOneChange(
		ctx, repl, desc, conf, false /* scatter */, true, /* dryRun */
	)
	if err != nil {
		log.Eventf(ctx, "error simulating allocator on replica %s: %s", repl, err)
	}
	return collectAndFinish(), nil
}

// AllocatorCheckRange takes a range descriptor and a node liveness override (or
// nil, to use the configured StorePool's), looks up the configuration of
// range, and utilizes the allocator to get the action needed to repair the
// range, as well as any upreplication target if needed, returning along with
// any encountered errors as well as the collected tracing spans.
//
// This functionality is similar to ReplicateQueueDryRun, but operates on the
// basis of a range, evaluating the action and target determined by the allocator.
// The range does not need to have a replica on the store in order to check the
// needed allocator action and target. The store pool, if provided, will be
// used, otherwise it will fall back to the store's configured store pool.
//
// Assuming the span config is available, a valid allocator action should
// always be returned, even in case of errors.
//
// NB: In the case of removal or rebalance actions, a target cannot be
// evaluated, as a leaseholder is required for evaluation.
func (s *Store) AllocatorCheckRange(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	collectTraces bool,
	overrideStorePool storepool.AllocatorStorePool,
) (allocatorimpl.AllocatorAction, roachpb.ReplicationTarget, tracingpb.Recording, error) {
	var spanOptions []tracing.SpanOption
	if collectTraces {
		spanOptions = append(spanOptions, tracing.WithRecording(tracingpb.RecordingVerbose))
	}
	ctx, sp := tracing.EnsureChildSpan(ctx, s.cfg.AmbientCtx.Tracer, "allocator check range", spanOptions...)

	confReader, err := s.GetConfReader(ctx)
	if err != nil {
		log.Eventf(ctx, "span configs unavailable: %s", err)
		return allocatorimpl.AllocatorNoop, roachpb.ReplicationTarget{}, sp.FinishAndGetConfiguredRecording(), err
	}

	conf, _, err := confReader.GetSpanConfigForKey(ctx, desc.StartKey)
	if err != nil {
		log.Eventf(ctx, "error retrieving span config for range %s: %s", desc, err)
		return allocatorimpl.AllocatorNoop, roachpb.ReplicationTarget{}, sp.FinishAndGetConfiguredRecording(), err
	}

	// If a store pool was provided, use that, otherwise use the store's
	// configured store pool.
	var storePool storepool.AllocatorStorePool
	if overrideStorePool != nil {
		storePool = overrideStorePool
	} else if s.cfg.StorePool != nil {
		storePool = s.cfg.StorePool
	}

	action, _ := s.allocator.ComputeAction(ctx, storePool, &conf, desc)

	// In the case that the action does not require a target, return immediately.
	if !(action.Add() || action.Replace()) {
		return action, roachpb.ReplicationTarget{}, sp.FinishAndGetConfiguredRecording(), err
	}

	filteredVoters, filteredNonVoters, replacing, nothingToDo, err :=
		allocatorimpl.FilterReplicasForAction(storePool, desc, action)

	if nothingToDo || err != nil {
		return action, roachpb.ReplicationTarget{}, sp.FinishAndGetConfiguredRecording(), err
	}

	target, _, err := s.allocator.AllocateTarget(ctx, storePool, &conf,
		filteredVoters, filteredNonVoters, replacing, action.ReplicaStatus(), action.TargetReplicaType(),
	)
	if err == nil {
		log.Eventf(ctx, "found valid allocation of %s target %v", action.TargetReplicaType(), target)

		// Ensure that if we are upreplicating, we are avoiding a state in which we
		// have a fragile quorum that we cannot avoid by allocating more voters.
		fragileQuorumErr := s.allocator.CheckAvoidsFragileQuorum(
			ctx,
			storePool,
			&conf,
			desc.Replicas().VoterDescriptors(),
			filteredVoters,
			action.ReplicaStatus(),
			action.TargetReplicaType(),
			target,
			replacing != nil,
		)

		if fragileQuorumErr != nil {
			err = errors.Wrap(fragileQuorumErr, "avoid up-replicating to fragile quorum")
		}
	}

	return action, target, sp.FinishAndGetConfiguredRecording(), err
}

// Enqueue runs the given replica through the requested queue. If `async` is
// specified, the replica is enqueued into the requested queue for asynchronous
// processing and this method returns nothing. Otherwise, it returns all trace
// events collected along the way as well as the error message returned from the
// queue's process method, if any. Intended to help power the
// server.decommissionMonitor and an admin debug endpoint.
func (s *Store) Enqueue(
	ctx context.Context, queueName string, repl *Replica, skipShouldQueue bool, async bool,
) (processError error, enqueueError error) {
	ctx = repl.AnnotateCtx(ctx)

	if fn := s.TestingKnobs().EnqueueReplicaInterceptor; fn != nil {
		fn(queueName, repl)
	}

	// Do not enqueue uninitialized replicas. The baseQueue ignores these during
	// normal queue scheduling, but we error here to signal to the user that the
	// operation was unsuccessful.
	if !repl.IsInitialized() {
		return nil, errors.Errorf("not enqueueing uninitialized replica %s", repl)
	}

	var queue replicaQueue
	var qImpl queueImpl
	var needsLease bool
	for _, q := range s.scanner.queues {
		if strings.EqualFold(q.Name(), queueName) {
			queue = q
			qImpl = q.(queueImpl)
			needsLease = q.NeedsLease()
		}
	}
	if queue == nil {
		return nil, errors.Errorf("unknown queue type %q", queueName)
	}

	confReader, err := s.GetConfReader(ctx)
	if err != nil {
		return nil, errors.Wrap(err,
			"unable to retrieve conf reader, cannot run queue; make sure "+
				"the cluster has been initialized and all nodes connected to it")
	}

	// Many queues are only meant to be run on leaseholder replicas, so attempt to
	// take the lease here or bail out early if a different replica has it.
	if needsLease {
		if _, pErr := repl.redirectOnOrAcquireLease(ctx); pErr != nil {
			return nil, errors.Wrapf(pErr.GoError(), "replica %v does not have the range lease", repl)
		}
	}

	if async {
		// NB: 1e5 is a placeholder for now. We want to use a high enough priority
		// to ensure that these replicas are priority-ordered first (just below the
		// replacement of dead replicas).
		//
		// TODO(aayush): Once we address
		// https://github.com/cockroachdb/cockroach/issues/79266, we can consider
		// removing the `AddAsync` path here and just use the `MaybeAddAsync` path,
		// which will allow us to stop specifiying the priority ad-hoc.
		const asyncEnqueuePriority = 1e5
		if skipShouldQueue {
			queue.AddAsync(ctx, repl, asyncEnqueuePriority)
		} else {
			queue.MaybeAddAsync(ctx, repl, repl.Clock().NowAsClockTimestamp())
		}
		return nil, nil
	}

	if !skipShouldQueue {
		log.Eventf(ctx, "running %s.shouldQueue", queueName)
		shouldQueue, priority := qImpl.shouldQueue(ctx, s.cfg.Clock.NowAsClockTimestamp(), repl, confReader)
		log.Eventf(ctx, "shouldQueue=%v, priority=%f", shouldQueue, priority)
		if !shouldQueue {
			return nil, nil
		}
	}

	log.Eventf(ctx, "running %s.process", queueName)
	processed, processErr := qImpl.process(ctx, repl, confReader)
	log.Eventf(ctx, "processed: %t (err: %v)", processed, processErr)
	return processErr, nil
}

// PurgeOutdatedReplicas purges all replicas with a version less than the one
// specified. This entails clearing out replicas in the replica GC queue that
// fit the bill.
func (s *Store) PurgeOutdatedReplicas(ctx context.Context, version roachpb.Version) error {
	if interceptor := s.TestingKnobs().PurgeOutdatedReplicasInterceptor; interceptor != nil {
		interceptor()
	}

	// Let's set a reasonable bound on the number of replicas being processed in
	// parallel.
	qp := quotapool.NewIntPool("purge-outdated-replicas", 50)
	g := ctxgroup.WithContext(ctx)
	s.VisitReplicas(func(repl *Replica) (wantMore bool) {
		if !repl.Version().Less(version) {
			// Nothing to do here. The less-than check also considers replicas
			// with unset replica versions, which are only possible if they're
			// left-over, GC-able replicas from before the first below-raft
			// migration. We'll want to purge those.
			return true
		}

		alloc, err := qp.Acquire(ctx, 1)
		if err != nil {
			g.GoCtx(func(ctx context.Context) error {
				return err
			})
			return false
		}

		g.GoCtx(func(ctx context.Context) error {
			defer alloc.Release()

			processed, err := s.replicaGCQueue.process(ctx, repl, nil)
			if err != nil {
				return errors.Wrapf(err, "on %s", repl.Desc())
			}
			if !processed {
				// We're either still part of the raft group, in which same
				// something has gone horribly wrong, or more likely (though
				// still very unlikely in practice): this range has been merged
				// away, and this store has the replica of the subsuming range
				// where we're unable to determine if it has applied the merge
				// trigger. See replicaGCQueue.process for more details. Either
				// way, we error out.
				return errors.Newf("unable to gc %s", repl.Desc())
			}
			return nil
		})

		return true
	})

	return g.Wait()
}

// WaitForSpanConfigSubscription waits until the store is wholly subscribed to
// the global span configurations state.
func (s *Store) WaitForSpanConfigSubscription(ctx context.Context) error {
	if s.cfg.SpanConfigsDisabled {
		return nil // nothing to do here
	}

	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		if !s.cfg.SpanConfigSubscriber.LastUpdated().IsEmpty() {
			return nil
		}

		log.Warningf(ctx, "waiting for span config subscription...")
		continue
	}

	return errors.Newf("unable to subscribe to span configs")
}

// registerLeaseholder registers the provided replica as a leaseholder in the
// node's closed timestamp side transport.
func (s *Store) registerLeaseholder(
	ctx context.Context, r *Replica, leaseSeq roachpb.LeaseSequence,
) {
	if s.ctSender != nil {
		s.ctSender.RegisterLeaseholder(ctx, r, leaseSeq)
	}
}

// unregisterLeaseholder unregisters the provided replica from node's closed
// timestamp side transport if it had been previously registered as a
// leaseholder.
func (s *Store) unregisterLeaseholder(ctx context.Context, r *Replica) {
	s.unregisterLeaseholderByID(ctx, r.RangeID)
}

// unregisterLeaseholderByID is like unregisterLeaseholder, but it accepts a
// range ID instead of a replica.
func (s *Store) unregisterLeaseholderByID(ctx context.Context, rangeID roachpb.RangeID) {
	if s.ctSender != nil {
		s.ctSender.UnregisterLeaseholder(ctx, s.StoreID(), rangeID)
	}
}

// getRootMemoryMonitorForKV returns a BytesMonitor to use for KV memory
// tracking.
func (s *Store) getRootMemoryMonitorForKV() *mon.BytesMonitor {
	return s.cfg.KVMemoryMonitor
}

func (s *Store) getRangefeedScheduler() *rangefeed.Scheduler {
	return s.rangefeedScheduler
}

// getNodeRangeCount returns the number of total ranges on this node. The value
// is cached and updated every few seconds by Node.computeMetricsPeriodically.
func (s *Store) getNodeRangeCount() int64 {
	return s.cfg.RangeCount.Load()
}

// Implementation of the storeForTruncator interface.
type storeForTruncatorImpl Store

var _ storeForTruncator = &storeForTruncatorImpl{}

func (s *storeForTruncatorImpl) acquireReplicaForTruncator(
	rangeID roachpb.RangeID,
) replicaForTruncator {
	r, err := (*Store)(s).GetReplica(rangeID)
	if err != nil || r == nil {
		// The only error we can see here is kvpb.NewRangeNotFoundError, so we
		// can ignore it.
		return nil
	}
	r.raftMu.Lock()
	if isAlive := func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.mu.destroyStatus.IsAlive()
	}(); !isAlive {
		r.raftMu.Unlock()
		return nil
	}
	return (*raftTruncatorReplica)(r)
}

func (s *storeForTruncatorImpl) releaseReplicaForTruncator(r replicaForTruncator) {
	replica := r.(*raftTruncatorReplica)
	replica.raftMu.Unlock()
}

func (s *storeForTruncatorImpl) getEngine() storage.Engine {
	// TODO(sep-raft-log): we'll need the log engine here but need
	// to read code to see if more needs to be done.
	return (*Store)(s).TODOEngine()
}

func init() {
	tracing.RegisterTagRemapping("s", "store")
}
