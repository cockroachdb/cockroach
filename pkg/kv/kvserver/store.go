// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/sidetransport"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/idalloc"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/intentresolver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftentry"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/tenantrate"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/tscache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnrecovery"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnwait"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigstore"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/shuffle"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	raft "go.etcd.io/etcd/raft/v3"
	"golang.org/x/time/rate"
)

const (
	// rangeIDAllocCount is the number of Range IDs to allocate per allocation.
	rangeIDAllocCount = 10

	// defaultRaftEntryCacheSize is the default size in bytes for a
	// store's Raft log entry cache.
	defaultRaftEntryCacheSize = 1 << 24 // 16M

	// replicaRequestQueueSize specifies the maximum number of requests to queue
	// for a replica.
	replicaRequestQueueSize = 100

	defaultGossipWhenCapacityDeltaExceedsFraction = 0.01

	// systemDataGossipInterval is the interval at which range lease
	// holders verify that the most recent system data is gossiped.
	// This ensures that system data is always eventually gossiped, even
	// if a range lease holder experiences a failure causing a missed
	// gossip update.
	systemDataGossipInterval = 1 * time.Minute
)

var storeSchedulerConcurrency = envutil.EnvOrDefaultInt(
	// For small machines, we scale the scheduler concurrency by the number of
	// CPUs. 8*NumCPU was determined in 9a68241 (April 2017) as the optimal
	// concurrency level on 8 CPU machines. For larger machines, we've seen
	// (#56851) that this scaling curve can be too aggressive and lead to too much
	// contention in the Raft scheduler, so we cap the concurrency level at 96.
	//
	// As of November 2020, this default value could be re-tuned.
	"COCKROACH_SCHEDULER_CONCURRENCY", min(8*runtime.GOMAXPROCS(0), 96))

var logSSTInfoTicks = envutil.EnvOrDefaultInt(
	"COCKROACH_LOG_SST_INFO_TICKS_INTERVAL", 60)

// bulkIOWriteLimit is defined here because it is used by BulkIOWriteLimiter.
var bulkIOWriteLimit = settings.RegisterByteSizeSetting(
	settings.TenantWritable,
	"kv.bulk_io_write.max_rate",
	"the rate limit (bytes/sec) to use for writes to disk on behalf of bulk io ops",
	1<<40,
).WithPublic()

// addSSTableRequestLimit limits concurrent AddSSTable requests.
var addSSTableRequestLimit = settings.RegisterIntSetting(
	settings.TenantWritable,
	"kv.bulk_io_write.concurrent_addsstable_requests",
	"number of concurrent AddSSTable requests per store before queueing",
	1,
	settings.PositiveInt,
)

// addSSTableAsWritesRequestLimit limits concurrent AddSSTable requests with
// IngestAsWrites set. These are smaller (kv.bulk_io_write.small_write_size),
// and will end up in the Pebble memtable (default 64 MB) before flushing to
// disk, so we can allow a greater amount of concurrency than regular AddSSTable
// requests. Applied independently of concurrent_addsstable_requests.
var addSSTableAsWritesRequestLimit = settings.RegisterIntSetting(
	settings.TenantWritable,
	"kv.bulk_io_write.concurrent_addsstable_as_writes_requests",
	"number of concurrent AddSSTable requests ingested as writes per store before queueing",
	10,
	settings.PositiveInt,
)

// concurrentRangefeedItersLimit limits concurrent rangefeed catchup iterators.
var concurrentRangefeedItersLimit = settings.RegisterIntSetting(
	settings.TenantWritable,
	"kv.rangefeed.concurrent_catchup_iterators",
	"number of rangefeeds catchup iterators a store will allow concurrently before queueing",
	16,
	settings.PositiveInt,
)

// concurrentscanInterleavedIntentsLimit is the number of concurrent
// ScanInterleavedIntents requests that will be run on a store. Used as part
// of pre-evaluation throttling.
var concurrentscanInterleavedIntentsLimit = settings.RegisterIntSetting(
	settings.TenantWritable,
	"kv.migration.concurrent_scan_interleaved_intents",
	"number of scan interleaved intents requests a store will handle concurrently before queueing",
	1,
	settings.PositiveInt,
)

// Minimum time interval between system config updates which will lead to
// enqueuing replicas.
var queueAdditionOnSystemConfigUpdateRate = settings.RegisterFloatSetting(
	settings.TenantWritable,
	"kv.store.system_config_update.queue_add_rate",
	"the rate (per second) at which the store will add, all replicas to the split and merge queue due to system config gossip",
	.5,
	settings.NonNegativeFloat,
)

// Minimum time interval between system config updates which will lead to
// enqueuing replicas. The default is relatively high to deal with startup
// scenarios.
var queueAdditionOnSystemConfigUpdateBurst = settings.RegisterIntSetting(
	settings.TenantWritable,
	"kv.store.system_config_update.queue_add_burst",
	"the burst rate at which the store will add all replicas to the split and merge queue due to system config gossip",
	32,
	settings.NonNegativeInt,
)

// leaseTransferWait is the timeout for a single iteration of draining range leases.
var leaseTransferWait = func() *settings.DurationSetting {
	s := settings.RegisterDurationSetting(
		settings.TenantWritable,
		leaseTransferWaitSettingName,
		"the timeout for a single iteration of the range lease transfer phase of draining "+
			"(note that the --drain-wait parameter for cockroach node drain may need adjustment "+
			"after changing this setting)",
		5*time.Second,
		func(v time.Duration) error {
			if v < 0 {
				return errors.Errorf("cannot set %s to a negative duration: %s",
					leaseTransferWaitSettingName, v)
			}
			return nil
		},
	)
	s.SetVisibility(settings.Public)
	return s
}()

const leaseTransferWaitSettingName = "server.shutdown.lease_transfer_wait"

// ExportRequestsLimit is the number of Export requests that can run at once.
// Each extracts data from RocksDB to a temp file and then uploads it to cloud
// storage. In order to not exhaust the disk or memory, or saturate the network,
// limit the number of these that can be run in parallel. This number was chosen
// by a guessing - it could be improved by more measured heuristics. Exported
// here since we check it in in the caller to limit generated requests as well
// to prevent excessive queuing.
var ExportRequestsLimit = settings.RegisterIntSetting(
	settings.TenantWritable,
	"kv.bulk_io_write.concurrent_export_requests",
	"number of export requests a store will handle concurrently before queuing",
	3,
	settings.PositiveInt,
)

// TestStoreConfig has some fields initialized with values relevant in tests.
func TestStoreConfig(clock *hlc.Clock) StoreConfig {
	return testStoreConfig(clock, clusterversion.TestingBinaryVersion)
}

func testStoreConfig(clock *hlc.Clock, version roachpb.Version) StoreConfig {
	if clock == nil {
		clock = hlc.NewClock(hlc.UnixNano, time.Nanosecond)
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

		// Use a constant empty system config, which mirrors the previously
		// existing logic to install an empty system config in gossip.
		SystemConfigProvider: config.NewConstantSystemConfigProvider(
			config.NewSystemConfig(zonepb.DefaultZoneConfigRef()),
		),
	}

	// Use shorter Raft tick settings in order to minimize start up and failover
	// time in tests.
	sc.RaftHeartbeatIntervalTicks = 1
	sc.RaftElectionTimeoutTicks = 3
	sc.RaftTickInterval = 100 * time.Millisecond
	sc.SetDefaults()
	return sc
}

func newRaftConfig(
	strg raft.Storage, id uint64, appliedIndex uint64, storeCfg StoreConfig, logger raft.Logger,
) *raft.Config {
	return &raft.Config{
		ID:                        id,
		Applied:                   appliedIndex,
		ElectionTick:              storeCfg.RaftElectionTimeoutTicks,
		HeartbeatTick:             storeCfg.RaftHeartbeatIntervalTicks,
		MaxUncommittedEntriesSize: storeCfg.RaftMaxUncommittedEntriesSize,
		MaxCommittedSizePerReady:  storeCfg.RaftMaxCommittedSizePerReady,
		MaxSizePerMsg:             storeCfg.RaftMaxSizePerMsg,
		MaxInflightMsgs:           storeCfg.RaftMaxInflightMsgs,
		Storage:                   strg,
		Logger:                    logger,

		PreVote: true,
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

// A NotBootstrappedError indicates that an engine has not yet been
// bootstrapped due to a store identifier not being present.
type NotBootstrappedError struct{}

// Error formats error.
func (e *NotBootstrappedError) Error() string {
	return "store has not been bootstrapped"
}

// A storeReplicaVisitor calls a visitor function for each of a store's
// initialized Replicas (in unspecified order). It provides an option
// to visit replicas in increasing RangeID order.
type storeReplicaVisitor struct {
	store   *Store
	repls   []*Replica // Replicas to be visited
	ordered bool       // Option to visit replicas in sorted order
	visited int        // Number of visited ranges, -1 before first call to Visit()
}

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
	rs.ordered = true
	return rs
}

// Visit calls the visitor with each Replica until false is returned.
func (rs *storeReplicaVisitor) Visit(visitor func(*Replica) bool) {
	// Copy the range IDs to a slice so that we iterate over some (possibly
	// stale) view of all Replicas without holding the Store lock. In particular,
	// no locks are acquired during the copy process.
	rs.repls = nil
	rs.store.mu.replicasByRangeID.Range(func(repl *Replica) {
		rs.repls = append(rs.repls, repl)
	})

	if rs.ordered {
		// If the replicas were requested in sorted order, perform the sort.
		sort.Sort(rs)
	} else {
		// The Replicas are already in "unspecified order" due to map iteration,
		// but we want to make sure it's completely random to prevent issues in
		// tests where stores are scanning replicas in lock-step and one store is
		// winning the race and getting a first crack at processing the replicas on
		// its queues.
		//
		// TODO(peter): Re-evaluate whether this is necessary after we allow
		// rebalancing away from the leaseholder. See TestRebalance_3To5Small.
		shuffle.Shuffle(rs)
	}

	rs.visited = 0
	for _, repl := range rs.repls {
		// TODO(tschottdorf): let the visitor figure out if something's been
		// destroyed once we return errors from mutexes (#9190). After all, it
		// can still happen with this code.
		rs.visited++
		repl.mu.RLock()
		destroyed := repl.mu.destroyStatus
		initialized := repl.isInitializedRLocked()
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

/*
A Store maintains a set of Replicas whose data is stored on a storage.Engine
usually corresponding to a dedicated storage medium. It also houses a collection
of subsystems that, in broad terms, perform maintenance of each Replica on the
Store when required. In particular, this includes various queues such as the
split, merge, rebalance, GC, and replicaGC queues (to name just a few).

INVARIANT: the set of all Ranges (as determined by, e.g. a transactionally
consistent scan of the meta index ranges) always exactly covers the addressable
keyspace roachpb.KeyMin (inclusive) to roachpb.KeyMax (exclusive).

Ranges

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

RangeDescriptor

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
the (application of the) roachpb.EndTxnRequest committing the transaction.

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

Replica Lifecycle

A Replica should be thought of primarily as a State Machine applying commands
from a replicated log (the log being replicated across the members of the
Range). The Store's RaftTransport receives Raft messages from Replicas residing
on other Stores and routes them to the appropriate Replicas via
Store.HandleRaftRequest (which is part of the RaftMessageHandler interface),
ultimately resulting in a call to Replica.handleRaftReadyRaftMuLocked, which
houses the integration with the etcd/raft library (raft.RawNode). This may
generate Raft messages to be sent to other Stores; these are handed to
Replica.sendRaftMessagesRaftMuLocked which ultimately hands them to the Store's
RaftTransport.SendAsync method. Raft uses message passing (not
request-response), and outgoing messages will use a gRPC stream that differs
from that used for incoming messages (which makes asymmetric partitions more
likely in case of stream-specific problems). The steady state is relatively
straightforward but when Ranges are being reconfigured, an understanding the
Replica Lifecycle becomes important and upholding the Store's invariants becomes
more complex.

A first phenomenon to understand is that of uninitialized Replicas, which is the
State Machine at applied index zero, i.e. has an empty state. In CockroachDB, an
uninitialized Replica can only advance to a nonzero log position ("become
initialized") via a Raft snapshot (this is because we initialize all Ranges in
the system at log index raftInitialLogIndex which allows us to write arbitrary
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
Replicas (via roachpb.SubsumeRequest) to prevent additional read or write
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
	Ident           *roachpb.StoreIdent // pointer to catch access before Start() is called
	cfg             StoreConfig
	db              *kv.DB
	engine          storage.Engine // The underlying key-value store
	tsCache         tscache.Cache  // Most recent timestamps for keys / key ranges
	allocator       Allocator      // Makes allocation decisions
	replRankings    *replicaRankings
	storeRebalancer *StoreRebalancer
	rangeIDAlloc    *idalloc.Allocator // Range ID allocator
	mvccGCQueue     *mvccGCQueue       // MVCC GC queue
	mergeQueue      *mergeQueue        // Range merging queue
	splitQueue      *splitQueue        // Range splitting queue
	replicateQueue  *replicateQueue    // Replication queue
	replicaGCQueue  *replicaGCQueue    // Replica GC queue
	raftLogQueue    *raftLogQueue      // Raft log truncation queue
	// Carries out truncations proposed by the raft log queue, and "replicated"
	// via raft, when they are safe. Created in Store.Start.
	raftTruncator      *raftLogTruncator
	raftSnapshotQueue  *raftSnapshotQueue          // Raft repair queue
	tsMaintenanceQueue *timeSeriesMaintenanceQueue // Time series maintenance queue
	scanner            *replicaScanner             // Replica scanner
	consistencyQueue   *consistencyQueue           // Replica consistency check queue
	consistencyLimiter *quotapool.RateLimiter      // Rate limits consistency checks
	metrics            *StoreMetrics
	intentResolver     *intentresolver.IntentResolver
	recoveryMgr        txnrecovery.Manager
	raftEntryCache     *raftentry.Cache
	limiters           batcheval.Limiters
	txnWaitMetrics     *txnwait.Metrics
	sstSnapshotStorage SSTSnapshotStorage
	protectedtsReader  spanconfig.ProtectedTSReader
	ctSender           *sidetransport.Sender

	// gossipRangeCountdown and leaseRangeCountdown are countdowns of
	// changes to range and leaseholder counts, after which the store
	// descriptor will be re-gossiped earlier than the normal periodic
	// gossip interval. Updated atomically.
	gossipRangeCountdown int32
	gossipLeaseCountdown int32
	// gossipQueriesPerSecondVal and gossipWritesPerSecond serve similar
	// purposes, but simply record the most recently gossiped value so that we
	// can tell if a newly measured value differs by enough to justify
	// re-gossiping the store.
	gossipQueriesPerSecondVal syncutil.AtomicFloat64
	gossipWritesPerSecondVal  syncutil.AtomicFloat64

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

	// Semaphore to limit concurrent non-empty snapshot application.
	snapshotApplySem chan struct{}

	// Track newly-acquired expiration-based leases that we want to proactively
	// renew. An object is sent on the signal whenever a new entry is added to
	// the map.
	renewableLeases       syncutil.IntMap // map[roachpb.RangeID]*Replica
	renewableLeasesSignal chan struct{}   // 1-buffered

	// draining holds a bool which indicates whether this store is draining. See
	// SetDraining() for a more detailed explanation of behavior changes.
	//
	// TODO(bdarnell,tschottdorf): Would look better inside of `mu`, which at
	// the time of its creation was riddled with deadlock (but that situation
	// has likely improved).
	draining atomic.Value

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
		replicasByRangeID rangeIDReplicaMap
		// A btree key containing objects of type *Replica or *ReplicaPlaceholder.
		// Both types have an associated key range; the btree is keyed on their
		// start keys.
		//
		// INVARIANT: Any ReplicaPlaceholder in this map is also in replicaPlaceholders.
		// INVARIANT: Any Replica with Replica.IsInitialized()==true is also in replicasByRangeID.
		replicasByKey *storeReplicaBTree
		// All *Replica objects for which Replica.IsInitialized is false.
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
		m map[roachpb.RangeID]struct{}
	}

	// replicaQueues is a map of per-Replica incoming request queues. These
	// queues might more naturally belong in Replica, but are kept separate to
	// avoid reworking the locking in getOrCreateReplica which requires
	// Replica.raftMu to be held while a replica is being inserted into
	// Store.mu.replicas.
	replicaQueues syncutil.IntMap // map[roachpb.RangeID]*raftRequestQueue

	scheduler *raftScheduler

	// livenessMap is a map from nodeID to a bool indicating
	// liveness. It is updated periodically in raftTickLoop()
	// and reactively in nodeIsLiveCallback() on liveness updates.
	livenessMap atomic.Value

	// cachedCapacity caches information on store capacity to prevent
	// expensive recomputations in case leases or replicas are rapidly
	// rebalancing.
	cachedCapacity struct {
		syncutil.Mutex
		roachpb.StoreCapacity
	}

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

	computeInitialMetrics              sync.Once
	systemConfigUpdateQueueRateLimiter *quotapool.RateLimiter
	spanConfigUpdateQueueRateLimiter   *quotapool.RateLimiter
}

var _ kv.Sender = &Store{}

// A StoreConfig encompasses the auxiliary objects and configuration
// required to create a store.
// All fields holding a pointer or an interface are required to create
// a store; the rest will have sane defaults set if omitted.
type StoreConfig struct {
	AmbientCtx log.AmbientContext
	base.RaftConfig

	DefaultSpanConfig    roachpb.SpanConfig
	Settings             *cluster.Settings
	Clock                *hlc.Clock
	DB                   *kv.DB
	Gossip               *gossip.Gossip
	NodeLiveness         *liveness.NodeLiveness
	StorePool            *StorePool
	Transport            *RaftTransport
	NodeDialer           *nodedialer.Dialer
	RPCContext           *rpc.Context
	RangeDescriptorCache *rangecache.RangeCache

	ClosedTimestampSender   *sidetransport.Sender
	ClosedTimestampReceiver sidetransportReceiver

	// SQLExecutor is used by the store to execute SQL statements.
	SQLExecutor sqlutil.InternalExecutor

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

	// If LogRangeEvents is true, major changes to ranges will be logged into
	// the range event log.
	LogRangeEvents bool

	// RaftEntryCacheSize is the size in bytes of the Raft log entry cache
	// shared by all Raft groups managed by the store.
	RaftEntryCacheSize uint64

	// IntentResolverTaskLimit is the maximum number of asynchronous tasks that
	// may be started by the intent resolver. -1 indicates no asynchronous tasks
	// are allowed. 0 uses the default value (defaultIntentResolverTaskLimit)
	// which is non-zero.
	IntentResolverTaskLimit int

	TestingKnobs StoreTestingKnobs

	// concurrentSnapshotApplyLimit specifies the maximum number of empty
	// snapshots and the maximum number of non-empty snapshots that are permitted
	// to be applied concurrently.
	concurrentSnapshotApplyLimit int

	// HistogramWindowInterval is (server.Config).HistogramWindowInterval
	HistogramWindowInterval time.Duration

	// ExternalStorage creates ExternalStorage objects which allows access to external files
	ExternalStorage        cloud.ExternalStorageFactory
	ExternalStorageFromURI cloud.ExternalStorageFromURIFactory

	// ProtectedTimestampReader provides a read-only view into the protected
	// timestamp subsystem. It is queried during the GC process.
	ProtectedTimestampReader spanconfig.ProtectedTSReader

	// KV Memory Monitor. Must be non-nil for production, and can be nil in some
	// tests.
	KVMemoryMonitor        *mon.BytesMonitor
	RangefeedBudgetFactory *rangefeed.BudgetFactory

	// SpanConfigsDisabled determines whether we're able to use the span configs
	// infrastructure or not.
	SpanConfigsDisabled bool
	// Used to subscribe to span configuration changes, keeping up-to-date a
	// data structure useful for retrieving span configs. Only available if
	// SpanConfigsDisabled is unset.
	SpanConfigSubscriber spanconfig.KVSubscriber

	// KVAdmissionController is an optional field used for admission control.
	KVAdmissionController KVAdmissionController

	// SystemConfigProvider is used to drive replication decision-making in the
	// mixed-version state, before the span configuration infrastructure has been
	// bootstrapped.
	//
	// TODO(ajwerner): Remove in 22.2.
	SystemConfigProvider config.SystemConfigProvider
}

// ConsistencyTestingKnobs is a BatchEvalTestingKnobs struct used to control the
// behavior of the consistency checker for tests.
type ConsistencyTestingKnobs struct {
	// If non-nil, OnBadChecksumFatal is called by CheckConsistency() (instead of
	// calling log.Fatal) on a checksum mismatch.
	OnBadChecksumFatal func(roachpb.StoreIdent)
	// If non-nil, BadChecksumReportDiff is called by CheckConsistency() on a
	// checksum mismatch to report the diff between snapshots.
	BadChecksumReportDiff      func(roachpb.StoreIdent, ReplicaSnapshotDiffSlice)
	ConsistencyQueueResultHook func(response roachpb.CheckConsistencyResponse)
}

// Valid returns true if the StoreConfig is populated correctly.
// We don't check for Gossip and DB since some of our tests pass
// that as nil.
func (sc *StoreConfig) Valid() bool {
	return sc.Clock != nil && sc.Transport != nil &&
		sc.RaftTickInterval != 0 && sc.RaftHeartbeatIntervalTicks > 0 &&
		sc.RaftElectionTimeoutTicks > 0 && sc.ScanInterval >= 0 &&
		sc.AmbientCtx.Tracer != nil
}

// SetDefaults initializes unset fields in StoreConfig to values
// suitable for use on a local network.
// TODO(tschottdorf): see if this ought to be configurable via flags.
func (sc *StoreConfig) SetDefaults() {
	sc.RaftConfig.SetDefaults()

	if sc.CoalescedHeartbeatsInterval == 0 {
		sc.CoalescedHeartbeatsInterval = sc.RaftTickInterval / 2
	}
	if sc.RaftEntryCacheSize == 0 {
		sc.RaftEntryCacheSize = defaultRaftEntryCacheSize
	}
	if sc.concurrentSnapshotApplyLimit == 0 {
		// NB: setting this value higher than 1 is likely to degrade client
		// throughput.
		sc.concurrentSnapshotApplyLimit =
			envutil.EnvOrDefaultInt("COCKROACH_CONCURRENT_SNAPSHOT_APPLY_LIMIT", 1)
	}

	if sc.TestingKnobs.GossipWhenCapacityDeltaExceedsFraction == 0 {
		sc.TestingKnobs.GossipWhenCapacityDeltaExceedsFraction = defaultGossipWhenCapacityDeltaExceedsFraction
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
	return 2 * (sc.RangeLeaseActiveDuration() + maxOffset).Nanoseconds()
}

// NewStore returns a new instance of a store.
func NewStore(
	ctx context.Context, cfg StoreConfig, eng storage.Engine, nodeDesc *roachpb.NodeDescriptor,
) *Store {
	// TODO(tschottdorf): find better place to set these defaults.
	cfg.SetDefaults()

	if !cfg.Valid() {
		log.Fatalf(ctx, "invalid store configuration: %+v", &cfg)
	}
	s := &Store{
		cfg:      cfg,
		db:       cfg.DB, // TODO(tschottdorf): remove redundancy.
		engine:   eng,
		nodeDesc: nodeDesc,
		metrics:  newStoreMetrics(cfg.HistogramWindowInterval),
		ctSender: cfg.ClosedTimestampSender,
	}
	if cfg.RPCContext != nil {
		s.allocator = MakeAllocator(
			cfg.StorePool,
			cfg.RPCContext.RemoteClocks.Latency,
			cfg.TestingKnobs.AllocatorKnobs,
		)
	} else {
		s.allocator = MakeAllocator(
			cfg.StorePool, func(string) (time.Duration, bool) {
				return 0, false
			},
			cfg.TestingKnobs.AllocatorKnobs,
		)
	}
	s.replRankings = newReplicaRankings()

	s.draining.Store(false)
	s.scheduler = newRaftScheduler(cfg.AmbientCtx, s.metrics, s, storeSchedulerConcurrency)

	s.raftEntryCache = raftentry.NewCache(cfg.RaftEntryCacheSize)
	s.metrics.registry.AddMetricStruct(s.raftEntryCache.Metrics())

	s.coalescedMu.Lock()
	s.coalescedMu.heartbeats = map[roachpb.StoreIdent][]kvserverpb.RaftHeartbeat{}
	s.coalescedMu.heartbeatResponses = map[roachpb.StoreIdent][]kvserverpb.RaftHeartbeat{}
	s.coalescedMu.Unlock()

	s.mu.Lock()
	s.mu.replicaPlaceholders = map[roachpb.RangeID]*ReplicaPlaceholder{}
	s.mu.replicasByKey = newStoreReplicaBTree()
	s.mu.uninitReplicas = map[roachpb.RangeID]*Replica{}
	s.mu.Unlock()

	s.unquiescedReplicas.Lock()
	s.unquiescedReplicas.m = map[roachpb.RangeID]struct{}{}
	s.unquiescedReplicas.Unlock()

	s.rangefeedReplicas.Lock()
	s.rangefeedReplicas.m = map[roachpb.RangeID]struct{}{}
	s.rangefeedReplicas.Unlock()

	s.tsCache = tscache.New(cfg.Clock)
	s.metrics.registry.AddMetricStruct(s.tsCache.Metrics())

	s.txnWaitMetrics = txnwait.NewMetrics(cfg.HistogramWindowInterval)
	s.metrics.registry.AddMetricStruct(s.txnWaitMetrics)
	s.snapshotApplySem = make(chan struct{}, cfg.concurrentSnapshotApplyLimit)

	if ch := s.cfg.TestingKnobs.LeaseRenewalSignalChan; ch != nil {
		s.renewableLeasesSignal = ch
	} else {
		s.renewableLeasesSignal = make(chan struct{}, 1)
	}

	s.limiters.BulkIOWriteRate = rate.NewLimiter(rate.Limit(bulkIOWriteLimit.Get(&cfg.Settings.SV)), bulkIOWriteBurst)
	bulkIOWriteLimit.SetOnChange(&cfg.Settings.SV, func(ctx context.Context) {
		s.limiters.BulkIOWriteRate.SetLimit(rate.Limit(bulkIOWriteLimit.Get(&cfg.Settings.SV)))
	})
	s.limiters.ConcurrentExportRequests = limit.MakeConcurrentRequestLimiter(
		"exportRequestLimiter", int(ExportRequestsLimit.Get(&cfg.Settings.SV)),
	)
	s.limiters.ConcurrentScanInterleavedIntents = limit.MakeConcurrentRequestLimiter(
		"scanInterleavedIntentsLimiter", int(concurrentscanInterleavedIntentsLimit.Get(&cfg.Settings.SV)))
	concurrentscanInterleavedIntentsLimit.SetOnChange(&cfg.Settings.SV, func(ctx context.Context) {
		s.limiters.ConcurrentScanInterleavedIntents.SetLimit(int(concurrentscanInterleavedIntentsLimit.Get(&cfg.Settings.SV)))
	})

	// The snapshot storage is usually empty at this point since it is cleared
	// after each snapshot application, except when the node crashed right before
	// it can clean it up. If this fails it's not a correctness issue since the
	// storage is also cleared before receiving a snapshot.
	s.sstSnapshotStorage = NewSSTSnapshotStorage(s.engine, s.limiters.BulkIOWriteRate)
	if err := s.sstSnapshotStorage.Clear(); err != nil {
		log.Warningf(ctx, "failed to clear snapshot storage: %v", err)
	}
	s.protectedtsReader = cfg.ProtectedTimestampReader

	// On low-CPU instances, a default limit value may still allow ExportRequests
	// to tie up all cores so cap limiter at cores-1 when setting value is higher.
	exportCores := runtime.GOMAXPROCS(0) - 1
	if exportCores < 1 {
		exportCores = 1
	}
	ExportRequestsLimit.SetOnChange(&cfg.Settings.SV, func(ctx context.Context) {
		limit := int(ExportRequestsLimit.Get(&cfg.Settings.SV))
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

	s.tenantRateLimiters = tenantrate.NewLimiterFactory(&cfg.Settings.SV, &cfg.TestingKnobs.TenantRateKnobs)
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
		// Add range scanner and configure with queues.
		s.scanner = newReplicaScanner(
			s.cfg.AmbientCtx, s.cfg.Clock, cfg.ScanInterval,
			cfg.ScanMinIdleTime, cfg.ScanMaxIdleTime, newStoreReplicaVisitor(s),
		)
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
			s.raftLogQueue, s.raftSnapshotQueue, s.consistencyQueue)
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
		s.setGCQueueActive(false)
	}
	if cfg.TestingKnobs.DisableMergeQueue {
		s.setMergeQueueActive(false)
	}
	if cfg.TestingKnobs.DisableRaftLogQueue {
		s.setRaftLogQueueActive(false)
	}
	if cfg.TestingKnobs.DisableReplicaGCQueue {
		s.setReplicaGCQueueActive(false)
	}
	if cfg.TestingKnobs.DisableReplicateQueue {
		s.SetReplicateQueueActive(false)
	}
	if cfg.TestingKnobs.DisableSplitQueue {
		s.setSplitQueueActive(false)
	}
	if cfg.TestingKnobs.DisableTimeSeriesMaintenanceQueue {
		s.setTimeSeriesMaintenanceQueueActive(false)
	}
	if cfg.TestingKnobs.DisableRaftSnapshotQueue {
		s.setRaftSnapshotQueueActive(false)
	}
	if cfg.TestingKnobs.DisableConsistencyQueue {
		s.setConsistencyQueueActive(false)
	}
	if cfg.TestingKnobs.DisableScanner {
		s.setScannerActive(false)
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
						// (server.shutdown.lease_transfer_wait).
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

					// Learner replicas aren't allowed to become the leaseholder or raft
					// leader, so only consider the `Voters` replicas.
					needsLeaseTransfer := len(r.Desc().Replicas().VoterDescriptors()) > 1 &&
						drainingLeaseStatus.IsValid() &&
						drainingLeaseStatus.OwnedBy(s.StoreID())

					// Note that this code doesn't deal with transferring the Raft
					// leadership. Leadership tries to follow the lease, so when leases
					// are transferred, leadership will be transferred too. For ranges
					// without leases we probably should try to move the leadership
					// manually to a non-draining replica.

					if !needsLeaseTransfer {
						if verbose || log.V(1) {
							// This logging is useful to troubleshoot incomplete drains.
							log.Info(ctx, "not moving out")
						}
						atomic.AddInt32(&numTransfersAttempted, -1)
						return
					}

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
						transferLeaseOptions{},
					)
					duration := timeutil.Since(start).Microseconds()

					if transferStatus != transferOK {
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
	transferTimeout := leaseTransferWait.Get(&s.cfg.Settings.SV)

	drainLeasesOp := "transfer range leases"
	if err := contextutil.RunWithTimeout(ctx, drainLeasesOp, transferTimeout,
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
		if tErr := (*contextutil.TimeoutError)(nil); errors.As(err, &tErr) && tErr.Operation() == drainLeasesOp {
			// You expect this message when shutting down a server in an unhealthy
			// cluster, or when draining all nodes with replicas for some range at the
			// same time. If we see it on healthy ones, there's likely something to fix.
			log.Warningf(ctx, "unable to drain cleanly within %s (cluster setting %s), "+
				"service might briefly deteriorate if the node is terminated: %s",
				transferTimeout, leaseTransferWaitSettingName, tErr.Cause())
		} else {
			log.Warningf(ctx, "drain error: %+v", err)
		}
	}
}

// IsStarted returns true if the Store has been started.
func (s *Store) IsStarted() bool {
	return atomic.LoadInt32(&s.started) == 1
}

// IterateIDPrefixKeys helps visit system keys that use RangeID prefixing (such
// as RaftHardStateKey, RangeTombstoneKey, and many others). Such keys could in
// principle exist at any RangeID, and this helper efficiently discovers all the
// keys of the desired type (as specified by the supplied `keyFn`) and, for each
// key-value pair discovered, unmarshals it into `msg` and then invokes `f`.
//
// Iteration stops on the first error (and will pass through that error).
func IterateIDPrefixKeys(
	ctx context.Context,
	reader storage.Reader,
	keyFn func(roachpb.RangeID) roachpb.Key,
	msg protoutil.Message,
	f func(_ roachpb.RangeID) error,
) error {
	rangeID := roachpb.RangeID(1)
	// NB: Range-ID local keys have no versions and no intents.
	iter := reader.NewMVCCIterator(storage.MVCCKeyIterKind, storage.IterOptions{
		UpperBound: keys.LocalRangeIDPrefix.PrefixEnd().AsRawKey(),
	})
	defer iter.Close()

	for {
		bumped := false
		mvccKey := storage.MakeMVCCMetadataKey(keyFn(rangeID))
		iter.SeekGE(mvccKey)

		if ok, err := iter.Valid(); !ok {
			return err
		}

		unsafeKey := iter.UnsafeKey()

		if !bytes.HasPrefix(unsafeKey.Key, keys.LocalRangeIDPrefix) {
			// Left the local keyspace, so we're done.
			return nil
		}

		curRangeID, _, _, _, err := keys.DecodeRangeIDKey(unsafeKey.Key)
		if err != nil {
			return err
		}

		if curRangeID > rangeID {
			// `bumped` is always `false` here, but let's be explicit.
			if !bumped {
				rangeID = curRangeID
				bumped = true
			}
			mvccKey = storage.MakeMVCCMetadataKey(keyFn(rangeID))
		}

		if !unsafeKey.Key.Equal(mvccKey.Key) {
			if !bumped {
				// Don't increment the rangeID if it has already been incremented
				// above, or we could skip past a value we ought to see.
				rangeID++
				bumped = true // for completeness' sake; continuing below anyway
			}
			continue
		}

		ok, err := storage.MVCCGetProto(
			ctx, reader, unsafeKey.Key, hlc.Timestamp{}, msg, storage.MVCCGetOptions{})
		if err != nil {
			return err
		}
		if !ok {
			return errors.Errorf("unable to unmarshal %s into %T", unsafeKey.Key, msg)
		}

		if err := f(rangeID); err != nil {
			if iterutil.Done(err) {
				return nil
			}
			return err
		}
		rangeID++
	}
}

// IterateRangeDescriptorsFromDisk calls the provided function with each
// descriptor from the provided Engine. The return values of this method and fn
// have semantics similar to engine.MVCCIterate.
func IterateRangeDescriptorsFromDisk(
	ctx context.Context, reader storage.Reader, fn func(desc roachpb.RangeDescriptor) error,
) error {
	log.Event(ctx, "beginning range descriptor iteration")
	// MVCCIterator over all range-local key-based data.
	start := keys.RangeDescriptorKey(roachpb.RKeyMin)
	end := keys.RangeDescriptorKey(roachpb.RKeyMax)

	allCount := 0
	matchCount := 0
	bySuffix := make(map[string]int)
	kvToDesc := func(kv roachpb.KeyValue) error {
		allCount++
		// Only consider range metadata entries; ignore others.
		_, suffix, _, err := keys.DecodeRangeKey(kv.Key)
		if err != nil {
			return err
		}
		bySuffix[string(suffix)]++
		if !bytes.Equal(suffix, keys.LocalRangeDescriptorSuffix) {
			return nil
		}
		var desc roachpb.RangeDescriptor
		if err := kv.Value.GetProto(&desc); err != nil {
			return err
		}
		matchCount++
		if err := fn(desc); iterutil.Done(err) {
			return iterutil.StopIteration()
		}
		return err
	}

	_, err := storage.MVCCIterate(ctx, reader, start, end, hlc.MaxTimestamp,
		storage.MVCCScanOptions{Inconsistent: true}, kvToDesc)
	log.Eventf(ctx, "iterated over %d keys to find %d range descriptors (by suffix: %v)",
		allCount, matchCount, bySuffix)
	return err
}

// ReadStoreIdent reads the StoreIdent from the store.
// It returns *NotBootstrappedError if the ident is missing (meaning that the
// store needs to be bootstrapped).
func ReadStoreIdent(ctx context.Context, eng storage.Engine) (roachpb.StoreIdent, error) {
	var ident roachpb.StoreIdent
	ok, err := storage.MVCCGetProto(
		ctx, eng, keys.StoreIdentKey(), hlc.Timestamp{}, &ident, storage.MVCCGetOptions{})
	if err != nil {
		return roachpb.StoreIdent{}, err
	} else if !ok {
		return roachpb.StoreIdent{}, &NotBootstrappedError{}
	}
	return ident, err
}

// Start the engine, set the GC and read the StoreIdent.
func (s *Store) Start(ctx context.Context, stopper *stop.Stopper) error {
	s.stopper = stopper

	// Populate the store ident. If not bootstrapped, ReadStoreIntent will
	// return an error.
	ident, err := ReadStoreIdent(ctx, s.engine)
	if err != nil {
		return err
	}
	s.Ident = &ident

	// Set the store ID for logging.
	s.cfg.AmbientCtx.AddLogTag("s", s.StoreID())
	ctx = s.AnnotateCtx(ctx)
	log.Event(ctx, "read store identity")

	// Communicate store ID to engine, if it needs it.
	if logSetter, ok := s.engine.(storage.StoreIDSetter); ok {
		logSetter.SetStoreID(ctx, int32(s.StoreID()))
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
		s.engine.RegisterFlushCompletedCallback(func() {
			truncator.durabilityAdvancedCallback()
		})
	}

	// Create the recovery manager.
	s.recoveryMgr = txnrecovery.NewManager(
		s.cfg.AmbientCtx, s.cfg.Clock, s.db, stopper,
	)
	s.metrics.registry.AddMetricStruct(s.recoveryMgr.Metrics())

	s.rangeIDAlloc = idAlloc

	now := s.cfg.Clock.Now()
	s.startedAt = now.WallTime

	// Iterate over all range descriptors, ignoring uncommitted versions
	// (consistent=false). Uncommitted intents which have been abandoned
	// due to a split crashing halfway will simply be resolved on the
	// next split attempt. They can otherwise be ignored.

	// TODO(peter): While we have to iterate to find the replica descriptors
	// serially, we can perform the migrations and replica creation
	// concurrently. Note that while we can perform this initialization
	// concurrently, all of the initialization must be performed before we start
	// listening for Raft messages and starting the process Raft loop.
	err = IterateRangeDescriptorsFromDisk(ctx, s.engine,
		func(desc roachpb.RangeDescriptor) error {
			if !desc.IsInitialized() {
				return errors.Errorf("found uninitialized RangeDescriptor: %+v", desc)
			}
			replicaDesc, found := desc.GetReplicaDescriptor(s.StoreID())
			if !found {
				// This is a pre-emptive snapshot. It's also possible that this is a
				// range which has processed a raft command to remove itself (which is
				// possible prior to 19.2 or if the DisableEagerReplicaRemoval is
				// enabled) and has not yet been removed by the replica gc queue.
				// We treat both cases the same way. These should no longer exist in
				// 20.2 or after as there was a migration in 20.1 to remove them and
				// no pre-emptive snapshot should have been sent since 19.2 was
				// finalized.
				return errors.AssertionFailedf(
					"found RangeDescriptor for range %d at generation %d which does not"+
						" contain this store %d",
					redact.Safe(desc.RangeID),
					redact.Safe(desc.Generation),
					redact.Safe(s.StoreID()))
			}

			rep, err := newReplica(ctx, &desc, s, replicaDesc.ReplicaID)
			if err != nil {
				return err
			}

			// We can't lock s.mu across NewReplica due to the lock ordering
			// constraint (*Replica).raftMu < (*Store).mu. See the comment on
			// (Store).mu.
			s.mu.Lock()
			err = s.addReplicaInternalLocked(rep)
			s.mu.Unlock()
			if err != nil {
				return err
			}

			// Add this range and its stats to our counter.
			s.metrics.ReplicaCount.Inc(1)
			if _, ok := rep.TenantID(); ok {
				// TODO(tbg): why the check? We're definitely an initialized range so
				// we have a tenantID.
				s.metrics.addMVCCStats(ctx, rep.tenantMetricsRef, rep.GetMVCCStats())
			} else {
				return errors.AssertionFailedf("found newly constructed replica"+
					" for range %d at generation %d with an invalid tenant ID in store %d",
					redact.Safe(desc.RangeID),
					redact.Safe(desc.Generation),
					redact.Safe(s.StoreID()))
			}

			if _, ok := desc.GetReplicaDescriptor(s.StoreID()); !ok {
				// We are no longer a member of the range, but we didn't GC the replica
				// before shutting down. Add the replica to the GC queue.
				s.replicaGCQueue.AddAsync(ctx, rep, replicaGCPriorityRemoved)
			}

			// Note that we do not create raft groups at this time; they will be created
			// on-demand the first time they are needed. This helps reduce the amount of
			// election-related traffic in a cold start.
			// Raft initialization occurs when we propose a command on this range or
			// receive a raft message addressed to it.
			// TODO(bdarnell): Also initialize raft groups when read leases are needed.
			// TODO(bdarnell): Scan all ranges at startup for unapplied log entries
			// and initialize those groups.
			return nil
		})
	if err != nil {
		return err
	}

	// Start Raft processing goroutines.
	s.cfg.Transport.Listen(s.StoreID(), s)
	s.processRaft(ctx)

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

		// When toggling between the system config span and the span
		// configs infrastructure, we want to re-apply configs on all
		// replicas from whatever the new source is.
		spanconfigstore.EnabledSetting.SetOnChange(&s.ClusterSettings().SV, func(ctx context.Context) {
			enabled := spanconfigstore.EnabledSetting.Get(&s.ClusterSettings().SV)
			if enabled {
				s.applyAllFromSpanConfigStore(ctx)
			} else if scp := s.cfg.SystemConfigProvider; scp != nil {
				if sc := scp.GetSystemConfig(); sc != nil {
					s.systemGossipUpdate(sc)
				}
			}
		})
	}

	if !s.cfg.TestingKnobs.DisableAutomaticLeaseRenewal {
		s.startLeaseRenewer(ctx)
	}

	// Connect rangefeeds to closed timestamp updates.
	s.startRangefeedUpdater(ctx)

	if s.replicateQueue != nil {
		s.storeRebalancer = NewStoreRebalancer(
			s.cfg.AmbientCtx, s.cfg.Settings, s.replicateQueue, s.replRankings)
		s.storeRebalancer.Start(ctx, s.stopper)
	}

	s.consistencyLimiter = quotapool.NewRateLimiter(
		"ConsistencyQueue",
		quotapool.Limit(consistencyCheckRate.Get(&s.ClusterSettings().SV)),
		consistencyCheckRate.Get(&s.ClusterSettings().SV)*consistencyCheckRateBurstFactor,
		quotapool.WithMinimumWait(consistencyCheckRateMinWait))

	consistencyCheckRate.SetOnChange(&s.ClusterSettings().SV, func(ctx context.Context) {
		rate := consistencyCheckRate.Get(&s.ClusterSettings().SV)
		s.consistencyLimiter.UpdateLimit(quotapool.Limit(rate), rate*consistencyCheckRateBurstFactor)
	})

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

var errPeriodicGossipsDisabled = errors.New("periodic gossip is disabled")

// startGossip runs an infinite loop in a goroutine which regularly checks
// whether the store has a first range or config replica and asks those ranges
// to gossip accordingly.
func (s *Store) startGossip() {
	wakeReplica := func(ctx context.Context, repl *Replica) error {
		// Acquire the range lease, which in turn triggers system data gossip
		// functions (e.g. MaybeGossipSystemConfig or MaybeGossipNodeLiveness).
		_, pErr := repl.getLeaseForGossip(ctx)
		return pErr.GoError()
	}
	gossipFns := []struct {
		key         roachpb.Key
		fn          func(context.Context, *Replica) error
		description redact.SafeString
		interval    time.Duration
	}{
		{
			key: roachpb.KeyMin,
			fn: func(ctx context.Context, repl *Replica) error {
				// The first range is gossiped by all replicas, not just the lease
				// holder, so wakeReplica is not used here.
				return repl.maybeGossipFirstRange(ctx).GoError()
			},
			description: "first range descriptor",
			interval:    s.cfg.SentinelGossipTTL() / 2,
		},
		{
			key:         keys.SystemConfigSpan.Key,
			fn:          wakeReplica,
			description: "system config",
			interval:    systemDataGossipInterval,
		},
		{
			key:         keys.NodeLivenessSpan.Key,
			fn:          wakeReplica,
			description: "node liveness",
			interval:    systemDataGossipInterval,
		},
	}

	cannotGossipEvery := log.Every(time.Minute)
	cannotGossipEvery.ShouldLog() // only log next time after waiting out the delay

	// Periodic updates run in a goroutine and signal a WaitGroup upon completion
	// of their first iteration.
	s.initComplete.Add(len(gossipFns))
	for _, gossipFn := range gossipFns {
		gossipFn := gossipFn // per-iteration copy
		bgCtx := s.AnnotateCtx(context.Background())
		if err := s.stopper.RunAsyncTask(bgCtx, "store-gossip", func(ctx context.Context) {
			ticker := time.NewTicker(gossipFn.interval)
			defer ticker.Stop()
			for first := true; ; {
				// Retry in a backoff loop until gossipFn succeeds. The gossipFn might
				// temporarily fail (e.g. because node liveness hasn't initialized yet
				// making it impossible to get an epoch-based range lease), in which
				// case we want to retry quickly.
				retryOptions := base.DefaultRetryOptions()
				retryOptions.Closer = s.stopper.ShouldQuiesce()
				for r := retry.Start(retryOptions); r.Next(); {
					if repl := s.LookupReplica(roachpb.RKey(gossipFn.key)); repl != nil {
						annotatedCtx := repl.AnnotateCtx(ctx)
						if err := gossipFn.fn(annotatedCtx, repl); err != nil {
							if cannotGossipEvery.ShouldLog() {
								log.Infof(annotatedCtx, "could not gossip %s: %v", gossipFn.description, err)
							}
							if !errors.Is(err, errPeriodicGossipsDisabled) {
								continue
							}
						}
					}
					break
				}
				if first {
					first = false
					s.initComplete.Done()
				}
				select {
				case <-ticker.C:
				case <-s.stopper.ShouldQuiesce():
					return
				}
			}
		}); err != nil {
			s.initComplete.Done()
		}
	}
}

var errSysCfgUnavailable = errors.New("system config not available in gossip")

// GetConfReader exposes access to a configuration reader.
func (s *Store) GetConfReader(ctx context.Context) (spanconfig.StoreReader, error) {
	if s.cfg.TestingKnobs.MakeSystemConfigSpanUnavailableToQueues {
		return nil, errSysCfgUnavailable
	}

	// We need a version gate here before switching over to the span configs
	// infrastructure. In a mixed-version cluster we need to wait for
	// the host tenant to have fully populated `system.span_configurations`
	// (read: reconciled) at least once before using it as a view for all
	// split/config decisions.
	_ = clusterversion.EnsureSpanConfigReconciliation
	//
	// We also want to ensure that the KVSubscriber on each store is at least as
	// up-to-date as some full reconciliation timestamp.
	_ = clusterversion.EnsureSpanConfigSubscription
	//
	// Without a version gate, it would be possible for a replica on a
	// new-binary-server to apply the static fallback config (assuming no
	// entries in `system.span_configurations`), in violation of explicit
	// configs directly set by the user. Though unlikely, it's also possible for
	// us to merge all ranges into a single one -- with no entries in
	// system.span_configurations, the infrastructure can erroneously conclude
	// that there are zero split points.
	//
	// We achieve all this through a three-step migration process, culminating
	// in the following cluster version gate:
	_ = clusterversion.EnableSpanConfigStore

	if s.cfg.SpanConfigsDisabled ||
		!spanconfigstore.EnabledSetting.Get(&s.ClusterSettings().SV) ||
		!s.cfg.Settings.Version.IsActive(ctx, clusterversion.EnableSpanConfigStore) ||
		s.TestingKnobs().UseSystemConfigSpanForQueues {

		sysCfg := s.cfg.SystemConfigProvider.GetSystemConfig()
		if sysCfg == nil {
			return nil, errSysCfgUnavailable
		}
		return sysCfg, nil
	}

	return s.cfg.SpanConfigSubscriber, nil
}

// startLeaseRenewer runs an infinite loop in a goroutine which regularly
// checks whether the store has any expiration-based leases that should be
// proactively renewed and attempts to continue renewing them.
//
// This reduces user-visible latency when range lookups are needed to serve a
// request and reduces ping-ponging of r1's lease to different replicas as
// maybeGossipFirstRange is called on each (e.g.  #24753).
func (s *Store) startLeaseRenewer(ctx context.Context) {
	// Start a goroutine that watches and proactively renews certain
	// expiration-based leases.
	_ = s.stopper.RunAsyncTask(ctx, "lease-renewer", func(ctx context.Context) {
		timer := timeutil.NewTimer()
		defer timer.Stop()

		// Determine how frequently to attempt to ensure that we have each lease.
		// The divisor used here is somewhat arbitrary, but needs to be large
		// enough to ensure we'll attempt to renew the lease reasonably early
		// within the RangeLeaseRenewalDuration time window. This means we'll wake
		// up more often that strictly necessary, but it's more maintainable than
		// attempting to accurately determine exactly when each iteration of a
		// lease expires and when we should attempt to renew it as a result.
		renewalDuration := s.cfg.RangeLeaseActiveDuration() / 5
		if d := s.cfg.TestingKnobs.LeaseRenewalDurationOverride; d > 0 {
			renewalDuration = d
		}
		for {
			var numRenewableLeases int
			s.renewableLeases.Range(func(k int64, v unsafe.Pointer) bool {
				numRenewableLeases++
				repl := (*Replica)(v)
				annotatedCtx := repl.AnnotateCtx(ctx)
				if _, pErr := repl.redirectOnOrAcquireLease(annotatedCtx); pErr != nil {
					if _, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError); !ok {
						log.Warningf(annotatedCtx, "failed to proactively renew lease: %s", pErr)
					}
					s.renewableLeases.Delete(k)
				}
				return true
			})

			if numRenewableLeases > 0 {
				timer.Reset(renewalDuration)
			}
			if fn := s.cfg.TestingKnobs.LeaseRenewalOnPostCycle; fn != nil {
				fn()
			}
			select {
			case <-s.renewableLeasesSignal:
			case <-timer.C:
				timer.Read = true
			case <-s.stopper.ShouldQuiesce():
				return
			}
		}
	})
}

// startRangefeedUpdater periodically informs all the replicas with rangefeeds
// about closed timestamp updates.
func (s *Store) startRangefeedUpdater(ctx context.Context) {
	_ /* err */ = s.stopper.RunAsyncTaskEx(ctx,
		stop.TaskOpts{
			TaskName: "closedts-rangefeed-updater",
			SpanOpt:  stop.SterileRootSpan,
		}, func(ctx context.Context) {
			timer := timeutil.NewTimer()
			defer timer.Stop()
			var replIDs []roachpb.RangeID
			st := s.cfg.Settings

			confCh := make(chan struct{}, 1)
			confChanged := func(ctx context.Context) {
				select {
				case confCh <- struct{}{}:
				default:
				}
			}
			closedts.SideTransportCloseInterval.SetOnChange(&st.SV, confChanged)
			RangeFeedRefreshInterval.SetOnChange(&st.SV, confChanged)

			getInterval := func() time.Duration {
				refresh := RangeFeedRefreshInterval.Get(&st.SV)
				if refresh != 0 {
					return refresh
				}
				return closedts.SideTransportCloseInterval.Get(&st.SV)
			}

			for {
				interval := getInterval()
				if interval > 0 {
					timer.Reset(interval)
				} else {
					// Disable the side-transport.
					timer.Stop()
					timer = timeutil.NewTimer()
				}
				select {
				case <-timer.C:
					timer.Read = true
					s.rangefeedReplicas.Lock()
					replIDs = replIDs[:0]
					for replID := range s.rangefeedReplicas.m {
						replIDs = append(replIDs, replID)
					}
					s.rangefeedReplicas.Unlock()
					// Notify each replica with an active rangefeed to check for an updated
					// closed timestamp.
					for _, replID := range replIDs {
						r := s.GetReplicaIfExists(replID)
						if r == nil {
							continue
						}
						r.handleClosedTimestampUpdate(ctx, r.GetClosedTimestamp(ctx))
					}
				case <-confCh:
					// Loop around to use the updated timer.
					continue
				case <-s.stopper.ShouldQuiesce():
					return
				}

			}
		})
}

func (s *Store) addReplicaWithRangefeed(rangeID roachpb.RangeID) {
	s.rangefeedReplicas.Lock()
	s.rangefeedReplicas.m[rangeID] = struct{}{}
	s.rangefeedReplicas.Unlock()
}

func (s *Store) removeReplicaWithRangefeed(rangeID roachpb.RangeID) {
	s.rangefeedReplicas.Lock()
	delete(s.rangefeedReplicas.m, rangeID)
	s.rangefeedReplicas.Unlock()
}

// systemGossipUpdate is a callback for gossip updates to
// the system config which affect range split boundaries.
func (s *Store) systemGossipUpdate(sysCfg *config.SystemConfig) {
	if !s.cfg.SpanConfigsDisabled && spanconfigstore.EnabledSetting.Get(&s.ClusterSettings().SV) {
		return // nothing to do
	}

	ctx := s.AnnotateCtx(context.Background())
	s.computeInitialMetrics.Do(func() {
		// Metrics depend in part on the system config. Compute them as soon as we
		// get the first system config, then periodically in the background
		// (managed by the Node).
		if err := s.ComputeMetrics(ctx, -1); err != nil {
			log.Infof(ctx, "%s: failed initial metrics computation: %s", s, err)
		}
		log.Event(ctx, "computed initial metrics")
	})

	// We'll want to offer all replicas to the split and merge queues. Be a little
	// careful about not spawning too many individual goroutines.
	shouldQueue := s.systemConfigUpdateQueueRateLimiter.AdmitN(1)

	// For every range, update its zone config and check if it needs to
	// be split or merged.
	now := s.cfg.Clock.NowAsClockTimestamp()
	newStoreReplicaVisitor(s).Visit(func(repl *Replica) bool {
		key := repl.Desc().StartKey
		conf, err := sysCfg.GetSpanConfigForKey(ctx, key)
		if err != nil {
			if log.V(1) {
				log.Infof(context.TODO(), "failed to get span config for key %s", key)
			}
			conf = s.cfg.DefaultSpanConfig
		}

		if s.cfg.SpanConfigsDisabled ||
			!spanconfigstore.EnabledSetting.Get(&s.ClusterSettings().SV) ||
			!s.cfg.Settings.Version.IsActive(ctx, clusterversion.EnableSpanConfigStore) {
			repl.SetSpanConfig(conf)
		}

		if shouldQueue {
			s.splitQueue.Async(ctx, "gossip update", true /* wait */, func(ctx context.Context, h queueHelper) {
				h.MaybeAdd(ctx, repl, now)
			})
			s.mergeQueue.Async(ctx, "gossip update", true /* wait */, func(ctx context.Context, h queueHelper) {
				h.MaybeAdd(ctx, repl, now)
			})
		}
		return true // more
	})
}

// onSpanConfigUpdate is the callback invoked whenever this store learns of a
// span config update.
func (s *Store) onSpanConfigUpdate(ctx context.Context, updated roachpb.Span) {
	if !spanconfigstore.EnabledSetting.Get(&s.ClusterSettings().SV) {
		return
	}

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

				// TODO(irfansharif): It's possible for a config to be applied over an
				// entire range when it only pertains to the first half of the range.
				// This will be corrected shortly -- we enqueue the range for a split
				// below where we then apply the right config on each half. But still,
				// it's surprising behavior and gets in the way of a desirable
				// consistency guarantee: a key's config at any point in time is one
				// that was explicitly declared over it, or the default config.
				//
				// We can do better, we can skip applying the config entirely and
				// enqueue the split, then relying on the split trigger to install
				// the right configs on each half. The current structure is as it is
				// to maintain parity with the system config span variant.

				conf, err := s.cfg.SpanConfigSubscriber.GetSpanConfigForKey(replCtx, startKey)
				if err != nil {
					log.Errorf(ctx, "skipped applying update, unexpected error reading from subscriber: %v", err)
					return err
				}
				repl.SetSpanConfig(conf)
			}

			// TODO(irfansharif): For symmetry with the system config span variant,
			// we queue blindly; we could instead only queue it if we knew the
			// range's keyspans has a split in there somewhere, or was now part of a
			// larger range and eligible for a merge.
			s.splitQueue.Async(replCtx, "span config update", true /* wait */, func(ctx context.Context, h queueHelper) {
				h.MaybeAdd(ctx, repl, now)
			})
			s.mergeQueue.Async(replCtx, "span config update", true /* wait */, func(ctx context.Context, h queueHelper) {
				h.MaybeAdd(ctx, repl, now)
			})
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
		conf, err := s.cfg.SpanConfigSubscriber.GetSpanConfigForKey(replCtx, key)
		if err != nil {
			log.Errorf(ctx, "skipped applying config update, unexpected error reading from subscriber: %v", err)
			return true // more
		}

		repl.SetSpanConfig(conf)
		s.splitQueue.Async(replCtx, "span config update", true /* wait */, func(ctx context.Context, h queueHelper) {
			h.MaybeAdd(ctx, repl, now)
		})
		s.mergeQueue.Async(replCtx, "span config update", true /* wait */, func(ctx context.Context, h queueHelper) {
			h.MaybeAdd(ctx, repl, now)
		})
		return true // more
	})
}

func (s *Store) asyncGossipStore(ctx context.Context, reason string, useCached bool) {
	if err := s.stopper.RunAsyncTask(
		ctx, fmt.Sprintf("storage.Store: gossip on %s", reason),
		func(ctx context.Context) {
			if err := s.GossipStore(ctx, useCached); err != nil {
				log.Warningf(ctx, "error gossiping on %s: %+v", reason, err)
			}
		}); err != nil {
		log.Warningf(ctx, "unable to gossip on %s: %+v", reason, err)
	}
}

// GossipStore broadcasts the store on the gossip network.
func (s *Store) GossipStore(ctx context.Context, useCached bool) error {
	// Temporarily indicate that we're gossiping the store capacity to avoid
	// recursively triggering a gossip of the store capacity.
	syncutil.StoreFloat64(&s.gossipQueriesPerSecondVal, -1)
	syncutil.StoreFloat64(&s.gossipWritesPerSecondVal, -1)

	storeDesc, err := s.Descriptor(ctx, useCached)
	if err != nil {
		return errors.Wrapf(err, "problem getting store descriptor for store %+v", s.Ident)
	}

	// Set countdown target for re-gossiping capacity earlier than
	// the usual periodic interval. Re-gossip more rapidly for RangeCount
	// changes because allocators with stale information are much more
	// likely to make bad decisions.
	rangeCountdown := float64(storeDesc.Capacity.RangeCount) * s.cfg.TestingKnobs.GossipWhenCapacityDeltaExceedsFraction
	atomic.StoreInt32(&s.gossipRangeCountdown, int32(math.Ceil(math.Min(rangeCountdown, 3))))
	leaseCountdown := float64(storeDesc.Capacity.LeaseCount) * s.cfg.TestingKnobs.GossipWhenCapacityDeltaExceedsFraction
	atomic.StoreInt32(&s.gossipLeaseCountdown, int32(math.Ceil(math.Max(leaseCountdown, 1))))
	syncutil.StoreFloat64(&s.gossipQueriesPerSecondVal, storeDesc.Capacity.QueriesPerSecond)
	syncutil.StoreFloat64(&s.gossipWritesPerSecondVal, storeDesc.Capacity.WritesPerSecond)

	// Unique gossip key per store.
	gossipStoreKey := gossip.MakeStoreKey(storeDesc.StoreID)
	// Gossip store descriptor.
	return s.cfg.Gossip.AddInfoProto(gossipStoreKey, storeDesc, gossip.StoreTTL)
}

type capacityChangeEvent int

const (
	rangeAddEvent capacityChangeEvent = iota
	rangeRemoveEvent
	leaseAddEvent
	leaseRemoveEvent
)

// maybeGossipOnCapacityChange decrements the countdown on range
// and leaseholder counts. If it reaches 0, then we trigger an
// immediate gossip of this store's descriptor, to include updated
// capacity information.
func (s *Store) maybeGossipOnCapacityChange(ctx context.Context, cce capacityChangeEvent) {
	if s.cfg.TestingKnobs.DisableLeaseCapacityGossip && (cce == leaseAddEvent || cce == leaseRemoveEvent) {
		return
	}

	// Incrementally adjust stats to keep them up to date even if the
	// capacity is gossiped, but isn't due yet to be recomputed from scratch.
	s.cachedCapacity.Lock()
	switch cce {
	case rangeAddEvent:
		s.cachedCapacity.RangeCount++
	case rangeRemoveEvent:
		s.cachedCapacity.RangeCount--
	case leaseAddEvent:
		s.cachedCapacity.LeaseCount++
	case leaseRemoveEvent:
		s.cachedCapacity.LeaseCount--
	}
	s.cachedCapacity.Unlock()

	if ((cce == rangeAddEvent || cce == rangeRemoveEvent) && atomic.AddInt32(&s.gossipRangeCountdown, -1) == 0) ||
		((cce == leaseAddEvent || cce == leaseRemoveEvent) && atomic.AddInt32(&s.gossipLeaseCountdown, -1) == 0) {
		// Reset countdowns to avoid unnecessary gossiping.
		atomic.StoreInt32(&s.gossipRangeCountdown, 0)
		atomic.StoreInt32(&s.gossipLeaseCountdown, 0)
		s.asyncGossipStore(ctx, "capacity change", true /* useCached */)
	}
}

// recordNewPerSecondStats takes recently calculated values for the number of
// queries and key writes the store is handling and decides whether either has
// changed enough to justify re-gossiping the store's capacity.
func (s *Store) recordNewPerSecondStats(newQPS, newWPS float64) {
	oldQPS := syncutil.LoadFloat64(&s.gossipQueriesPerSecondVal)
	oldWPS := syncutil.LoadFloat64(&s.gossipWritesPerSecondVal)
	if oldQPS == -1 || oldWPS == -1 {
		// Gossiping of store capacity is already ongoing.
		return
	}

	const minAbsoluteChange = 100
	updateForQPS := (newQPS < oldQPS*.5 || newQPS > oldQPS*1.5) && math.Abs(newQPS-oldQPS) > minAbsoluteChange
	updateForWPS := (newWPS < oldWPS*.5 || newWPS > oldWPS*1.5) && math.Abs(newWPS-oldWPS) > minAbsoluteChange

	if !updateForQPS && !updateForWPS {
		return
	}

	var message string
	if updateForQPS && updateForWPS {
		message = "queries-per-second and writes-per-second change"
	} else if updateForQPS {
		message = "queries-per-second change"
	} else {
		message = "writes-per-second change"
	}
	// TODO(a-robinson): Use the provided values to avoid having to recalculate
	// them in GossipStore.
	s.asyncGossipStore(context.TODO(), message, false /* useCached */)
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
		s.engine,
		nil,
		keys.StoreLastUpKey(),
		hlc.Timestamp{},
		nil,
		&time,
	)
}

// ReadLastUpTimestamp returns the "last up" timestamp recorded in this store.
// This value can be used to approximate the last time the engine was was being
// served as a store by a running node. If the store does not contain a "last
// up" timestamp (for example, on a newly bootstrapped store), the zero
// timestamp is returned instead.
func (s *Store) ReadLastUpTimestamp(ctx context.Context) (hlc.Timestamp, error) {
	var timestamp hlc.Timestamp
	ok, err := storage.MVCCGetProto(ctx, s.Engine(), keys.StoreLastUpKey(), hlc.Timestamp{},
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
	batch := s.Engine().NewBatch()
	// Write has to sync to disk to ensure HLC monotonicity across restarts
	defer batch.Close()
	if err := storage.MVCCPutProto(
		ctx,
		batch,
		nil,
		keys.StoreHLCUpperBoundKey(),
		hlc.Timestamp{},
		nil,
		&ts,
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

// checkCanInitializeEngine ensures that the engine is empty except for a
// cluster version, which must be present.
func checkCanInitializeEngine(ctx context.Context, eng storage.Engine) error {
	// See if this is an already-bootstrapped store.
	ident, err := ReadStoreIdent(ctx, eng)
	if err == nil {
		return errors.Errorf("engine already initialized as %s", ident.String())
	} else if !errors.HasType(err, (*NotBootstrappedError)(nil)) {
		return errors.Wrap(err, "unable to read store ident")
	}
	// Engine is not bootstrapped yet (i.e. no StoreIdent). Does it contain a
	// cluster version, cached settings and nothing else? Note that there is one
	// cluster version key and many cached settings key, and the cluster version
	// key precedes the cached settings.
	//
	// We use an EngineIterator to ensure that there are no keys that cannot be
	// parsed as MVCCKeys (e.g. lock table keys) in the engine.
	iter := eng.NewEngineIterator(storage.IterOptions{UpperBound: roachpb.KeyMax})
	defer iter.Close()
	valid, err := iter.SeekEngineKeyGE(storage.EngineKey{Key: roachpb.KeyMin})
	if !valid {
		if err == nil {
			return errors.New("no cluster version found on uninitialized engine")
		}
		return err
	}
	getMVCCKey := func() (storage.MVCCKey, error) {
		var k storage.EngineKey
		k, err = iter.EngineKey()
		if err != nil {
			return storage.MVCCKey{}, err
		}
		if !k.IsMVCCKey() {
			return storage.MVCCKey{}, errors.Errorf("found non-mvcc key: %s", k)
		}
		return k.ToMVCCKey()
	}
	var k storage.MVCCKey
	if k, err = getMVCCKey(); err != nil {
		return err
	}
	if !k.Key.Equal(keys.StoreClusterVersionKey()) {
		return errors.New("no cluster version found on uninitialized engine")
	}
	valid, err = iter.NextEngineKey()
	for valid {
		// Only allowed to find cached cluster settings on an uninitialized
		// engine.
		if k, err = getMVCCKey(); err != nil {
			return err
		}
		if _, err := keys.DecodeStoreCachedSettingsKey(k.Key); err != nil {
			return errors.Errorf("engine cannot be bootstrapped, contains key:\n%s", k.String())
		}
		// There may be more cached cluster settings, so continue iterating.
		valid, err = iter.NextEngineKey()
	}
	return err
}

// GetReplica fetches a replica by Range ID. Returns an error if no replica is found.
//
// See also GetReplicaIfExists for a more perfomant version.
func (s *Store) GetReplica(rangeID roachpb.RangeID) (*Replica, error) {
	if r := s.GetReplicaIfExists(rangeID); r != nil {
		return r, nil
	}
	return nil, roachpb.NewRangeNotFoundError(rangeID, s.StoreID())
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

// Engine accessor.
func (s *Store) Engine() storage.Engine { return s.engine }

// DB accessor.
func (s *Store) DB() *kv.DB { return s.cfg.DB }

// Gossip accessor.
func (s *Store) Gossip() *gossip.Gossip { return s.cfg.Gossip }

// Stopper accessor.
func (s *Store) Stopper() *stop.Stopper { return s.stopper }

// TestingKnobs accessor.
func (s *Store) TestingKnobs() *StoreTestingKnobs { return &s.cfg.TestingKnobs }

// IsDraining accessor.
func (s *Store) IsDraining() bool {
	return s.draining.Load().(bool)
}

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
	return s.engine.Attrs()
}

// Properties returns the properties of the underlying store.
func (s *Store) Properties() roachpb.StoreProperties {
	return s.engine.Properties()
}

// Capacity returns the capacity of the underlying storage engine. Note that
// this does not include reservations.
// Note that Capacity() has the side effect of updating some of the store's
// internal statistics about its replicas.
func (s *Store) Capacity(ctx context.Context, useCached bool) (roachpb.StoreCapacity, error) {
	if useCached {
		s.cachedCapacity.Lock()
		capacity := s.cachedCapacity.StoreCapacity
		s.cachedCapacity.Unlock()
		if capacity != (roachpb.StoreCapacity{}) {
			return capacity, nil
		}
	}

	capacity, err := s.engine.Capacity()
	if err != nil {
		return capacity, err
	}

	now := s.cfg.Clock.NowAsClockTimestamp()
	var leaseCount int32
	var rangeCount int32
	var logicalBytes int64
	var totalQueriesPerSecond float64
	var totalWritesPerSecond float64
	replicaCount := s.metrics.ReplicaCount.Value()
	bytesPerReplica := make([]float64, 0, replicaCount)
	writesPerReplica := make([]float64, 0, replicaCount)
	rankingsAccumulator := s.replRankings.newAccumulator()
	newStoreReplicaVisitor(s).Visit(func(r *Replica) bool {
		rangeCount++
		if r.OwnsValidLease(ctx, now) {
			leaseCount++
		}
		mvccStats := r.GetMVCCStats()
		logicalBytes += mvccStats.Total()
		bytesPerReplica = append(bytesPerReplica, float64(mvccStats.Total()))
		// TODO(a-robinson): How dangerous is it that these numbers will be
		// incorrectly low the first time or two it gets gossiped when a store
		// starts? We can't easily have a countdown as its value changes like for
		// leases/replicas.
		var qps float64
		if avgQPS, dur := r.leaseholderStats.avgQPS(); dur >= MinStatsDuration {
			qps = avgQPS
			totalQueriesPerSecond += avgQPS
			// TODO(a-robinson): Calculate percentiles for qps? Get rid of other percentiles?
		}
		if wps, dur := r.writeStats.avgQPS(); dur >= MinStatsDuration {
			totalWritesPerSecond += wps
			writesPerReplica = append(writesPerReplica, wps)
		}
		rankingsAccumulator.addReplica(replicaWithStats{
			repl: r,
			qps:  qps,
		})
		return true
	})
	capacity.RangeCount = rangeCount
	capacity.LeaseCount = leaseCount
	capacity.LogicalBytes = logicalBytes
	capacity.QueriesPerSecond = totalQueriesPerSecond
	capacity.WritesPerSecond = totalWritesPerSecond
	capacity.ReadAmplification = s.metrics.RdbReadAmplification.Value()
	capacity.BytesPerReplica = roachpb.PercentilesFromData(bytesPerReplica)
	capacity.WritesPerReplica = roachpb.PercentilesFromData(writesPerReplica)
	s.recordNewPerSecondStats(totalQueriesPerSecond, totalWritesPerSecond)
	s.replRankings.update(rankingsAccumulator)

	s.cachedCapacity.Lock()
	s.cachedCapacity.StoreCapacity = capacity
	s.cachedCapacity.Unlock()

	return capacity, nil
}

// ReplicaCount returns the number of replicas contained by this store. This
// method is O(n) in the number of replicas and should not be called from
// performance critical code.
func (s *Store) ReplicaCount() int {
	var count int
	s.mu.replicasByRangeID.Range(func(*Replica) {
		count++
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
// the provided stream and returns with an optional error when the rangefeed is
// complete.
func (s *Store) RangeFeed(
	args *roachpb.RangeFeedRequest, stream roachpb.Internal_RangeFeedServer,
) *roachpb.Error {

	if filter := s.TestingKnobs().TestingRangefeedFilter; filter != nil {
		if pErr := filter(args, stream); pErr != nil {
			return pErr
		}
	}

	if err := verifyKeys(args.Span.Key, args.Span.EndKey, true); err != nil {
		return roachpb.NewError(err)
	}

	// Get range and add command to the range for execution.
	repl, err := s.GetReplica(args.RangeID)
	if err != nil {
		return roachpb.NewError(err)
	}
	if !repl.IsInitialized() {
		// (*Store).Send has an optimization for uninitialized replicas to send back
		// a NotLeaseHolderError with a hint of where an initialized replica might
		// be found. RangeFeeds can always be served from followers and so don't
		// otherwise return NotLeaseHolderError. For simplicity we also don't return
		// one here.
		return roachpb.NewError(roachpb.NewRangeNotFoundError(args.RangeID, s.StoreID()))
	}
	return repl.RangeFeed(args, stream)
}

// updateReplicationGauges counts a number of simple replication statistics for
// the ranges in this store.
// TODO(bram): #4564 It may be appropriate to compute these statistics while
// scanning ranges. An ideal solution would be to create incremental events
// whenever availability changes.
func (s *Store) updateReplicationGauges(ctx context.Context) error {
	var (
		raftLeaderCount               int64
		leaseHolderCount              int64
		leaseExpirationCount          int64
		leaseEpochCount               int64
		raftLeaderNotLeaseHolderCount int64
		quiescentCount                int64
		uninitializedCount            int64
		averageQueriesPerSecond       float64
		averageWritesPerSecond        float64

		rangeCount                int64
		unavailableRangeCount     int64
		underreplicatedRangeCount int64
		overreplicatedRangeCount  int64
		behindCount               int64

		locks                          int64
		totalLockHoldDurationNanos     int64
		maxLockHoldDurationNanos       int64
		locksWithWaitQueues            int64
		lockWaitQueueWaiters           int64
		totalLockWaitDurationNanos     int64
		maxLockWaitDurationNanos       int64
		maxLockWaitQueueWaitersForLock int64

		minMaxClosedTS hlc.Timestamp
	)

	now := s.cfg.Clock.NowAsClockTimestamp()
	var livenessMap liveness.IsLiveMap
	if s.cfg.NodeLiveness != nil {
		livenessMap = s.cfg.NodeLiveness.GetIsLiveMap()
	}
	clusterNodes := s.ClusterNodeCount()

	s.mu.RLock()
	uninitializedCount = int64(len(s.mu.uninitReplicas))
	s.mu.RUnlock()

	newStoreReplicaVisitor(s).Visit(func(rep *Replica) bool {
		metrics := rep.Metrics(ctx, now, livenessMap, clusterNodes)
		if metrics.Leader {
			raftLeaderCount++
			if metrics.LeaseValid && !metrics.Leaseholder {
				raftLeaderNotLeaseHolderCount++
			}
		}
		if metrics.Leaseholder {
			leaseHolderCount++
			switch metrics.LeaseType {
			case roachpb.LeaseNone:
			case roachpb.LeaseExpiration:
				leaseExpirationCount++
			case roachpb.LeaseEpoch:
				leaseEpochCount++
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
		}
		behindCount += metrics.BehindCount
		if qps, dur := rep.leaseholderStats.avgQPS(); dur >= MinStatsDuration {
			averageQueriesPerSecond += qps
		}
		if wps, dur := rep.writeStats.avgQPS(); dur >= MinStatsDuration {
			averageWritesPerSecond += wps
		}
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
		mc := rep.GetClosedTimestamp(ctx)
		if minMaxClosedTS.IsEmpty() || mc.Less(minMaxClosedTS) {
			minMaxClosedTS = mc
		}
		return true // more
	})

	s.metrics.RaftLeaderCount.Update(raftLeaderCount)
	s.metrics.RaftLeaderNotLeaseHolderCount.Update(raftLeaderNotLeaseHolderCount)
	s.metrics.LeaseHolderCount.Update(leaseHolderCount)
	s.metrics.LeaseExpirationCount.Update(leaseExpirationCount)
	s.metrics.LeaseEpochCount.Update(leaseEpochCount)
	s.metrics.QuiescentCount.Update(quiescentCount)
	s.metrics.UninitializedCount.Update(uninitializedCount)
	s.metrics.AverageQueriesPerSecond.Update(averageQueriesPerSecond)
	s.metrics.AverageWritesPerSecond.Update(averageWritesPerSecond)
	s.recordNewPerSecondStats(averageQueriesPerSecond, averageWritesPerSecond)

	s.metrics.RangeCount.Update(rangeCount)
	s.metrics.UnavailableRangeCount.Update(unavailableRangeCount)
	s.metrics.UnderReplicatedRangeCount.Update(underreplicatedRangeCount)
	s.metrics.OverReplicatedRangeCount.Update(overreplicatedRangeCount)
	s.metrics.RaftLogFollowerBehindCount.Update(behindCount)

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

	s.metrics.RaftEnqueuedPending.Update(s.cfg.Transport.queuedMessageCount())

	return nil
}

// checkpoint creates a RocksDB checkpoint in the auxiliary directory with the
// provided tag used in the filepath. The filepath for the checkpoint directory
// is returned.
func (s *Store) checkpoint(ctx context.Context, tag string) (string, error) {
	checkpointBase := filepath.Join(s.engine.GetAuxiliaryDir(), "checkpoints")
	_ = s.engine.MkdirAll(checkpointBase)

	checkpointDir := filepath.Join(checkpointBase, tag)
	if err := s.engine.CreateCheckpoint(checkpointDir); err != nil {
		return "", err
	}

	return checkpointDir, nil
}

// ComputeMetrics immediately computes the current value of store metrics which
// cannot be computed incrementally. This method should be invoked periodically
// by a higher-level system which records store metrics.
//
// The tick argument should increment across repeated calls to this
// method. It is used to compute some metrics less frequently than others.
func (s *Store) ComputeMetrics(ctx context.Context, tick int) error {
	ctx = s.AnnotateCtx(ctx)
	if err := s.updateCapacityGauges(ctx); err != nil {
		return err
	}
	if err := s.updateReplicationGauges(ctx); err != nil {
		return err
	}

	// Get the latest engine metrics.
	m := s.engine.GetMetrics()
	s.metrics.updateEngineMetrics(m)

	// Get engine Env stats.
	envStats, err := s.engine.GetEnvStats()
	if err != nil {
		return err
	}
	s.metrics.updateEnvStats(*envStats)

	// Log this metric infrequently (with current configurations,
	// every 10 minutes). Trigger on tick 1 instead of tick 0 so that
	// non-periodic callers of this method don't trigger expensive
	// stats.
	if tick%logSSTInfoTicks == 1 /* every 10m */ {
		// NB: The initial blank line ensures that compaction stats display
		// will not contain the log prefix.
		log.Infof(ctx, "\n%s", m.Metrics)
	}
	return nil
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
	Desc *roachpb.RangeDescriptor
	QPS  float64
}

// HottestReplicas returns the hottest replicas on a store, sorted by their
// QPS. Only contains ranges for which this store is the leaseholder.
//
// Note that this uses cached information, so it's cheap but may be slightly
// out of date.
func (s *Store) HottestReplicas() []HotReplicaInfo {
	topQPS := s.replRankings.topQPS()
	hotRepls := make([]HotReplicaInfo, len(topQPS))
	for i := range topQPS {
		hotRepls[i].Desc = topQPS[i].repl.Desc()
		hotRepls[i].QPS = topQPS[i].qps
	}
	return hotRepls
}

// StoreKeySpanStats carries the result of a stats computation over a key range.
type StoreKeySpanStats struct {
	ReplicaCount         int
	MVCC                 enginepb.MVCCStats
	ApproximateDiskBytes uint64
}

// ComputeStatsForKeySpan computes the aggregated MVCCStats for all replicas on
// this store which contain any keys in the supplied range.
func (s *Store) ComputeStatsForKeySpan(startKey, endKey roachpb.RKey) (StoreKeySpanStats, error) {
	var result StoreKeySpanStats

	newStoreReplicaVisitor(s).Visit(func(repl *Replica) bool {
		desc := repl.Desc()
		if bytes.Compare(startKey, desc.EndKey) >= 0 || bytes.Compare(desc.StartKey, endKey) >= 0 {
			return true // continue
		}
		result.MVCC.Add(repl.GetMVCCStats())
		result.ReplicaCount++
		return true
	})

	var err error
	result.ApproximateDiskBytes, err = s.engine.ApproximateDiskBytes(startKey.AsRawKey(), endKey.AsRawKey())
	return result, err
}

// AllocatorDryRun runs the given replica through the allocator without actually
// carrying out any changes, returning all trace messages collected along the way.
// Intended to help power a debug endpoint.
func (s *Store) AllocatorDryRun(ctx context.Context, repl *Replica) (tracing.Recording, error) {
	ctx, collectAndFinish := tracing.ContextWithRecordingSpan(ctx, s.cfg.AmbientCtx.Tracer, "allocator dry run")
	defer collectAndFinish()
	canTransferLease := func(ctx context.Context, repl *Replica) bool { return true }
	_, err := s.replicateQueue.processOneChange(
		ctx, repl, canTransferLease, false /* scatter */, true, /* dryRun */
	)
	if err != nil {
		log.Eventf(ctx, "error simulating allocator on replica %s: %s", repl, err)
	}
	return collectAndFinish(), nil
}

// ManuallyEnqueue runs the given replica through the requested queue,
// returning all trace events collected along the way as well as the error
// message returned from the queue's process method, if any.  Intended to help
// power an admin debug endpoint.
func (s *Store) ManuallyEnqueue(
	ctx context.Context, queueName string, repl *Replica, skipShouldQueue bool,
) (recording tracing.Recording, processError error, enqueueError error) {
	ctx = repl.AnnotateCtx(ctx)

	// Do not enqueue uninitialized replicas. The baseQueue ignores these during
	// normal queue scheduling, but we error here to signal to the user that the
	// operation was unsuccessful.
	if !repl.IsInitialized() {
		return nil, nil, errors.Errorf("not enqueueing uninitialized replica %s", repl)
	}

	var queue queueImpl
	var needsLease bool
	for _, replicaQueue := range s.scanner.queues {
		if strings.EqualFold(replicaQueue.Name(), queueName) {
			queue = replicaQueue.(queueImpl)
			needsLease = replicaQueue.NeedsLease()
		}
	}
	if queue == nil {
		return nil, nil, errors.Errorf("unknown queue type %q", queueName)
	}

	confReader, err := s.GetConfReader(ctx)
	if err != nil {
		return nil, nil, errors.Wrap(err,
			"unable to retrieve conf reader, cannot run queue; make sure "+
				"the cluster has been initialized and all nodes connected to it")
	}

	// Many queues are only meant to be run on leaseholder replicas, so attempt to
	// take the lease here or bail out early if a different replica has it.
	if needsLease {
		hasLease, pErr := repl.getLeaseForGossip(ctx)
		if pErr != nil {
			return nil, nil, pErr.GoError()
		}
		if !hasLease {
			return nil, errors.Newf("replica %v does not have the range lease", repl), nil
		}
	}

	ctx, collectAndFinish := tracing.ContextWithRecordingSpan(
		ctx, s.cfg.AmbientCtx.Tracer, fmt.Sprintf("manual %s queue run", queueName))
	defer collectAndFinish()

	if !skipShouldQueue {
		log.Eventf(ctx, "running %s.shouldQueue", queueName)
		shouldQueue, priority := queue.shouldQueue(ctx, s.cfg.Clock.NowAsClockTimestamp(), repl, confReader)
		log.Eventf(ctx, "shouldQueue=%v, priority=%f", shouldQueue, priority)
		if !shouldQueue {
			return collectAndFinish(), nil, nil
		}
	}

	log.Eventf(ctx, "running %s.process", queueName)
	processed, processErr := queue.process(ctx, repl, confReader)
	log.Eventf(ctx, "processed: %t (err: %v)", processed, processErr)
	return collectAndFinish(), processErr, nil
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

// Implementation of the storeForTruncator interface.
type storeForTruncatorImpl Store

var _ storeForTruncator = &storeForTruncatorImpl{}

func (s *storeForTruncatorImpl) acquireReplicaForTruncator(
	rangeID roachpb.RangeID,
) replicaForTruncator {
	r, err := (*Store)(s).GetReplica(rangeID)
	if err != nil || r == nil {
		// The only error we can see here is roachpb.NewRangeNotFoundError, so we
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
	return (*Store)(s).engine
}

// WriteClusterVersion writes the given cluster version to the store-local
// cluster version key. We only accept a raw engine to ensure we're persisting
// the write durably.
func WriteClusterVersion(
	ctx context.Context, eng storage.Engine, cv clusterversion.ClusterVersion,
) error {
	err := storage.MVCCPutProto(ctx, eng, nil, keys.StoreClusterVersionKey(), hlc.Timestamp{}, nil, &cv)
	if err != nil {
		return err
	}

	// The storage engine sometimes must make backwards incompatible
	// changes. However, the store cluster version key is a key stored
	// within the storage engine, so it's unavailable when the store is
	// opened.
	//
	// The storage engine maintains its own minimum version on disk that
	// it may consult it before opening the Engine. This version is
	// stored in a separate file on the filesystem. For now, write to
	// this file in combination with the store cluster version key.
	//
	// This parallel version state is a bit of a wart and an eventual
	// goal is to replace the store cluster version key with the storage
	// engine's flat file. This requires that there are no writes to the
	// engine until either bootstrapping or joining an existing cluster.
	// Writing the version to this file would happen before opening the
	// engine for completing the rest of bootstrapping/joining the
	// cluster.
	return eng.SetMinVersion(cv.Version)
}

// ReadClusterVersion reads the cluster version from the store-local version
// key. Returns an empty version if the key is not found.
func ReadClusterVersion(
	ctx context.Context, reader storage.Reader,
) (clusterversion.ClusterVersion, error) {
	var cv clusterversion.ClusterVersion
	_, err := storage.MVCCGetProto(ctx, reader, keys.StoreClusterVersionKey(), hlc.Timestamp{},
		&cv, storage.MVCCGetOptions{})
	return cv, err
}

func init() {
	tracing.RegisterTagRemapping("s", "store")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// KVAdmissionController provides admission control for the KV layer.
type KVAdmissionController interface {
	// AdmitKVWork must be called before performing KV work.
	// BatchRequest.AdmissionHeader and BatchRequest.Replica.StoreID must be
	// populated for admission to work correctly. If err is non-nil, the
	// returned handle can be ignored. If err is nil, AdmittedKVWorkDone must be
	// called after the KV work is done executing.
	AdmitKVWork(
		ctx context.Context, tenantID roachpb.TenantID, ba *roachpb.BatchRequest,
	) (handle interface{}, err error)
	// AdmittedKVWorkDone is called after the admitted KV work is done
	// executing.
	AdmittedKVWorkDone(handle interface{})
}

// KVAdmissionControllerImpl implements KVAdmissionController interface.
type KVAdmissionControllerImpl struct {
	// Admission control queues and coordinators. Both should be nil or non-nil.
	kvAdmissionQ     *admission.WorkQueue
	storeGrantCoords *admission.StoreGrantCoordinators
}

var _ KVAdmissionController = KVAdmissionControllerImpl{}

type admissionHandle struct {
	tenantID                           roachpb.TenantID
	callAdmittedWorkDoneOnKVAdmissionQ bool
	storeAdmissionQ                    *admission.WorkQueue
}

func isSingleHeartbeatTxnRequest(b *roachpb.BatchRequest) bool {
	if len(b.Requests) != 1 {
		return false
	}
	_, ok := b.Requests[0].GetInner().(*roachpb.HeartbeatTxnRequest)
	return ok
}

// MakeKVAdmissionController returns a KVAdmissionController. Both parameters
// must together either be nil or non-nil.
func MakeKVAdmissionController(
	kvAdmissionQ *admission.WorkQueue, storeGrantCoords *admission.StoreGrantCoordinators,
) KVAdmissionController {
	return KVAdmissionControllerImpl{
		kvAdmissionQ:     kvAdmissionQ,
		storeGrantCoords: storeGrantCoords,
	}
}

// AdmitKVWork implements the KVAdmissionController interface.
func (n KVAdmissionControllerImpl) AdmitKVWork(
	ctx context.Context, tenantID roachpb.TenantID, ba *roachpb.BatchRequest,
) (handle interface{}, err error) {
	ah := admissionHandle{tenantID: tenantID}
	if n.kvAdmissionQ != nil {
		bypassAdmission := ba.IsAdmin()
		source := ba.AdmissionHeader.Source
		if !roachpb.IsSystemTenantID(tenantID.ToUint64()) {
			// Request is from a SQL node.
			bypassAdmission = false
			source = roachpb.AdmissionHeader_FROM_SQL
		}
		if source == roachpb.AdmissionHeader_OTHER {
			bypassAdmission = true
		}
		createTime := ba.AdmissionHeader.CreateTime
		if !bypassAdmission && createTime == 0 {
			// TODO(sumeer): revisit this for multi-tenant. Specifically, the SQL use
			// of zero CreateTime needs to be revisited. It should use high priority.
			createTime = timeutil.Now().UnixNano()
		}
		admissionInfo := admission.WorkInfo{
			TenantID:        tenantID,
			Priority:        admission.WorkPriority(ba.AdmissionHeader.Priority),
			CreateTime:      createTime,
			BypassAdmission: bypassAdmission,
		}
		var err error
		// Don't subject HeartbeatTxnRequest to the storeAdmissionQ. Even though
		// it would bypass admission, it would consume a slot. When writes are
		// throttled, we start generating more txn heartbeats, which then consume
		// all the slots, causing no useful work to happen. We do want useful work
		// to continue even when throttling since there are often significant
		// number of tokens available.
		if ba.IsWrite() && !isSingleHeartbeatTxnRequest(ba) {
			ah.storeAdmissionQ = n.storeGrantCoords.TryGetQueueForStore(int32(ba.Replica.StoreID))
		}
		admissionEnabled := true
		if ah.storeAdmissionQ != nil {
			if admissionEnabled, err = ah.storeAdmissionQ.Admit(ctx, admissionInfo); err != nil {
				return admissionHandle{}, err
			}
			if !admissionEnabled {
				// Set storeAdmissionQ to nil so that we don't call AdmittedWorkDone
				// on it. Additionally, the code below will not call
				// kvAdmissionQ.Admit, and so callAdmittedWorkDoneOnKVAdmissionQ will
				// stay false.
				ah.storeAdmissionQ = nil
			}
		}
		if admissionEnabled {
			ah.callAdmittedWorkDoneOnKVAdmissionQ, err = n.kvAdmissionQ.Admit(ctx, admissionInfo)
			if err != nil {
				return admissionHandle{}, err
			}
		}
	}
	return ah, nil
}

// AdmittedKVWorkDone implement the KVAdmissionController interface.
func (n KVAdmissionControllerImpl) AdmittedKVWorkDone(handle interface{}) {
	ah := handle.(admissionHandle)
	if ah.callAdmittedWorkDoneOnKVAdmissionQ {
		n.kvAdmissionQ.AdmittedWorkDone(ah.tenantID)
	}
	if ah.storeAdmissionQ != nil {
		ah.storeAdmissionQ.AdmittedWorkDone(ah.tenantID)
	}
}
