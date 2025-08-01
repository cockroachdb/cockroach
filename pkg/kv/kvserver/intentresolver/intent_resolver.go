// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package intentresolver

import (
	"bytes"
	"context"
	"math"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client/requestbatcher"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnwait"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/debugutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// defaultTaskLimit is the maximum number of asynchronous tasks that may be
// started by intentResolver. When this limit is reached asynchronous tasks will
// start to block to apply backpressure.  This is a last line of defense against
// issues like #4925.
//
// TODO(bdarnell): how to determine best value?
var defaultTaskLimit = envutil.EnvOrDefaultInt(
	"COCKROACH_ASYNC_INTENT_RESOLVER_TASK_LIMIT", 1000,
)

// asyncIntentResolutionTimeout is the timeout when processing a group of
// intents asynchronously. The timeout prevents async intent resolution from
// getting stuck. Since processing intents is best effort, we'd rather give
// up than wait too long (this helps avoid deadlocks during test shutdown).
var asyncIntentResolutionTimeout = envutil.EnvOrDefaultDuration(
	"COCKROACH_ASYNC_INTENT_RESOLUTION_TIMEOUT", 30*time.Second,
)

// gcBatchSize is the maximum number of transaction records that will be GCed
// in a single batch. Batches that span many ranges (which is possible for the
// transaction records that spans many ranges) will be split into many batches
// by the DistSender.
var gcBatchSize = envutil.EnvOrDefaultInt(
	"COCKROACH_TXN_RECORD_GC_BATCH_SIZE", 1024,
)

// intentResolverBatchSize is the maximum number of single-intent resolution
// requests that will be sent in a single batch. Batches that span many
// ranges (which is possible for the commit of a transaction that spans many
// ranges) will be split into many batches by the DistSender.
//
// TODO(ajwerner): justify this value
var intentResolverBatchSize = envutil.EnvOrDefaultInt(
	"COCKROACH_INTENT_RESOLVER_BATCH_SIZE", 100,
)

// intentResolverRangeBatchSize is the maximum number of ranged intent
// resolutions requests that will be sent in a single batch.  It is set
// lower that intentResolverBatchSize since each request can fan out to a
// large number of intents.
var intentResolverRangeBatchSize = envutil.EnvOrDefaultInt(
	"COCKROACH_RANGED_INTENT_RESOLVER_BATCH_SIZE", 10,
)

// intentResolverRangeRequestSize is the maximum number of intents a single
// range request can resolve. When exceeded, the response will include a
// ResumeSpan and the batcher will send a new range request.
var intentResolverRangeRequestSize = envutil.EnvOrDefaultInt(
	"COCKROACH_RANGED_INTENT_RESOLVER_REQUEST_SIZE", 200,
)

// intentResolverRequestTargetBytes is the target number of bytes of the
// write batch resulting from an intent resolution request. When exceeded,
// the response will include a ResumeSpan and the batcher will send a new
// intent resolution request.
var intentResolverRequestTargetBytes = envutil.EnvOrDefaultBytes(
	"COCKROACH_INTENT_RESOLVER_REQUEST_TARGET_BYTES", 4<<20, // 4 MB
)

// intentResolverSendBatchTimeout is the maximum amount of time an intent
// resolution batch request can run for before timeout.
var intentResolverSendBatchTimeout = envutil.EnvOrDefaultDuration(
	"COCKROACH_INTENT_RESOLVER_SEND_BATCH_TIMEOUT", 1*time.Minute,
)

// MaxTxnsPerIntentCleanupBatch is the number of transactions whose
// corresponding intents will be resolved at a time. Intents are batched
// by transaction to avoid timeouts while resolving intents and ensure that
// progress is made.
var MaxTxnsPerIntentCleanupBatch = envutil.EnvOrDefaultInt64(
	"COCKROACH_MAX_TXNS_PER_INTENT_CLEANUP_BATCH", 100,
)

// defaultGCBatchIdle is the default duration which the gc request batcher
// will wait between requests for a range before sending it.
var defaultGCBatchIdle = envutil.EnvOrDefaultDuration(
	"COCKROACH_GC_BATCH_IDLE", -1, // disabled
)

// defaultGCBatchWait is the default duration which the gc request batcher
// will wait between requests for a range before sending it.
var defaultGCBatchWait = envutil.EnvOrDefaultDuration("COCKROACH_GC_BATCH_WAIT_TIME", time.Second)

// intentResolutionBatchWait is used to configure the RequestBatcher which
// batches intent resolution requests across transactions. Intent resolution
// needs to occur in a relatively short period of time after the completion
// of a transaction in order to minimize the contention footprint of the write
// for other contending reads or writes. The chosen value was selected based
// on some light experimentation to ensure that performance does not degrade
// in the face of highly contended workloads.
var defaultIntentResolutionBatchWait = envutil.EnvOrDefaultDuration(
	"COCKROACH_INTENT_RESOLVER_BATCH_WAIT", 10*time.Millisecond,
)

// intentResolutionBatchIdle is similar to the above setting but is used when
// when no additional traffic hits the batch.
var defaultIntentResolutionBatchIdle = envutil.EnvOrDefaultDuration(
	"COCKROACH_INTENT_RESOLVER_BATCH_IDLE", 5*time.Millisecond,
)

// gcTxnRecordTimeout is the timeout for asynchronous txn record removal
// during cleanupFinishedTxnIntents.
var gcTxnRecordTimeout = envutil.EnvOrDefaultDuration("COCKROACH_GC_TXN_RECORD_TIMEOUT", 20*time.Second)

// Config contains the dependencies to construct an IntentResolver.
type Config struct {
	Clock                *hlc.Clock
	DB                   *kv.DB
	Stopper              *stop.Stopper
	Settings             *cluster.Settings
	AmbientCtx           log.AmbientContext
	TestingKnobs         kvserverbase.IntentResolverTestingKnobs
	RangeDescriptorCache RangeCache

	TaskLimit                    int
	MaxGCBatchWait               time.Duration
	MaxGCBatchIdle               time.Duration
	MaxIntentResolutionBatchWait time.Duration
	MaxIntentResolutionBatchIdle time.Duration
}

// RangeCache is a simplified interface to the rngcache.RangeCache.
type RangeCache interface {
	// LookupRangeID looks up range ID for the range containing key.
	LookupRangeID(ctx context.Context, key roachpb.RKey) (roachpb.RangeID, error)
}

// IntentResolver manages the process of pushing transactions and
// resolving intents.
type IntentResolver struct {
	Metrics Metrics

	clock        *hlc.Clock
	db           *kv.DB
	stopper      *stop.Stopper
	testingKnobs kvserverbase.IntentResolverTestingKnobs
	settings     *cluster.Settings
	ambientCtx   log.AmbientContext
	sem          *quotapool.IntPool // semaphore to limit async goroutines

	rdc RangeCache

	gcBatcher      *requestbatcher.RequestBatcher
	irBatcher      *requestbatcher.RequestBatcher
	irRangeBatcher *requestbatcher.RequestBatcher

	mu struct {
		syncutil.Mutex
		// Map from txn ID being pushed to a refcount of requests waiting on the
		// push.
		inFlightPushes map[uuid.UUID]int
		// Set of txn IDs whose list of lock spans are being resolved. Note
		// that this pertains only to EndTxn-style lock cleanups, whether
		// called directly after EndTxn evaluation or during GC of txn spans.
		inFlightTxnCleanups map[uuid.UUID]struct{}
	}
	every                       log.EveryN
	everyAdmissionHeaderMissing log.EveryN
}

func setConfigDefaults(c *Config) {
	if c.TaskLimit == 0 {
		c.TaskLimit = defaultTaskLimit
	}
	if c.TaskLimit == -1 || c.TestingKnobs.ForceSyncIntentResolution {
		c.TaskLimit = 0
	}
	if c.MaxGCBatchIdle == 0 {
		c.MaxGCBatchIdle = defaultGCBatchIdle
	}
	if c.MaxGCBatchWait == 0 {
		c.MaxGCBatchWait = defaultGCBatchWait
	}
	if c.MaxIntentResolutionBatchIdle == 0 {
		c.MaxIntentResolutionBatchIdle = defaultIntentResolutionBatchIdle
	}
	if c.MaxIntentResolutionBatchWait == 0 {
		c.MaxIntentResolutionBatchWait = defaultIntentResolutionBatchWait
	}
	if c.RangeDescriptorCache == nil {
		c.RangeDescriptorCache = nopRangeDescriptorCache{}
	}
}

type nopRangeDescriptorCache struct{}

func (nrdc nopRangeDescriptorCache) LookupRangeID(
	ctx context.Context, key roachpb.RKey,
) (roachpb.RangeID, error) {
	return 0, nil
}

// New creates an new IntentResolver.
func New(c Config) *IntentResolver {
	setConfigDefaults(&c)
	ir := &IntentResolver{
		clock:                       c.Clock,
		db:                          c.DB,
		stopper:                     c.Stopper,
		sem:                         quotapool.NewIntPool("intent resolver", uint64(c.TaskLimit)),
		every:                       log.Every(time.Minute),
		Metrics:                     makeMetrics(),
		rdc:                         c.RangeDescriptorCache,
		testingKnobs:                c.TestingKnobs,
		settings:                    c.Settings,
		everyAdmissionHeaderMissing: log.Every(5 * time.Minute),
	}
	c.Stopper.AddCloser(ir.sem.Closer("stopper"))
	ir.mu.inFlightPushes = map[uuid.UUID]int{}
	ir.mu.inFlightTxnCleanups = map[uuid.UUID]struct{}{}
	intentResolutionSendBatchTimeout := intentResolverSendBatchTimeout
	if c.TestingKnobs.MaxIntentResolutionSendBatchTimeout != 0 {
		intentResolutionSendBatchTimeout = c.TestingKnobs.MaxIntentResolutionSendBatchTimeout
	}
	inFlightGCBackpressureLimit := requestbatcher.DefaultInFlightBackpressureLimit
	if c.TestingKnobs.InFlightBackpressureLimit != 0 {
		inFlightGCBackpressureLimit = c.TestingKnobs.InFlightBackpressureLimit
	}
	gcBatchSize := gcBatchSize
	if c.TestingKnobs.MaxIntentResolutionBatchSize > 0 {
		gcBatchSize = c.TestingKnobs.MaxGCBatchSize
	}
	ir.gcBatcher = requestbatcher.New(requestbatcher.Config{
		AmbientCtx:      c.AmbientCtx,
		Name:            "intent_resolver_gc_batcher",
		MaxMsgsPerBatch: gcBatchSize,
		MaxWait:         c.MaxGCBatchWait,
		MaxIdle:         c.MaxGCBatchIdle,
		MaxTimeout:      intentResolutionSendBatchTimeout,
		// NB: async GC work is not limited by ir.sem, so we do need an in-flight
		// backpressure limit.
		InFlightBackpressureLimit: func() int { return inFlightGCBackpressureLimit },
		Stopper:                   c.Stopper,
		Sender:                    c.DB.NonTransactionalSender(),
	})
	intentResolutionBatchSize := intentResolverBatchSize
	intentResolutionRangeBatchSize := intentResolverRangeBatchSize
	if c.TestingKnobs.MaxIntentResolutionBatchSize > 0 {
		intentResolutionBatchSize = c.TestingKnobs.MaxIntentResolutionBatchSize
		intentResolutionRangeBatchSize = c.TestingKnobs.MaxIntentResolutionBatchSize
	}
	inFlightLimit := inFlightLimitProvider{
		settings:                         c.Settings,
		testingInFlightBackpressureLimit: c.TestingKnobs.InFlightBackpressureLimit,
	}
	ir.irBatcher = requestbatcher.New(requestbatcher.Config{
		AmbientCtx:                c.AmbientCtx,
		Name:                      "intent_resolver_ir_batcher",
		MaxMsgsPerBatch:           intentResolutionBatchSize,
		TargetBytesPerBatchReq:    intentResolverRequestTargetBytes,
		MaxWait:                   c.MaxIntentResolutionBatchWait,
		MaxIdle:                   c.MaxIntentResolutionBatchIdle,
		MaxTimeout:                intentResolutionSendBatchTimeout,
		InFlightBackpressureLimit: inFlightLimit.limit,
		Stopper:                   c.Stopper,
		Sender:                    c.DB.NonTransactionalSender(),
	})
	ir.irRangeBatcher = requestbatcher.New(requestbatcher.Config{
		AmbientCtx:                c.AmbientCtx,
		Name:                      "intent_resolver_ir_range_batcher",
		MaxMsgsPerBatch:           intentResolutionRangeBatchSize,
		MaxKeysPerBatchReq:        intentResolverRangeRequestSize,
		TargetBytesPerBatchReq:    intentResolverRequestTargetBytes,
		MaxWait:                   c.MaxIntentResolutionBatchWait,
		MaxIdle:                   c.MaxIntentResolutionBatchIdle,
		MaxTimeout:                intentResolutionSendBatchTimeout,
		InFlightBackpressureLimit: inFlightLimit.limit,
		Stopper:                   c.Stopper,
		Sender:                    c.DB.NonTransactionalSender(),
	})
	return ir
}

func getPusherTxn(h kvpb.Header) roachpb.Transaction {
	// If the txn is nil, we communicate a priority by sending an empty
	// txn with only the priority set. This is official usage of PushTxn.
	txn := h.Txn
	if txn == nil {
		txn = &roachpb.Transaction{
			TxnMeta: enginepb.TxnMeta{
				Priority: roachpb.MakePriority(h.UserPriority),
			},
		}
	}
	return *txn
}

// updateIntentTxnStatus takes a slice of intents and a set of pushed
// transactions (like returned from MaybePushTransactions) and updates
// each intent with its corresponding TxnMeta and Status.
// resultSlice is an optional value to allow the caller to preallocate
// the returned intent slice.
func updateIntentTxnStatus(
	ctx context.Context,
	pushedTxns map[uuid.UUID]*roachpb.Transaction,
	intents []roachpb.Intent,
	skipIfInFlight bool,
	results []roachpb.LockUpdate,
) []roachpb.LockUpdate {
	for _, intent := range intents {
		pushee, ok := pushedTxns[intent.Txn.ID]
		if !ok {
			// The intent was not pushed.
			if !skipIfInFlight {
				log.Fatalf(ctx, "no PushTxn response for intent %+v", intent)
			}
			// It must have been skipped.
			continue
		}
		up := roachpb.MakeLockUpdate(pushee, roachpb.Span{Key: intent.Key})
		results = append(results, up)
	}
	return results
}

// PushTransaction takes a transaction and pushes its record using the specified
// push type and request header. It returns the transaction proto corresponding
// to the pushed transaction, and in the case of an ABORTED transaction, a bool
// indicating whether the abort was ambiguous (see
// PushTxnResponse.AmbiguousAbort).
//
// NB: ambiguousAbort may be false with nodes <24.1.
func (ir *IntentResolver) PushTransaction(
	ctx context.Context, pushTxn *enginepb.TxnMeta, h kvpb.Header, pushType kvpb.PushTxnType,
) (_ *roachpb.Transaction, ambiguousAbort bool, _ *kvpb.Error) {
	pushTxns := make(map[uuid.UUID]*enginepb.TxnMeta, 1)
	pushTxns[pushTxn.ID] = pushTxn
	pushedTxns, ambiguousAbort, pErr := ir.MaybePushTransactions(
		ctx, pushTxns, h, pushType, false /* skipIfInFlight */)
	if pErr != nil {
		return nil, false, pErr
	}
	pushedTxn, ok := pushedTxns[pushTxn.ID]
	if !ok {
		log.Fatalf(ctx, "missing PushTxn responses for %s", pushTxn)
	}
	return pushedTxn, ambiguousAbort, nil
}

// MaybePushTransactions tries to push the conflicting transaction(s):
// either moving their timestamp forward on a read/write conflict, aborting
// it on a write/write conflict, or doing nothing if the transaction is no
// longer pending.
//
// Returns a set of transaction protos who correspond to the pushed transactions
// and whose intents can now be resolved, along with a bool indicating whether
// any of the responses were an ambiguous abort (see
// PushTxnResponse.AmbiguousAbort), and an error.
//
// NB: anyAmbiguousAbort may be false with nodes <24.1.
//
// If skipIfInFlight is true, then no PushTxns will be sent and no intents
// will be returned for any transaction for which there is another push in
// progress. This should only be used by callers who are not relying on the
// side effect of a push (i.e. only pushType==PUSH_TOUCH), and who also
// don't need to synchronize with the resolution of those intents (e.g.
// asynchronous resolutions of intents skipped on inconsistent reads).
//
// Callers are involved with
// a) conflict resolution for commands being executed at the Store with the
//
//	client waiting,
//
// b) resolving intents encountered during inconsistent operations, and
// c) resolving intents upon EndTxn which are not local to the given range.
//
//	This is the only path in which the transaction is going to be in
//	non-pending state and doesn't require a push.
func (ir *IntentResolver) MaybePushTransactions(
	ctx context.Context,
	pushTxns map[uuid.UUID]*enginepb.TxnMeta,
	h kvpb.Header,
	pushType kvpb.PushTxnType,
	skipIfInFlight bool,
) (_ map[uuid.UUID]*roachpb.Transaction, anyAmbiguousAbort bool, _ *kvpb.Error) {
	// Decide which transactions to push and which to ignore because
	// of other in-flight requests. For those transactions that we
	// will be pushing, increment their ref count in the in-flight
	// pushes map.
	ir.mu.Lock()
	for txnID := range pushTxns {
		_, pushTxnInFlight := ir.mu.inFlightPushes[txnID]
		if pushTxnInFlight && skipIfInFlight {
			// Another goroutine is working on this transaction so we can
			// skip it.
			if log.V(1) {
				log.Infof(ctx, "skipping PushTxn for %s; attempt already in flight", txnID)
			}
			delete(pushTxns, txnID)
		} else {
			ir.mu.inFlightPushes[txnID]++
		}
	}
	cleanupInFlightPushes := func() {
		ir.mu.Lock()
		for txnID := range pushTxns {
			ir.mu.inFlightPushes[txnID]--
			if ir.mu.inFlightPushes[txnID] == 0 {
				delete(ir.mu.inFlightPushes, txnID)
			}
		}
		ir.mu.Unlock()
	}
	ir.mu.Unlock()
	if len(pushTxns) == 0 {
		return nil, false, nil
	}

	pusherTxn := getPusherTxn(h)
	log.Eventf(ctx, "pushing %d transaction(s)", len(pushTxns))

	// Attempt to push the transaction(s).
	pushTo := h.Timestamp.Next()
	b := &kv.Batch{}
	b.Header.Timestamp = ir.clock.Now()
	b.Header.Timestamp.Forward(pushTo)
	b.Header.WaitPolicy = h.WaitPolicy
	for _, pushTxn := range pushTxns {
		b.AddRawRequest(&kvpb.PushTxnRequest{
			RequestHeader: kvpb.RequestHeader{
				Key: pushTxn.Key,
			},
			PusherTxn: pusherTxn,
			PusheeTxn: *pushTxn,
			PushTo:    pushTo,
			PushType:  pushType,
		})
	}
	err := ir.db.Run(ctx, b)
	cleanupInFlightPushes()
	if err != nil {
		return nil, false, b.MustPErr()
	}

	br := b.RawResponse()
	pushedTxns := make(map[uuid.UUID]*roachpb.Transaction, len(br.Responses))
	for _, resp := range br.Responses {
		resp := resp.GetInner().(*kvpb.PushTxnResponse)
		txn := &resp.PusheeTxn
		anyAmbiguousAbort = anyAmbiguousAbort || resp.AmbiguousAbort
		if _, ok := pushedTxns[txn.ID]; ok {
			log.Fatalf(ctx, "have two PushTxn responses for %s", txn.ID)
		}
		pushedTxns[txn.ID] = txn
		log.Eventf(ctx, "%s is now %s", txn.ID, txn.Status)
	}
	return pushedTxns, anyAmbiguousAbort, nil
}

// runAsyncTask semi-synchronously runs a generic task function. If
// there is spare capacity in the limited async task semaphore, it's
// run asynchronously; otherwise, it's run synchronously if
// allowSyncProcessing is true; if false, an error is returned.
func (ir *IntentResolver) runAsyncTask(
	ctx context.Context, allowSyncProcessing bool, taskFn func(context.Context),
) error {
	if ir.testingKnobs.DisableAsyncIntentResolution {
		return errors.New("intents not processed as async resolution is disabled")
	}
	err := ir.stopper.RunAsyncTaskEx(
		// If we've successfully launched a background task, dissociate
		// this work from our caller's context and timeout.
		ir.ambientCtx.AnnotateCtx(context.Background()),
		stop.TaskOpts{
			TaskName:   "storage.IntentResolver: processing intents",
			Sem:        ir.sem,
			WaitForSem: false,
		},
		taskFn,
	)
	if err != nil {
		if errors.Is(err, stop.ErrThrottled) {
			ir.Metrics.IntentResolverAsyncThrottled.Inc(1)
			if allowSyncProcessing {
				// A limited task was not available. Rather than waiting for
				// one, we reuse the current goroutine.
				taskFn(ctx)
				return nil
			}
		}
		return errors.Wrapf(err, "during async intent resolution")
	}
	return nil
}

// CleanupIntentsAsync asynchronously processes intents which were
// encountered during another command but did not interfere with the
// execution of that command. This occurs during inconsistent
// reads.
// TODO(nvanbenschoten): is this needed if the intents could not have
// expired yet (i.e. they are not at least 5s old)? Should we filter
// those out? If we don't, will this be too expensive for SKIP LOCKED?
func (ir *IntentResolver) CleanupIntentsAsync(
	ctx context.Context,
	admissionHeader kvpb.AdmissionHeader,
	intents []roachpb.Intent,
	allowSyncProcessing bool,
) error {
	if len(intents) == 0 {
		return nil
	}
	now := ir.clock.Now()
	return ir.runAsyncTask(ctx, allowSyncProcessing, func(ctx context.Context) {
		err := timeutil.RunWithTimeout(ctx, "async intent resolution",
			asyncIntentResolutionTimeout, func(ctx context.Context) error {
				_, err := ir.CleanupIntents(ctx, admissionHeader, intents, now, kvpb.PUSH_TOUCH)
				return err
			})
		if err != nil && ir.every.ShouldLog() {
			log.Warningf(ctx, "%v", err)
		}
	})
}

// CleanupIntents processes a collection of intents by pushing each
// implicated transaction using the specified pushType. Intents
// belonging to non-pending transactions after the push are resolved.
// On success, returns the number of resolved intents. On error, a
// subset of the intents may have been resolved, but zero will be
// returned.
func (ir *IntentResolver) CleanupIntents(
	ctx context.Context,
	admissionHeader kvpb.AdmissionHeader,
	intents []roachpb.Intent,
	now hlc.Timestamp,
	pushType kvpb.PushTxnType,
) (int, error) {
	h := kvpb.Header{Timestamp: now}

	// All transactions in MaybePushTransactions will be sent in a single batch.
	// In order to ensure that progress is made, we want to ensure that this
	// batch does not become too big as to time out due to a deadline set above
	// this call. If the attempt to push intents times out before any intents
	// have been resolved, no progress is made. Since batches are atomic, a
	// batch that times out has no effect. Hence, we chunk the work to ensure
	// progress even when a timeout is eventually hit.
	sort.Sort(intentsByTxn(intents))
	resolved := 0
	const skipIfInFlight = true
	pushTxns := make(map[uuid.UUID]*enginepb.TxnMeta)
	var resolveIntents []roachpb.LockUpdate
	for unpushed := intents; len(unpushed) > 0; {
		for k := range pushTxns { // clear the pushTxns map
			delete(pushTxns, k)
		}
		var prevTxnID uuid.UUID
		var i int
		for i = 0; i < len(unpushed); i++ {
			if curTxn := &unpushed[i].Txn; curTxn.ID != prevTxnID {
				if len(pushTxns) >= int(MaxTxnsPerIntentCleanupBatch) {
					break
				}
				prevTxnID = curTxn.ID
				pushTxns[curTxn.ID] = curTxn
			}
		}

		pushedTxns, _, pErr := ir.MaybePushTransactions(ctx, pushTxns, h, pushType, skipIfInFlight)
		if pErr != nil {
			return 0, errors.Wrapf(pErr.GoError(), "failed to push during intent resolution")
		}
		resolveIntents = updateIntentTxnStatus(ctx, pushedTxns, unpushed[:i],
			skipIfInFlight, resolveIntents[:0])
		// resolveIntents with poison=true because we're resolving
		// intents outside of the context of an EndTxn.
		//
		// Naively, it doesn't seem like we need to poison the abort
		// cache since we're pushing with PUSH_TOUCH - meaning that
		// the primary way our Push leads to aborting intents is that
		// of the transaction having timed out (and thus presumably no
		// client being around any more, though at the time of writing
		// we don't guarantee that). But there are other paths in which
		// the Push comes back successful while the coordinating client
		// may still be active. Examples of this are when:
		//
		// - the transaction was aborted by someone else, but the
		//   coordinating client may still be running.
		// - the transaction entry wasn't written yet, which at the
		//   time of writing has our push abort it, leading to the
		//   same situation as above.
		//
		// Thus, we must poison.
		opts := ResolveOptions{Poison: true, AdmissionHeader: admissionHeader}
		if pErr := ir.ResolveIntents(ctx, resolveIntents, opts); pErr != nil {
			return 0, errors.Wrapf(pErr.GoError(), "failed to resolve intents")
		}
		resolved += len(resolveIntents)
		unpushed = unpushed[i:]
	}
	return resolved, nil
}

// CleanupTxnIntentsAsync asynchronously cleans up intents owned by a
// transaction on completion. When all intents have been successfully resolved,
// the txn record is GC'ed.
//
// WARNING: Since this GCs the txn record, it should only be called in response
// to requests coming from the coordinator or the MVCC GC Queue. We don't want
// other actors to GC a txn record, since that can cause ambiguities for the
// coordinator: if it had STAGED the txn, it won't be able to tell the
// difference between a txn that had been implicitly committed, recovered, and
// GC'ed, and one that someone else aborted and GC'ed.
func (ir *IntentResolver) CleanupTxnIntentsAsync(
	ctx context.Context,
	rangeID roachpb.RangeID,
	endTxns []result.EndTxnIntents,
	allowSyncProcessing bool,
) error {
	for i := range endTxns {
		onComplete := func(err error) {
			if err != nil {
				ir.Metrics.FinalizedTxnCleanupFailed.Inc(1)
			}
		}
		et := &endTxns[i] // copy for goroutine
		if err := ir.runAsyncTask(ctx, allowSyncProcessing, func(ctx context.Context) {
			locked, release := ir.lockInFlightTxnCleanup(ctx, et.Txn.ID)
			if !locked {
				return
			}
			defer release()
			if err := ir.cleanupFinishedTxnIntents(
				// The admission header is constructed using the completed
				// transaction.
				ctx, kv.AdmissionHeaderForLockUpdateForTxn(et.Txn), rangeID, et.Txn, et.Poison, onComplete,
			); err != nil {
				if ir.every.ShouldLog() {
					log.Warningf(ctx, "failed to cleanup transaction intents: %v", err)
				}
			}
		}); err != nil {
			ir.Metrics.FinalizedTxnCleanupFailed.Inc(int64(len(endTxns) - i))
			return err
		}
	}
	return nil
}

// lockInFlightTxnCleanup ensures that only a single attempt is being made
// to cleanup the intents belonging to the specified transaction. Returns
// whether this attempt to lock succeeded and if so, a function to release
// the lock, to be invoked subsequently by the caller.
func (ir *IntentResolver) lockInFlightTxnCleanup(
	ctx context.Context, txnID uuid.UUID,
) (locked bool, release func()) {
	ir.mu.Lock()
	defer ir.mu.Unlock()
	_, inFlight := ir.mu.inFlightTxnCleanups[txnID]
	if inFlight {
		log.Eventf(ctx, "skipping txn resolved; already in flight")
		return false, nil
	}
	ir.mu.inFlightTxnCleanups[txnID] = struct{}{}
	return true, func() {
		ir.mu.Lock()
		delete(ir.mu.inFlightTxnCleanups, txnID)
		ir.mu.Unlock()
	}
}

// CleanupTxnIntentsOnGCAsync cleans up extant intents owned by a single
// transaction, asynchronously (but returning an error if the IntentResolver's
// semaphore is maxed out). If the transaction is not finalized, but expired, it
// is pushed first to abort it. onComplete is called if non-nil upon completion
// of async task with the intention that it be used as a hook to update metrics.
// It will not be called if an error is returned.
func (ir *IntentResolver) CleanupTxnIntentsOnGCAsync(
	ctx context.Context,
	admissionHeader kvpb.AdmissionHeader,
	rangeID roachpb.RangeID,
	txn *roachpb.Transaction,
	now hlc.Timestamp,
	onComplete func(pushed, succeeded bool),
) error {
	return ir.stopper.RunAsyncTaskEx(
		// If we've successfully launched a background task,
		// dissociate this work from our caller's context and
		// timeout.
		ir.ambientCtx.AnnotateCtx(context.Background()),
		stop.TaskOpts{
			TaskName: "processing txn intents",
			Sem:      ir.sem,
			// We really do not want to hang up the MVCC GC queue on this kind
			// of processing, so it's better to just skip txns which we can't
			// pass to the async processor (wait=false). Their intents will
			// get cleaned up on demand, and we'll eventually get back to
			// them. Not much harm in having old txn records lying around in
			// the meantime.
			WaitForSem: false,
		},
		func(ctx context.Context) {
			var pushed, succeeded bool
			defer func() {
				if onComplete != nil {
					onComplete(pushed, succeeded)
				}
			}()
			locked, release := ir.lockInFlightTxnCleanup(ctx, txn.ID)
			if !locked {
				return
			}
			defer release()
			// If the transaction is not yet finalized, but expired, push it
			// before resolving the intents.
			if !txn.Status.IsFinalized() {
				if !txnwait.IsExpired(now, txn) {
					log.VErrEventf(ctx, 3, "cannot push a %s transaction which is not expired: %s", txn.Status, txn)
					return
				}
				b := &kv.Batch{}
				b.Header.Timestamp = now
				b.AddRawRequest(&kvpb.PushTxnRequest{
					RequestHeader: kvpb.RequestHeader{Key: txn.Key},
					PusherTxn: roachpb.Transaction{
						TxnMeta: enginepb.TxnMeta{Priority: enginepb.MaxTxnPriority},
					},
					PusheeTxn: txn.TxnMeta,
					PushType:  kvpb.PUSH_ABORT,
				})
				pushed = true
				if err := ir.db.Run(ctx, b); err != nil {
					log.VErrEventf(ctx, 2, "failed to push %s, expired txn (%s): %s", txn.Status, txn, err)
					return
				}
				// Update the txn with the result of the push, such that the intents we're about
				// to resolve get a final status.
				finalizedTxn := &b.RawResponse().Responses[0].GetInner().(*kvpb.PushTxnResponse).PusheeTxn
				txn = txn.Clone()
				txn.Update(finalizedTxn)
			}
			var onCleanupComplete func(error)
			if onComplete != nil {
				onCompleteCopy := onComplete // copy onComplete for use in onCleanupComplete
				onCleanupComplete = func(err error) {
					onCompleteCopy(pushed, err == nil)
				}
			}
			// Set onComplete to nil to disable the deferred call as the call has now
			// been delegated to the callback passed to cleanupFinishedTxnIntents.
			onComplete = nil
			err := ir.cleanupFinishedTxnIntents(
				ctx, admissionHeader, rangeID, txn, false /* poison */, onCleanupComplete)
			if err != nil {
				if ir.every.ShouldLog() {
					log.Warningf(ctx, "failed to cleanup transaction intents: %+v", err)
				}
			}
		},
	)
}

func (ir *IntentResolver) gcTxnRecord(
	ctx context.Context, rangeID roachpb.RangeID, txn *roachpb.Transaction,
) error {
	// We successfully resolved the intents, so we're able to GC from
	// the txn span directly.
	txnKey := keys.TransactionKey(txn.Key, txn.ID)
	// This is pretty tricky. Transaction keys are range-local and
	// so they are encoded specially. The key range addressed by
	// (txnKey, txnKey.Next()) might be empty (since Next() does
	// not imply monotonicity on the address side). Instead, we
	// send this request to a range determined using the resolved
	// transaction anchor, i.e. if the txn is anchored on
	// /Local/RangeDescriptor/"a"/uuid, the key range below would
	// be ["a", "a\x00"). However, the first range is special again
	// because the above procedure results in KeyMin, but we need
	// at least KeyLocalMax.
	//
	// #7880 will address this by making GCRequest less special and
	// thus obviating the need to cook up an artificial range here.
	var gcArgs kvpb.GCRequest
	{
		key := keys.MustAddr(txn.Key)
		if localMax := keys.MustAddr(keys.LocalMax); key.Less(localMax) {
			key = localMax
		}
		endKey := key.Next()

		gcArgs.RequestHeader = kvpb.RequestHeader{
			Key:    key.AsRawKey(),
			EndKey: endKey.AsRawKey(),
		}
	}
	gcArgs.Keys = append(gcArgs.Keys, kvpb.GCRequest_GCKey{
		Key: txnKey,
	})
	// Although the IntentResolver has a RangeDescriptorCache it could consult to
	// to determine the range to which this request corresponds, GCRequests are
	// always issued on behalf of the range on which this record resides which is
	// a strong signal that it is the range which will contain the transaction
	// record now.
	_, err := ir.gcBatcher.Send(ctx, rangeID, &gcArgs)
	if err != nil {
		return errors.Wrapf(err, "could not GC completed transaction anchored at %s",
			roachpb.Key(txn.Key))
	}
	return nil
}

// cleanupFinishedTxnIntents cleans up a txn's extant intents and, when all
// intents have been successfully resolved, the transaction record is GC'ed
// asynchronously. onComplete will be called when all processing has completed
// which is likely to be after this call returns in the case of success.
func (ir *IntentResolver) cleanupFinishedTxnIntents(
	ctx context.Context,
	admissionHeader kvpb.AdmissionHeader,
	rangeID roachpb.RangeID,
	txn *roachpb.Transaction,
	poison bool,
	onComplete func(error),
) (err error) {
	defer func() {
		// When err is non-nil we are guaranteed that the async task is not started
		// so there is no race on calling onComplete.
		if err != nil && onComplete != nil {
			onComplete(err)
		}
	}()
	// Resolve intents.
	opts := ResolveOptions{
		Poison: poison, MinTimestamp: txn.MinTimestamp, AdmissionHeader: admissionHeader}
	if pErr := ir.resolveIntents(ctx, (*txnLockUpdates)(txn), opts); pErr != nil {
		return errors.Wrapf(pErr.GoError(), "failed to resolve intents")
	}
	// Run transaction record GC outside of ir.sem. We need a new context, in case
	// we're still connected to the client's context (which can happen when
	// allowSyncProcessing is true). Otherwise, we may return to the caller before
	// gcTxnRecord completes, which may cancel the context and abort the cleanup
	// either due to a defer cancel() or client disconnection. We give it a timeout
	// as well, to avoid goroutine leakage.
	return ir.stopper.RunAsyncTask(
		ir.ambientCtx.AnnotateCtx(context.Background()),
		"storage.IntentResolver: cleanup txn records",
		func(ctx context.Context) {
			err := timeutil.RunWithTimeout(ctx, "cleanup txn record",
				gcTxnRecordTimeout, func(ctx context.Context) error {
					return ir.gcTxnRecord(ctx, rangeID, txn)
				})
			if onComplete != nil {
				onComplete(err)
			}
			if err != nil {
				if ir.every.ShouldLog() {
					log.Warningf(ctx, "failed to gc transaction record: %v", err)
				}
			}
		})
}

// ResolveOptions is used during intent resolution.
type ResolveOptions struct {
	// If set, the abort spans on the ranges containing the intents are to be
	// poisoned. If the transaction that had laid down this intent is still
	// running (so this resolving is performed by a pusher) and goes back to these
	// ranges trying to read one of its old intents, the access will be trapped
	// and the read will return an error, thus avoiding the read missing to see
	// its own write.
	//
	// This field is ignored for intents that aren't resolved for an ABORTED txn;
	// in other words, only intents from ABORTED transactions ever poison the
	// abort spans.
	Poison bool
	// The original transaction timestamp from the earliest txn epoch; if
	// supplied, resolution of intent ranges can be optimized in some cases.
	MinTimestamp hlc.Timestamp
	// AdmissionHeader of the caller.
	AdmissionHeader kvpb.AdmissionHeader
	// If set, instructs the IntentResolver to send the intent resolution requests
	// immediately, instead of adding them to a batch and waiting for that batch
	// to fill up with other intent resolution requests. This can be used to avoid
	// any batching-induced latency, and should be used only by foreground traffic
	// that is willing to trade off some throughput for lower latency.
	//
	// In addition to disabling batching, the option will also disable key count
	// and byte size pagination. All requests will be sent in the same batch
	// (subject to splitting on range boundaries) and no MaxSpanRequestKeys or
	// TargetBytes limits will be assigned to limit the number or size of intents
	// resolved by multi-point or ranged intent resolution. Users of the flag
	// should be conscious of this.
	//
	// Because of these limitations, the flag is kept internal to this package. If
	// we want to expose the flag and use it in more cases, we will first need to
	// support key count and byte size pagination when bypassing the batcher.
	sendImmediately bool
}

// lookupRangeID maps a key to a RangeID for best effort batching of intent
// resolution requests.
func (ir *IntentResolver) lookupRangeID(ctx context.Context, key roachpb.Key) roachpb.RangeID {
	rKey, err := keys.Addr(key)
	if err != nil {
		if ir.every.ShouldLog() {
			log.Warningf(ctx, "failed to resolve addr for key %q: %+v", key, err)
		}
		return 0
	}
	rangeID, err := ir.rdc.LookupRangeID(ctx, rKey)
	if err != nil {
		if ir.every.ShouldLog() {
			log.Warningf(ctx, "failed to look up range descriptor for key %q: %+v", key, err)
		}
		return 0
	}
	return rangeID
}

// lockUpdates allows for eager or lazy translation of lock spans to lock updates.
type lockUpdates interface {
	Len() int
	Index(i int) roachpb.LockUpdate
}

var _ lockUpdates = (*txnLockUpdates)(nil)
var _ lockUpdates = (*singleLockUpdate)(nil)
var _ lockUpdates = (*sliceLockUpdates)(nil)

type txnLockUpdates roachpb.Transaction

// Len implements the lockUpdates interface.
func (t *txnLockUpdates) Len() int {
	return len(t.LockSpans)
}

// Index implements the lockUpdates interface.
func (t *txnLockUpdates) Index(i int) roachpb.LockUpdate {
	return roachpb.MakeLockUpdate((*roachpb.Transaction)(t), t.LockSpans[i])
}

type singleLockUpdate roachpb.LockUpdate

// Len implements the lockUpdates interface.
func (s *singleLockUpdate) Len() int {
	return 1
}

// Index implements the lockUpdates interface.
func (s *singleLockUpdate) Index(i int) roachpb.LockUpdate {
	if i != 0 {
		panic("index out of bounds")
	}
	return roachpb.LockUpdate(*s)
}

type sliceLockUpdates []roachpb.LockUpdate

// Len implements the lockUpdates interface.
func (s *sliceLockUpdates) Len() int {
	return len(*s)
}

// Index implements the lockUpdates interface.
func (s *sliceLockUpdates) Index(i int) roachpb.LockUpdate {
	return (*s)[i]
}

// ResolveIntent synchronously resolves an intent according to opts. The method
// is expected to be run on behalf of a user request, as opposed to a background
// task.
func (ir *IntentResolver) ResolveIntent(
	ctx context.Context, intent roachpb.LockUpdate, opts ResolveOptions,
) *kvpb.Error {
	if len(intent.EndKey) == 0 {
		// If the caller wants to resolve a single point intent, let it send the
		// request immediately. This is a performance optimization to resolve
		// conflicting intents immediately for latency-sensitive requests.
		//
		// We don't set this flag when resolving a range of keys or when resolving
		// multiple point intents (in ResolveIntents) due to the limitations around
		// pagination described in the comment on ResolveOptions.sendImmediately.
		opts.sendImmediately = true
	}
	return ir.resolveIntents(ctx, (*singleLockUpdate)(&intent), opts)
}

// ResolveIntents synchronously resolves intents according to opts. The method
// is expected to be run on behalf of a user request, as opposed to a background
// task.
func (ir *IntentResolver) ResolveIntents(
	ctx context.Context, intents []roachpb.LockUpdate, opts ResolveOptions,
) (pErr *kvpb.Error) {
	// TODO(nvanbenschoten): unlike IntentResolver.ResolveIntent, we don't set
	// sendImmediately on the ResolveOptions here. This is because we don't
	// support pagination when sending intent resolution immediately and not
	// through the batcher. If this becomes important, we'll need to lift this
	// limitation.
	return ir.resolveIntents(ctx, (*sliceLockUpdates)(&intents), opts)
}

// resolveIntents synchronously resolves intents according to opts.
// intents can be either sliceLockUpdates or txnLockUpdates. In the
// latter case, transaction LockSpans will be lazily translated to
// LockUpdates as they are accessed in this method.
func (ir *IntentResolver) resolveIntents(
	ctx context.Context, intents lockUpdates, opts ResolveOptions,
) (pErr *kvpb.Error) {
	if intents.Len() == 0 {
		return nil
	}
	defer func() {
		if pErr != nil {
			ir.Metrics.IntentResolutionFailed.Inc(int64(intents.Len()))
		}
	}()
	// Avoid doing any work on behalf of expired contexts. See
	// https://github.com/cockroachdb/cockroach/issues/15997.
	if err := ctx.Err(); err != nil {
		return kvpb.NewError(err)
	}
	log.Eventf(ctx, "resolving %d intents", intents.Len())
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Construct a slice of requests to send.
	var singleReq [1]kvpb.Request //gcassert:noescape
	reqs := resolveIntentReqs(intents, opts, singleReq[:])
	h := opts.AdmissionHeader
	// We skip the warning for release builds to avoid printing out verbose stack traces.
	// NB: this was disabled in general since there's a large backlog of reported warnings
	// that yet have to be resolved, and in the meantime it's not worth  more engineering
	// time making additional reports.
	// TODO(aaditya): reconsider this once #112680 is resolved.
	// if !build.IsRelease() && h == (kvpb.AdmissionHeader{}) && ir.everyAdmissionHeaderMissing.ShouldLog() {
	if false {
		log.Warningf(ctx,
			"test-only warning: if you see this, please report to https://github.com/cockroachdb/cockroach/issues/112680. empty admission header provided by %s", debugutil.Stack())
	}
	// Send the requests ...
	if opts.sendImmediately {
		bypassAdmission := sendImmediatelyBypassAdmissionControl.Get(&ir.settings.SV)
		if bypassAdmission {
			h = kv.AdmissionHeaderForBypass(h)
		}
		// ... using a single batch.
		b := &kv.Batch{}
		b.AdmissionHeader = h
		b.AddRawRequest(reqs...)
		if err := ir.db.Run(ctx, b); err != nil {
			return b.MustPErr()
		}
		return nil
	}
	// ... using their corresponding request batcher.
	respChan := make(chan requestbatcher.Response, len(reqs))
	batcherBypassAdmission := batchBypassAdmissionControl.Get(&ir.settings.SV)
	if batcherBypassAdmission {
		h = kv.AdmissionHeaderForBypass(h)
	}
	for _, req := range reqs {
		var batcher *requestbatcher.RequestBatcher
		switch req.Method() {
		case kvpb.ResolveIntent:
			batcher = ir.irBatcher
		case kvpb.ResolveIntentRange:
			batcher = ir.irRangeBatcher
		default:
			panic("unexpected")
		}
		rangeID := ir.lookupRangeID(ctx, req.Header().Key)
		if err := batcher.SendWithChan(ctx, respChan, rangeID, req, h); err != nil {
			return kvpb.NewError(err)
		}
	}
	// Collect responses.
	for range reqs {
		select {
		case resp := <-respChan:
			if resp.Err != nil {
				return kvpb.NewError(resp.Err)
			}
			_ = resp.Resp // ignore the response
		case <-ctx.Done():
			return kvpb.NewError(ctx.Err())
		case <-ir.stopper.ShouldQuiesce():
			return kvpb.NewErrorf("stopping")
		}
	}
	return nil
}

func resolveIntentReqs(
	intents lockUpdates, opts ResolveOptions, alloc []kvpb.Request,
) []kvpb.Request {
	var pointReqs []kvpb.ResolveIntentRequest
	var rangeReqs []kvpb.ResolveIntentRangeRequest
	for i := 0; i < intents.Len(); i++ {
		intent := intents.Index(i)
		if len(intent.EndKey) == 0 {
			pointReqs = append(pointReqs, kvpb.ResolveIntentRequest{
				RequestHeader:     kvpb.RequestHeaderFromSpan(intent.Span),
				IntentTxn:         intent.Txn,
				Status:            intent.Status,
				Poison:            opts.Poison,
				IgnoredSeqNums:    intent.IgnoredSeqNums,
				ClockWhilePending: intent.ClockWhilePending,
			})
		} else {
			rangeReqs = append(rangeReqs, kvpb.ResolveIntentRangeRequest{
				RequestHeader:     kvpb.RequestHeaderFromSpan(intent.Span),
				IntentTxn:         intent.Txn,
				Status:            intent.Status,
				Poison:            opts.Poison,
				MinTimestamp:      opts.MinTimestamp,
				IgnoredSeqNums:    intent.IgnoredSeqNums,
				ClockWhilePending: intent.ClockWhilePending,
			})
		}
	}
	var reqs []kvpb.Request
	if cap(alloc) >= intents.Len() {
		reqs = alloc[:0]
	} else {
		reqs = make([]kvpb.Request, 0, intents.Len())
	}
	for i := range pointReqs {
		reqs = append(reqs, &pointReqs[i])
	}
	for i := range rangeReqs {
		reqs = append(reqs, &rangeReqs[i])
	}
	return reqs
}

// intentsByTxn implements sort.Interface to sort intents based on txnID.
type intentsByTxn []roachpb.Intent

var _ sort.Interface = intentsByTxn(nil)

func (s intentsByTxn) Len() int      { return len(s) }
func (s intentsByTxn) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s intentsByTxn) Less(i, j int) bool {
	return bytes.Compare(s[i].Txn.ID[:], s[j].Txn.ID[:]) < 0
}

// inFlightBackpressureLimitEnabled controls whether the intent resolving
// requestbatcher.RequestBatchers created by the IntentResolver use an
// in-flight backpressure limit of DefaultInFlightBackpressureLimit. The
// default is false, i.e., there is no limit on in-flight requests. A limit on
// in-flight requests is considered superfluous since we have two limits on
// the number of active goroutines waiting to get their intent resolution
// requests processed: the async goroutines, limited to 1000
// (defaultTaskLimit), and the workload goroutines. Each waiter produces work
// for a single range, and that work can typically be batched into a single
// RPC (since requestbatcher.Config.MaxMsgsPerBatch is quite generous). In
// the rare case where a single waiter produces numerous concurrent RPCs,
// because of a large number of kvpb.Requests (note that wide
// ResolveIntentRange cause pagination, and not concurrent RPCs), we have
// already paid the memory cost of buffering these numerous kvpb.Requests, so
// we may as well send them out.
var inFlightBackpressureLimitEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.intent_resolver.batcher.in_flight_backpressure_limit.enabled",
	"set to true to enable the use of DefaultInFlightBackpressureLimit",
	false)

type inFlightLimitProvider struct {
	settings                         *cluster.Settings
	testingInFlightBackpressureLimit int
}

func (p inFlightLimitProvider) limit() int {
	if p.testingInFlightBackpressureLimit != 0 {
		return p.testingInFlightBackpressureLimit
	}
	if p.settings == nil || inFlightBackpressureLimitEnabled.Get(&p.settings.SV) {
		return requestbatcher.DefaultInFlightBackpressureLimit
	}
	return math.MaxInt32
}
