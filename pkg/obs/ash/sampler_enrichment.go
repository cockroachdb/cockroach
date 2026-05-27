// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/obs/ash/enrichment"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// EnrichmentResolverFn fetches enrichment attributes for a batch of
// execution IDs from a remote gateway node. It is called from the
// sampler tick once per gateway node that needs remote resolution.
// The returned map contains entries only for IDs the remote node
// could resolve; absent entries indicate "unknown / not loaded" and
// are retried on subsequent sampler ticks (up to the retry window).
type EnrichmentResolverFn func(
	ctx context.Context, nodeID roachpb.NodeID, ids []clusterunique.ID,
) (map[clusterunique.ID]enrichment.Attributes, error)

// EnrichmentRPCTimeout bounds the wall-clock time each per-node
// enrichment RPC may take. Per-node calls run in parallel within a
// sampler tick, so the overall enrichment budget for a tick is also
// bounded by this value (not the sum across nodes).
var EnrichmentRPCTimeout = settings.RegisterDurationSetting(
	settings.SystemVisible,
	"obs.ash.enrichment.rpc_timeout",
	"per-node timeout for outbound ASH enrichment RPCs",
	500*time.Millisecond,
	settings.PositiveDuration,
	settings.WithPublic,
)

// EnrichmentRetryMaxAge controls how long a sample may sit on the
// retry queue (waiting for its enrichment to succeed) before being
// dropped. When dropped, the sample is discarded — it does not
// appear in ASH at all. The original design's 30s threshold was
// reduced to 10s because the sampler-tick model retries on every
// tick rather than every 5s, so resolution either happens quickly
// or the sample is unlikely to ever resolve.
var EnrichmentRetryMaxAge = settings.RegisterDurationSetting(
	settings.SystemVisible,
	"obs.ash.enrichment.retry_max_age",
	"maximum time a sample may wait on the enrichment retry queue "+
		"before being dropped",
	10*time.Second,
	settings.PositiveDuration,
	settings.WithPublic,
)

// EnrichmentRetryQueueLimit bounds the number of samples held on the
// retry queue at any one time. When exceeded, the oldest entries are
// dropped (the same semantics as a sampling-buffer overflow). The
// default is sized for a sampler tick of 1s and ASH defaulting to
// roughly a few thousand goroutines under heavy load — small enough
// that retries don't accumulate unboundedly, large enough that brief
// network blips don't lose samples.
var EnrichmentRetryQueueLimit = settings.RegisterIntSetting(
	settings.SystemVisible,
	"obs.ash.enrichment.retry_queue.limit",
	"maximum number of samples held on the enrichment retry queue; "+
		"oldest entries are dropped when exceeded",
	4096,
	settings.PositiveInt,
	settings.WithPublic,
)

// EnrichmentNodeBackoffBase is the base interval for per-node
// exponential backoff. After F consecutive failures contacting a
// node, subsequent enrichment attempts to that node are skipped for
// `min(2^F * base, cap)`. A successful call resets F to zero.
var EnrichmentNodeBackoffBase = settings.RegisterDurationSetting(
	settings.SystemVisible,
	"obs.ash.enrichment.node_backoff.base",
	"base interval for per-node enrichment-RPC exponential backoff",
	time.Second,
	settings.PositiveDuration,
)

// EnrichmentNodeBackoffCap caps the per-node backoff so a permanently
// dead node still gets a retry attempt at this interval (rather than
// disappearing forever).
var EnrichmentNodeBackoffCap = settings.RegisterDurationSetting(
	settings.SystemVisible,
	"obs.ash.enrichment.node_backoff.cap",
	"upper bound on per-node enrichment-RPC backoff",
	time.Hour,
	settings.PositiveDuration,
)

// retryEntry holds a sample that needs another enrichment attempt
// on a future sampler tick. The original capture timestamp is
// preserved so retried samples appear in ASH with the time they
// were observed, not the time they were eventually enriched.
type retryEntry struct {
	sample ASHSample
	// firstCapture is the SampleTime of the work state at the moment
	// it was first captured. Used to age entries out of the queue
	// after EnrichmentRetryMaxAge.
	firstCapture time.Time
}

// nodeBackoff tracks per-gateway-node consecutive-failure state for
// the enrichment-RPC backoff logic.
type nodeBackoff struct {
	consecutiveFailures int
	// nextAttempt is the earliest time the sampler will issue a fresh
	// RPC to this node. Updated after every failed call to enforce
	// the exponential-backoff curve.
	nextAttempt time.Time
}

// enrichmentState holds the per-Sampler state required for in-tick
// enrichment. It is mutated only from the sampler goroutine.
type enrichmentState struct {
	// resolver, when set, calls GetASHEnrichmentData on a specific
	// remote node. Nil before the first SetEnrichmentResolver call,
	// in which case all enrichment is local-only.
	resolver atomic.Pointer[EnrichmentResolverFn]

	// retryQueue holds samples awaiting another enrichment attempt.
	// Owned by the sampler goroutine so no synchronization is needed.
	retryQueue []retryEntry

	// backoff tracks consecutive-failure state per gateway node ID.
	// Owned by the sampler goroutine.
	backoff map[roachpb.NodeID]*nodeBackoff
}

func newEnrichmentState() *enrichmentState {
	return &enrichmentState{
		backoff: make(map[roachpb.NodeID]*nodeBackoff),
	}
}

// SetEnrichmentResolver installs the callback used to fetch
// enrichment attributes from remote gateway nodes. Wired by the
// process bringup code (pkg/server) once the status RPC client is
// available.
func (s *Sampler) SetEnrichmentResolver(fn EnrichmentResolverFn) {
	s.enrichment.resolver.Store(&fn)
}

// SetGlobalEnrichmentResolver installs the resolver on the
// process-wide sampler singleton, if it has been initialized.
func SetGlobalEnrichmentResolver(fn EnrichmentResolverFn) {
	if s := globalSampler.Load(); s != nil {
		s.SetEnrichmentResolver(fn)
	}
}

// enrichSample fills the enrichment fields on sample from the
// per-tenant local cache if available. Returns true if enrichment
// succeeded, false if the sample's EnrichmentID is missing from the
// local cache (caller should attempt remote resolution).
func enrichSampleFromLocal(sample *ASHSample) bool {
	if sample.EnrichmentID == (clusterunique.ID{}) {
		// Nothing to enrich. Treat as "succeeded": the sample carries
		// no enrichment, but we don't want it to land on the retry
		// queue either.
		return true
	}
	cache := enrichment.Lookup(sample.TenantID)
	if cache == nil || !cache.Enabled() {
		// The cache hasn't been registered or is disabled; treat as
		// enriched-without-data so the sample is published as-is.
		return true
	}
	attrs, ok := cache.GetExecution(sample.EnrichmentID)
	if !ok {
		return false
	}
	applyAttributes(sample, attrs)
	return true
}

// applyAttributes copies the resolved enrichment attributes onto the
// sample. AppName is copied even though it may already be set from
// the legacy AppNameID path — the enrichment value always wins when
// present, since it's the canonical source going forward.
func applyAttributes(sample *ASHSample, attrs enrichment.Attributes) {
	if attrs.AppName != "" {
		sample.AppName = attrs.AppName
	}
	sample.Database = attrs.Database
	sample.User = attrs.User
	sample.Query = attrs.Query
	sample.PlanGist = attrs.PlanGist
	sample.CanaryStats = attrs.CanaryStats
	sample.TxnID = attrs.TxnID
	sample.SessionID = attrs.SessionID
}

// enrichRemote performs remote enrichment for a batch of samples,
// grouped per gateway node. Each per-node call runs in its own
// goroutine with a bounded timeout; this method blocks until all
// goroutines complete or time out.
//
// Samples that were enriched are returned in `enriched`; samples
// that could not be enriched (RPC failed, node in backoff, no
// resolver registered, or the responding cache had no entry) are
// returned in `unresolved` so the caller can decide whether to
// publish them un-enriched, retry next tick, or drop.
//
// The remoteByNode map captures the per-node grouping computed by
// the caller and is consumed by this method.
func (s *Sampler) enrichRemote(
	ctx context.Context, now time.Time, remoteByNode map[roachpb.NodeID][]*ASHSample,
) (enriched, unresolved []*ASHSample) {
	// Skip remote enrichment until the cluster has finalized to the
	// version that knows about GetASHEnrichmentData. Sending the RPC
	// to a pre-26.3 node would return an "unknown method" error and
	// trip the per-node backoff for every gateway, generating
	// pointless noise. The samples land on the retry queue exactly
	// as if the resolver hadn't been installed, and will be drained
	// once finalization succeeds.
	if !s.st.Version.IsActive(ctx, clusterversion.V26_3_ASHEnrichment) {
		for _, samples := range remoteByNode {
			unresolved = append(unresolved, samples...)
		}
		return nil, unresolved
	}

	resolverPtr := s.enrichment.resolver.Load()
	if resolverPtr == nil {
		// No resolver — everything is unresolved.
		for _, samples := range remoteByNode {
			unresolved = append(unresolved, samples...)
		}
		return nil, unresolved
	}
	resolver := *resolverPtr

	timeout := EnrichmentRPCTimeout.Get(&s.st.SV)
	backoffBase := EnrichmentNodeBackoffBase.Get(&s.st.SV)
	backoffCap := EnrichmentNodeBackoffCap.Get(&s.st.SV)

	type nodeResult struct {
		nodeID   roachpb.NodeID
		mappings map[clusterunique.ID]enrichment.Attributes
		err      error
		skipped  bool
	}
	resultCh := make(chan nodeResult, len(remoteByNode))
	var wg sync.WaitGroup

	for nodeID, samples := range remoteByNode {
		// Per-node backoff: skip the RPC if the node is still in its
		// failure window. The skipped samples become unresolved and
		// are handled the same as RPC failures.
		bo, hasBackoff := s.enrichment.backoff[nodeID]
		if hasBackoff && now.Before(bo.nextAttempt) {
			s.metrics.EnrichmentRPCSkipped.Inc(int64(len(samples)))
			resultCh <- nodeResult{nodeID: nodeID, skipped: true}
			continue
		}

		// Deduplicate IDs per node — multiple samples may share an
		// EnrichmentID (the same statement's work split across many
		// goroutines).
		uniqueIDs := make(map[clusterunique.ID]struct{}, len(samples))
		for _, sample := range samples {
			uniqueIDs[sample.EnrichmentID] = struct{}{}
		}
		idSlice := make([]clusterunique.ID, 0, len(uniqueIDs))
		for id := range uniqueIDs {
			idSlice = append(idSlice, id)
		}

		wg.Add(1)
		go func(nodeID roachpb.NodeID, ids []clusterunique.ID) {
			defer wg.Done()
			s.metrics.EnrichmentRPCAttempts.Inc(1)
			res := nodeResult{nodeID: nodeID}
			err := timeutil.RunWithTimeout(
				ctx, "ash-enrichment", timeout,
				func(rpcCtx context.Context) error {
					mappings, rpcErr := resolver(rpcCtx, nodeID, ids)
					if rpcErr != nil {
						return rpcErr
					}
					res.mappings = mappings
					return nil
				},
			)
			if err != nil {
				res.err = err
			}
			resultCh <- res
		}(nodeID, idSlice)
	}
	wg.Wait()
	close(resultCh)

	for res := range resultCh {
		samples := remoteByNode[res.nodeID]
		switch {
		case res.skipped:
			unresolved = append(unresolved, samples...)
		case res.err != nil:
			s.metrics.EnrichmentRPCErrors.Inc(1)
			s.recordNodeFailure(res.nodeID, now, backoffBase, backoffCap)
			log.Ops.Warningf(ctx, "ASH: enrichment RPC to n%d failed: %v", res.nodeID, res.err)
			unresolved = append(unresolved, samples...)
		default:
			// RPC succeeded; apply attributes per sample. Samples
			// whose ID was not in the response are unresolved (the
			// remote cache didn't have them, possibly because of
			// FIFO eviction or pre-execution sampling).
			s.recordNodeSuccess(res.nodeID)
			for _, sample := range samples {
				if attrs, ok := res.mappings[sample.EnrichmentID]; ok {
					applyAttributes(sample, attrs)
					enriched = append(enriched, sample)
				} else {
					unresolved = append(unresolved, sample)
				}
			}
		}
	}
	return enriched, unresolved
}

func (s *Sampler) recordNodeFailure(nodeID roachpb.NodeID, now time.Time, base, cap time.Duration) {
	bo, ok := s.enrichment.backoff[nodeID]
	if !ok {
		bo = &nodeBackoff{}
		s.enrichment.backoff[nodeID] = bo
	}
	bo.consecutiveFailures++
	// Exponential backoff with a hard cap. The shift is bounded to
	// avoid overflow for absurd failure counts; the cap kicks in
	// well before that becomes possible at production timescales.
	shift := bo.consecutiveFailures
	if shift > 30 {
		shift = 30
	}
	delay := time.Duration(int64(base) << shift)
	if delay <= 0 || delay > cap {
		delay = cap
	}
	bo.nextAttempt = now.Add(delay)
}

func (s *Sampler) recordNodeSuccess(nodeID roachpb.NodeID) {
	if bo, ok := s.enrichment.backoff[nodeID]; ok {
		bo.consecutiveFailures = 0
		bo.nextAttempt = time.Time{}
	}
}

// drainRetryQueue removes entries from the retry queue whose first
// capture is older than the configured max age (dropping them with
// a metric increment), and returns the remaining entries as samples
// for re-enrichment in the current tick.
func (s *Sampler) drainRetryQueue(now time.Time) []*ASHSample {
	if len(s.enrichment.retryQueue) == 0 {
		return nil
	}
	maxAge := EnrichmentRetryMaxAge.Get(&s.st.SV)
	cutoff := now.Add(-maxAge)
	pending := s.enrichment.retryQueue
	s.enrichment.retryQueue = s.enrichment.retryQueue[:0]
	out := make([]*ASHSample, 0, len(pending))
	for i := range pending {
		entry := &pending[i]
		if entry.firstCapture.Before(cutoff) {
			s.metrics.EnrichmentSamplesDropped.Inc(1)
			continue
		}
		// Copy the sample so the caller has stable pointer identity;
		// the slice header is reused for the next tick.
		sampleCopy := entry.sample
		out = append(out, &sampleCopy)
	}
	return out
}

// requeue adds samples that failed enrichment back onto the retry
// queue, preserving the per-sample firstCapture timestamp so the
// drain step can age them out. Drops the oldest entries when the
// queue would exceed its configured limit.
func (s *Sampler) requeue(samples []*ASHSample, firstCaptureByPtr map[*ASHSample]time.Time) {
	limit := int(EnrichmentRetryQueueLimit.Get(&s.st.SV))
	for _, sample := range samples {
		entry := retryEntry{sample: *sample}
		if t, ok := firstCaptureByPtr[sample]; ok {
			entry.firstCapture = t
		} else {
			entry.firstCapture = sample.SampleTime
		}
		s.enrichment.retryQueue = append(s.enrichment.retryQueue, entry)
	}
	if overflow := len(s.enrichment.retryQueue) - limit; overflow > 0 {
		s.metrics.EnrichmentSamplesDropped.Inc(int64(overflow))
		s.enrichment.retryQueue = s.enrichment.retryQueue[overflow:]
	}
}
