// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// coarseRestorePreSplitsPerNode is how many coarse pre-splits RESTORE makes per
// node. Zero turns the pass off.
//
// The pass aims for roughly equal data per node, so the node count sets how
// many pieces are needed. One per node would do if the pieces were all the same
// size, but they are not, so this over-splits by a constant factor to let that
// variation average out. The factor has to be well clear of one: pieces are
// placed independently, so the spread in how many land on a store falls only as
// the square root of their number. Sweeping it on a ten node cluster, two per
// node was no better than not running the pass, while sixteen and thirty-two
// were indistinguishable.
var coarseRestorePreSplitsPerNode = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"bulkio.restore.coarse_pre_splits_per_node",
	"number of coarse pre-splits per node that a linked RESTORE makes and scatters "+
		"before ingesting, bounding how much contiguous data shares a set of stores when "+
		"the split-and-scatter flow's scatters do not move replicas; 0 turns it off",
	int64(metamorphic.ConstantWithTestRange("restore-coarse-pre-splits-per-node", 16, 0, 32)),
	settings.NonNegativeInt,
)

// Goroutines per phase. Scattering gets more than splitting: a split is one
// fast operation while a scatter waits on the replicate queue and a lease
// transfer, and concurrent splits of what is still the same range conflict.
const (
	coarsePreSplitSplitWorkers   = 4
	coarsePreSplitScatterWorkers = 16
)

// coarseSplitKeyReservoir picks evenly spaced keys out of a stream whose length
// is not known in advance, in bounded memory. It keeps every stride-th key and,
// when full, discards every other key and doubles the stride, so it ends with
// between capacity/2 and capacity keys, exactly one stride apart.
//
// The even spacing is why this is not reservoir sampling: a uniform random
// sample of k keys has a largest gap around ln(k) times the mean, which would
// reintroduce the long run of same-placement data this pass exists to bound.
type coarseSplitKeyReservoir struct {
	keys     []roachpb.Key
	capacity int
	stride   int
	seen     int
}

func makeCoarseSplitKeyReservoir(capacity int) coarseSplitKeyReservoir {
	return coarseSplitKeyReservoir{
		keys:     make([]roachpb.Key, 0, capacity),
		capacity: capacity,
		stride:   1,
	}
}

// maybeAdd offers the next key in the stream to the reservoir. It is a no-op on
// a zero-capacity reservoir, i.e. when the pass is off.
func (r *coarseSplitKeyReservoir) maybeAdd(key roachpb.Key) {
	if r.capacity == 0 {
		return
	}
	r.seen++
	if r.seen%r.stride != 0 {
		return
	}
	r.keys = append(r.keys, key)
	if len(r.keys) < r.capacity {
		return
	}
	// Full: keep every other key, leaving the survivors 2*stride apart.
	kept := 0
	for i := 1; i < len(r.keys); i += 2 {
		r.keys[kept] = r.keys[i]
		kept++
	}
	r.keys = r.keys[:kept]
	r.stride *= 2
}

// forEachCoarseKey calls fn once per key, over numWorkers goroutines that each
// take a contiguous run so they work on different parts of the key space. fn
// reports its own failures; a key that fails costs only that key.
func forEachCoarseKey(
	ctx context.Context, keys []roachpb.Key, numWorkers int, fn func(context.Context, roachpb.Key),
) {
	if len(keys) == 0 {
		return
	}
	workers := min(numWorkers, len(keys))
	perWorker := (len(keys) + workers - 1) / workers
	g := ctxgroup.WithContext(ctx)
	for start := 0; start < len(keys); start += perWorker {
		run := keys[start:min(start+perWorker, len(keys))]
		g.GoCtx(func(ctx context.Context) error {
			for _, key := range run {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				fn(ctx, key)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		log.Dev.Warningf(ctx, "coarse pre-split: %v", err)
	}
}

// coarseSplitWithRetry splits at key. Mirrors dbSplitAndScatterer.split without
// the key rewriting, which the caller has already done.
func coarseSplitWithRetry(
	ctx context.Context, db *kv.DB, key roachpb.Key, expiration hlc.Timestamp,
) error {
	retryOpts := retry.Options{
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
		Multiplier:     2,
		MaxRetries:     maxAdminSplitAttempts,
	}
	var err error
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		if err = db.AdminSplit(ctx, key, expiration); err == nil {
			return nil
		}
	}
	return errors.Wrapf(err, "retries exhausted splitting at %s", key)
}

// coarsePreSplitAndScatter splits the restore's target key space at the given
// keys and scatters every range that creates, before the split-and-scatter flow
// runs and before any data is ingested.
//
// The flow splits and scatters one range at a time as it ingests, so a scatter
// that declines to move anything leaves that range on the replicas it inherited
// from the range it split out of, and a long run of consecutive ranges piles
// onto the stores that held the original range. Scattering a few large pieces
// while the key space is still empty bounds that run: the ranges the flow later
// splits out of a piece inherit its placement, so the worst case is one piece's
// worth of data sharing a set of stores rather than the whole restore.
//
// Every key split here is also scattered, since an unscattered piece keeps the
// placement the pass exists to break up. To bound the work, make fewer and
// larger pieces rather than scattering only some of them.
//
// These keys are picked by sampling import span boundaries, not by the
// chunking the split-and-scatter processor does, so they are not generally
// chunk boundaries and the flow will not re-split at exactly these points. It
// does not need to: it splits where it splits, within pieces this pass has
// already placed.
//
// Best effort: failures are logged and the restore proceeds, and the flow's own
// splits and scatters are unaffected either way.
func coarsePreSplitAndScatter(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	dataToRestore restorationData,
	backups []backuppb.BackupManifest,
	splitKeys []roachpb.Key,
) {
	if len(splitKeys) == 0 {
		return
	}
	// The flow does not start until this returns, so the span's duration is also
	// how long the pass delays the restore.
	ctx, sp := tracing.ChildSpan(ctx, "restore.coarsePreSplitAndScatter")
	defer sp.Finish()

	kr, err := MakeKeyRewriterFromRekeys(execCfg.Codec, dataToRestore.getRekeys(),
		dataToRestore.getTenantRekeys(), false /* restoreTenantFromStream */)
	if err != nil {
		log.Dev.Warningf(ctx, "skipping coarse pre-split: %v", err)
		return
	}
	// Matches the flow's own choice of split key; see the comment on its
	// derivation in runGenerativeSplitAndScatter.
	legacyEnsureSplitKey := len(backups) == 0 ||
		!backups[0].ClusterVersion.AtLeast(roachpb.Version{Major: 24, Minor: 3})

	// Rewrite into the target key space once here: a KeyRewriter carries mutable
	// state and cannot be shared by the workers below.
	targetKeys := make([]roachpb.Key, 0, len(splitKeys))
	for _, key := range splitKeys {
		targetKey, err := rewriteBackupSpanKey(execCfg.Codec, kr, key)
		if err != nil {
			log.Dev.Warningf(ctx, "coarse pre-split skipping %s: %v", key, err)
			continue
		}
		if legacyEnsureSplitKey {
			if safeKey, err := keys.EnsureSafeSplitKey(targetKey); err == nil && len(safeKey) != 0 {
				targetKey = safeKey
			}
		}
		targetKeys = append(targetKeys, targetKey)
	}

	// Split everything before scattering anything: scatter moves a replica only
	// once the allocator considers a store overfull, and creating all the ranges
	// up front, all on the origin stores, is what presents it with an imbalance
	// large enough to act on.
	//
	// The expiration matches the flow's own splits; the flow reaches every piece
	// well inside it.
	expiration := execCfg.DB.Clock().Now().Add(time.Hour.Nanoseconds(), 0)
	splitStart := timeutil.Now()
	forEachCoarseKey(ctx, targetKeys, coarsePreSplitSplitWorkers,
		func(ctx context.Context, key roachpb.Key) {
			if err := coarseSplitWithRetry(ctx, execCfg.DB, key, expiration); err != nil {
				log.Dev.Warningf(ctx, "coarse pre-split failed at %s: %v", key, err)
			}
		})
	splitTook := timeutil.Since(splitStart)

	scatterStart := timeutil.Now()
	forEachCoarseKey(ctx, targetKeys, coarsePreSplitScatterWorkers,
		func(ctx context.Context, key roachpb.Key) {
			// MaxSize of 1 restricts this to ranges that are still empty, as the
			// flow's own scatters do, so a resumed restore does not move real data.
			if _, err := execCfg.DB.AdminScatter(ctx, key, 1 /* maxSize */); err != nil {
				log.Dev.Warningf(ctx, "coarse pre-scatter failed at %s: %v", key, err)
			}
		})

	// ctx carries the span, so this lands in the trace as well as the logs.
	log.Dev.Infof(ctx, "coarse pre-split: split %d keys in %s, scattered them in %s",
		len(targetKeys), splitTook, timeutil.Since(scatterStart))
}
