// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execinfra

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// We ignore any limits that are higher than this value to avoid integer
// overflows. See limitHint for how this bound is used.
const readerOverflowProtection = 1000000000000000 /* 10^15 */

// LimitHint returns the limit hint to set for a KVFetcher based on
// the spec's limit hint and the PostProcessSpec.
func LimitHint(specLimitHint int64, post *execinfrapb.PostProcessSpec) (limitHint int64) {
	// We prioritize the post process's limit since ProcOutputHelper
	// will tell us to stop once we emit enough rows.
	if post.Limit != 0 && post.Limit+post.Offset <= readerOverflowProtection && post.Limit+post.Offset > 0 {
		limitHint = int64(post.Limit + post.Offset)
	} else if specLimitHint != 0 && specLimitHint <= readerOverflowProtection {
		limitHint = specLimitHint
	}

	return limitHint
}

// MisplannedRanges queries the range cache for all the passed-in spans and
// returns the list of ranges whose leaseholder is not on the indicated node.
// Ranges with unknown leases are not included in the result.
func MisplannedRanges(
	ctx context.Context, spans []roachpb.Span, nodeID roachpb.NodeID, rdc *rangecache.RangeCache,
) (misplannedRanges []roachpb.RangeInfo) {
	log.VEvent(ctx, 2, "checking range cache to see if range info updates should be communicated to the gateway")
	var misplanned map[roachpb.RangeID]struct{}
	for _, sp := range spans {
		rSpan, err := keys.SpanAddr(sp)
		if err != nil {
			panic(err)
		}
		if rSpan.EndKey == nil {
			// GetCachedOverlapping simply ignores spans if EndKey is not set.
			// Here we have a point span, so we explicitly set the EndKey
			// accordingly.
			rSpan.EndKey = rSpan.Key.Next()
		}
		overlapping := rdc.GetCachedOverlapping(ctx, rSpan)

		for _, ri := range overlapping {
			if _, ok := misplanned[ri.Desc.RangeID]; ok {
				// We're already returning info about this range.
				continue
			}
			// Ranges with unknown leases are not returned, as the current node might
			// actually have the lease without the local cache knowing about it.
			l := ri.Lease
			if !l.Empty() && l.Replica.NodeID != nodeID {
				misplannedRanges = append(misplannedRanges, ri)

				if misplanned == nil {
					misplanned = make(map[roachpb.RangeID]struct{})
				}
				misplanned[ri.Desc.RangeID] = struct{}{}
			}
		}
	}

	if len(misplannedRanges) != 0 && log.ExpensiveLogEnabled(ctx, 2) {
		var b strings.Builder
		for i := range misplannedRanges {
			if i > 0 {
				b.WriteString(", ")
			}
			if i > 3 {
				b.WriteString("...")
				break
			}
			fmt.Fprintf(&b, "%+v", misplannedRanges[i])
		}
		log.VEventf(ctx, 2, "%d misplanned ranges: %s", len(misplannedRanges), b.String())
	}

	return misplannedRanges
}

// SpansWithCopy tracks a set of spans (which can be modified) along with the
// copy of the original one if needed.
// NB: Spans field is **not** owned by SpansWithCopy (it comes from the
// TableReader spec).
type SpansWithCopy struct {
	Spans     roachpb.Spans
	SpansCopy roachpb.Spans
}

// MakeSpansCopy makes a copy of s.Spans (which are assumed to have already been
// set).
func (s *SpansWithCopy) MakeSpansCopy() {
	if cap(s.SpansCopy) >= len(s.Spans) {
		s.SpansCopy = s.SpansCopy[:len(s.Spans)]
	} else {
		s.SpansCopy = make(roachpb.Spans, len(s.Spans))
	}
	copy(s.SpansCopy, s.Spans)
}

// Reset deeply resets s.
func (s *SpansWithCopy) Reset() {
	for i := range s.SpansCopy {
		s.SpansCopy[i] = roachpb.Span{}
	}
	s.Spans = nil
	s.SpansCopy = s.SpansCopy[:0]
}

// limitHintBatchCount tracks how many times the caller has read LimitHint()
// number of rows.
type limitHintBatchCount int

const (
	limitHintFirstBatch limitHintBatchCount = iota
	limitHintSecondBatch
	limitHintDisabled
)

// limitHintSecondBatchFactor is a multiple used when determining the limit hint
// for the second batch of rows. This will be used when the original limit hint
// turned out to be insufficient to satisfy the query.
const limitHintSecondBatchFactor = 10

// LimitHintHelper is used for lookup and index joins in order to limit batches
// of input rows in the presence of hard and soft limits.
type LimitHintHelper struct {
	origLimitHint int64
	// currentLimitHint of zero indicates that the limit hint is disabled.
	currentLimitHint int64
	limitHintIdx     limitHintBatchCount
}

// MakeLimitHintHelper creates a new LimitHintHelper.
func MakeLimitHintHelper(specLimitHint int64, post *execinfrapb.PostProcessSpec) LimitHintHelper {
	limitHint := LimitHint(specLimitHint, post)
	return LimitHintHelper{
		origLimitHint:    limitHint,
		currentLimitHint: limitHint,
		limitHintIdx:     limitHintFirstBatch,
	}
}

// LimitHint returns the current guess on the remaining rows that need to be
// read. Zero is returned when the limit hint is disabled.
func (h *LimitHintHelper) LimitHint() int64 {
	return h.currentLimitHint
}

// ReadSomeRows notifies the helper that its user has fetched the specified
// number of rows. An error is returned when the user fetched more rows than the
// current limit hint.
func (h *LimitHintHelper) ReadSomeRows(rowsRead int64) error {
	if h.currentLimitHint != 0 {
		h.currentLimitHint -= rowsRead
		if h.currentLimitHint == 0 {
			// Set up the limit hint for the next batch of input rows if the
			// current batch turns out to be insufficient.
			//
			// If we just finished the first batch of rows, then use the
			// original limit hint times limitHintSecondBatchFactor. If we
			// finished the second or any of the following batches, then we keep
			// the limit hint as zero (i.e. disabled) since it appears that our
			// original hint was either way off or many input rows result in
			// lookup misses.
			switch h.limitHintIdx {
			case limitHintFirstBatch:
				h.currentLimitHint = limitHintSecondBatchFactor * h.origLimitHint
				h.limitHintIdx = limitHintSecondBatch
			default:
				h.currentLimitHint = 0
				h.limitHintIdx = limitHintDisabled
			}
		} else if h.currentLimitHint < 0 {
			return errors.AssertionFailedf(
				"unexpectedly the user of LimitHintHelper read " +
					"more rows that the current limit hint",
			)
		}
	}
	return nil
}

// UseStreamer returns whether the kvstreamer.Streamer API should be used as
// well as the txn that should be used (regardless of the boolean return value).
func (flowCtx *FlowCtx) UseStreamer(ctx context.Context) (bool, *kv.Txn, error) {
	useStreamer := flowCtx.EvalCtx.SessionData().StreamerEnabled && flowCtx.Txn != nil &&
		flowCtx.Txn.Type() == kv.LeafTxn && flowCtx.MakeLeafTxn != nil
	if !useStreamer {
		return false, flowCtx.Txn, nil
	}
	leafTxn, err := flowCtx.MakeLeafTxn(ctx)
	if leafTxn == nil || err != nil {
		// leafTxn might be nil in some flows which run outside of the txn, the
		// streamer should not be used in such cases.
		return false, flowCtx.Txn, err
	}
	return true, leafTxn, nil
}

// UseStreamerEnabled determines the default value for the 'streamer_enabled'
// session variable.
// TODO(yuzefovich): consider removing this at some point.
var UseStreamerEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.distsql.use_streamer.enabled",
	"determines whether the usage of the Streamer API is allowed. "+
		"Enabling this will increase the speed of lookup/index joins "+
		"while adhering to memory limits.",
	true,
)

// This number was chosen by running TPCH queries 3, 4, 5, 9, and 19 with
// varying batch sizes and choosing the smallest batch size that offered a
// significant performance improvement. Larger batch sizes offered small to no
// marginal improvements.
const joinReaderIndexJoinStrategyBatchSizeDefault = 4 << 20 /* 4 MiB */

// JoinReaderIndexJoinStrategyBatchSize determines the size of input batches
// used to construct a single lookup KV batch by
// rowexec.joinReaderIndexJoinStrategy as well as colfetcher.ColIndexJoin.
var JoinReaderIndexJoinStrategyBatchSize = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"sql.distsql.join_reader_index_join_strategy.batch_size",
	"size limit on the input rows to construct a single lookup KV batch "+
		"(by the joinReader processor and the ColIndexJoin operator (when the "+
		"latter doesn't use the Streamer API))",
	joinReaderIndexJoinStrategyBatchSizeDefault,
	settings.PositiveInt,
)

// GetIndexJoinBatchSize returns the lookup rows batch size hint for the index
// joins.
func GetIndexJoinBatchSize(sd *sessiondata.SessionData) int64 {
	if sd.JoinReaderIndexJoinStrategyBatchSize == 0 {
		// In some tests the session data might not be set - use the default
		// value then.
		return joinReaderIndexJoinStrategyBatchSizeDefault
	}
	return sd.JoinReaderIndexJoinStrategyBatchSize
}
