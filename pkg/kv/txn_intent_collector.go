// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package kv

import (
	"context"
	"sort"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// maxTxnIntentsBytes is a threshold in bytes for intent spans stored
// on the coordinator during the lifetime of a transaction. Intents
// are included with a transaction on commit or abort, to be cleaned
// up asynchronously. If they exceed this threshold, they're condensed
// to avoid memory blowup both on the coordinator and (critically) on
// the EndTransaction command at the Raft group responsible for the
// transaction record.
var maxTxnIntentsBytes = settings.RegisterIntSetting(
	"kv.transaction.max_intents_bytes",
	"maximum number of bytes used to track write intents in transactions",
	256*1000,
)

// txnIntentCollector is an implementation of txnReqInterceptor. It collects
// write intent spans from transactional requests and attaches them to
// EndTransaction requests to ensure that they are resolved after the
// transaction completes.
type txnIntentCollector struct {
	st *cluster.Settings
	ri *RangeIterator

	// meta contains all txn intents.
	meta *roachpb.TxnCoordMeta
	// intentsSizeBytes is the size in bytes of the intent spans in the
	// meta, maintained to efficiently check the threshold.
	intentsSizeBytes int64
}

var _ txnReqInterceptor = &txnIntentCollector{}

func (ic *txnIntentCollector) beforeSendLocked(
	ctx context.Context, ba roachpb.BatchRequest,
) (roachpb.BatchRequest, *roachpb.Error) {
	rArgs, hasET := ba.GetArg(roachpb.EndTransaction)
	if !hasET {
		// No-op.
		return ba, nil
	}

	et := rArgs.(*roachpb.EndTransactionRequest)
	if len(et.IntentSpans) > 0 {
		// TODO(tschottdorf): it may be useful to allow this later.
		// That would be part of a possible plan to allow txns which
		// write on multiple coordinators.
		return ba, roachpb.NewErrorf("client must not pass intents to EndTransaction")
	}
	if len(et.Key) != 0 {
		return ba, roachpb.NewErrorf("EndTransaction must not have a Key set")
	}
	et.Key = ba.Txn.Key

	// Defensively set distinctSpans to false if we had any previous
	// writes in this transaction. This effectively limits the distinct
	// spans optimization to 1pc transactions.
	distinctSpans := len(ic.meta.Intents) == 0

	// We can't pass in a batch response here to better limit the key
	// spans as we don't know what is going to be affected. This will
	// affect queries such as `DELETE FROM my.table LIMIT 10` when
	// executed as a 1PC transaction. e.g.: a (BeginTransaction,
	// DeleteRange, EndTransaction) batch.
	if err := ic.appendAndCondenseIntentsLocked(ctx, ba, nil); err != nil {
		return ba, roachpb.NewError(err)
	}

	// Populate et.IntentSpans, taking into account both any existing
	// and new writes, and taking care to perform proper deduplication.
	et.IntentSpans = append([]roachpb.Span(nil), ic.meta.Intents...)
	// TODO(peter): Populate DistinctSpans on all batches, not just batches
	// which contain an EndTransactionRequest.
	var distinct bool
	et.IntentSpans, distinct = roachpb.MergeSpans(et.IntentSpans)
	ba.Header.DistinctSpans = distinct && distinctSpans

	if len(et.IntentSpans) == 0 {
		// If there aren't any intents, then there's factually no
		// transaction to end. Read-only txns have all of their state
		// in the client.
		return ba, roachpb.NewErrorf("cannot commit a read-only transaction")
	}
	if log.V(3) {
		for _, intent := range et.IntentSpans {
			log.Infof(ctx, "intent: [%s,%s)", intent.Key, intent.EndKey)
		}
	}

	return ba, nil
}

func (*txnIntentCollector) maybeRetrySend(
	ctx context.Context, ba *roachpb.BatchRequest, br *roachpb.BatchResponse, pErr *roachpb.Error,
) (*roachpb.BatchResponse, *roachpb.Error) {
	// No-op.
	return br, pErr
}

func (ic *txnIntentCollector) afterSendLocked(
	ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse, pErr *roachpb.Error,
) (*roachpb.BatchResponse, *roachpb.Error) {
	// Append the intents from the current request to our txn meta.
	if err := ic.appendAndCondenseIntentsLocked(ctx, ba, br); err != nil {
		log.VEventf(ctx, 2, "failed to condense intent spans (%s); skipping", err)
	}
	return br, pErr
}

func (ic *txnIntentCollector) refreshMetaLocked() {
	ic.intentsSizeBytes = 0
	for _, i := range ic.meta.Intents {
		ic.intentsSizeBytes += int64(len(i.Key) + len(i.EndKey))
	}
}

func (ic *txnIntentCollector) appendAndCondenseIntentsLocked(
	ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse,
) error {
	// Adding the intents even on error reduces the likelihood of dangling
	// intents blocking concurrent writers for extended periods of time. See
	// #3346.
	ba.IntentSpanIterate(br, func(span roachpb.Span) {
		ic.meta.Intents = append(ic.meta.Intents, span)
		ic.intentsSizeBytes += int64(len(span.Key) + len(span.EndKey))
	})
	condensedIntents, condensedIntentsSize, err := ic.maybeCondenseIntentSpans(
		ctx, ic.meta.Intents, ic.intentsSizeBytes,
	)
	if err != nil {
		return err
	}
	ic.meta.Intents, ic.intentsSizeBytes = condensedIntents, condensedIntentsSize
	return nil
}

type spanBucket struct {
	rangeID roachpb.RangeID
	size    int64
	spans   []roachpb.Span
}

// maybeCondenseIntentSpans avoids sending massive EndTransaction
// requests which can consume excessive memory at evaluation time and
// in the txn intent collector itself. Spans are condensed based on
// current range boundaries. Returns the condensed set of spans and
// the new total spans size. Note that errors can be returned if the
// range iterator fails.
func (ic *txnIntentCollector) maybeCondenseIntentSpans(
	ctx context.Context, spans []roachpb.Span, spansSize int64,
) ([]roachpb.Span, int64, error) {
	ri := ic.ri
	if ri == nil {
		// If the intent collector was not given a RangeIterator, it cannot
		// condense intent spans.
		return spans, spansSize, nil
	}
	defer ri.Reset()

	maxBytes := maxTxnIntentsBytes.Get(&ic.st.SV)
	if spansSize < maxBytes {
		return spans, spansSize, nil
	}

	// Sort the spans by start key.
	sort.Slice(spans, func(i, j int) bool { return spans[i].Key.Compare(spans[j].Key) < 0 })

	// Divide them by range boundaries and condense. Iterate over spans
	// using a range iterator and add each to a bucket keyed by range
	// ID. Local keys are kept in a new slice and not added to buckets.
	buckets := []*spanBucket{}
	localSpans := []roachpb.Span{}
	for _, s := range spans {
		if keys.IsLocal(s.Key) {
			localSpans = append(localSpans, s)
			continue
		}
		ri.Seek(ctx, roachpb.RKey(s.Key), Ascending)
		if !ri.Valid() {
			return nil, 0, ri.Error().GoError()
		}
		rangeID := ri.Desc().RangeID
		if l := len(buckets); l > 0 && buckets[l-1].rangeID == rangeID {
			buckets[l-1].spans = append(buckets[l-1].spans, s)
		} else {
			buckets = append(buckets, &spanBucket{rangeID: rangeID, spans: []roachpb.Span{s}})
		}
		buckets[len(buckets)-1].size += int64(len(s.Key) + len(s.EndKey))
	}

	// Sort the buckets by size and collapse from largest to smallest
	// until total size of uncondensed spans no longer exceeds threshold.
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].size > buckets[j].size })
	spans = localSpans // reset to hold just the local spans; will add newly condensed and remainder
	for _, bucket := range buckets {
		// Condense until we get to half the threshold.
		if spansSize <= maxBytes/2 {
			// Collect remaining spans from each bucket into uncondensed slice.
			spans = append(spans, bucket.spans...)
			continue
		}
		spansSize -= bucket.size
		// TODO(spencer): consider further optimizations here to create
		// more than one span out of a bucket to avoid overly broad span
		// combinations.
		cs := bucket.spans[0]
		for _, s := range bucket.spans[1:] {
			cs = cs.Combine(s)
			if !cs.Valid() {
				return nil, 0, errors.Errorf("combining span %s yielded invalid result", s)
			}
		}
		spansSize += int64(len(cs.Key) + len(cs.EndKey))
		spans = append(spans, cs)
	}

	return spans, spansSize, nil
}
