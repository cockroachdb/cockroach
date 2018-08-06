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

package rangefeed

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// A runnable can be run as an async task.
type runnable interface {
	// Run executes the runnable. Cannot be called multiple times.
	Run(context.Context)
	// Must be called if runnable is not Run.
	Cancel()
}

// initResolvedTSScan scans over all keys using the provided iterator and
// informs the rangefeed Processor of any intents. This allows the Processor to
// backfill its unresolvedIntentQueue with any intents that were written before
// the Processor was started and hooked up to a stream of logical operations.
// The Processor can initialize its resolvedTimestamp once the scan completes
// because it knows it is now tracking all intents in its key range.
//
// Iterator Contract:
//   The provided Iterator must observe all intents in the Processor's keyspan.
//   An important implication of this is that if the iterator is a
//   TimeBoundIterator, its MinTimestamp cannot be above the keyspan's largest
//   known resolved timestamp, if one has ever been recorded. If one has never
//   been recorded, the TimeBoundIterator cannot have any lower bound.
//
type initResolvedTSScan struct {
	p  *Processor
	it engine.SimpleIterator
}

func makeInitResolvedTSScan(p *Processor, it engine.SimpleIterator) runnable {
	return &initResolvedTSScan{p: p, it: it}
}

func (s *initResolvedTSScan) Run(ctx context.Context) {
	defer s.it.Close()
	if err := s.iterateAndConsume(ctx); err != nil {
		err = errors.Wrap(err, "initial resolved timestamp scan failed")
		log.Error(ctx, err)
		s.p.StopWithErr(roachpb.NewError(err))
	} else {
		// Inform the processor that its resolved timestamp can be initialized.
		s.p.setResolvedTSInitialized()
	}
}

func (s *initResolvedTSScan) iterateAndConsume(ctx context.Context) error {
	startKey := engine.MakeMVCCMetadataKey(s.p.Span.Key.AsRawKey())
	endKey := engine.MakeMVCCMetadataKey(s.p.Span.EndKey.AsRawKey())

	// Iterate through all keys using NextKey. This will look at the first MVCC
	// version for each key. We're only looking for MVCCMetadata versions, which
	// will always be the first version of a key if it exists, so its fine that
	// we skip over all other versions of keys.
	var meta enginepb.MVCCMetadata
	for s.it.Seek(startKey); ; s.it.NextKey() {
		if ok, err := s.it.Valid(); err != nil {
			return err
		} else if !ok || !s.it.UnsafeKey().Less(endKey) {
			break
		}

		// If the key is not a metadata key, ignore it.
		unsafeKey := s.it.UnsafeKey()
		if unsafeKey.IsValue() {
			continue
		}

		// Found a metadata key. Unmarshal.
		if err := protoutil.Unmarshal(s.it.UnsafeValue(), &meta); err != nil {
			return errors.Wrapf(err, "unmarshaling mvcc meta: %v", unsafeKey)
		}

		// If this is an intent, inform the Processor.
		if meta.Txn != nil {
			var op enginepb.MVCCLogicalOp
			op.SetValue(&enginepb.MVCCWriteIntentOp{
				TxnID:     meta.Txn.ID,
				TxnKey:    meta.Txn.Key,
				Timestamp: meta.Txn.Timestamp,
			})
			s.p.ConsumeLogicalOps(op)
		}
	}
	return nil
}

func (s *initResolvedTSScan) Cancel() {
	s.it.Close()
}

// catchUpScan scans over the provided iterator and publishes committed values
// to the registration's stream. This backfill allows a registration to request
// a starting timestamp in the past and observe events for writes that have
// already happened.
//
// Iterator Contract:
//   Committed values beneath the registration's starting timestamp will be
//   ignored, but all values above the registration's starting timestamp must be
//   present. An important implication of this is that if the iterator is a
//   TimeBoundIterator, its MinTimestamp cannot be above the registration's
//   starting timestamp.
//
type catchUpScan struct {
	p  *Processor
	r  *registration
	it engine.SimpleIterator
}

func makeCatchUpScan(p *Processor, r *registration) runnable {
	s := catchUpScan{p: p, r: r, it: r.catchUpIter}
	r.catchUpIter = nil // detach
	return &s
}

func (s *catchUpScan) Run(ctx context.Context) {
	defer s.it.Close()
	if err := s.iterateAndSend(ctx); err != nil {
		err = errors.Wrap(err, "catch-up scan failed")
		log.Error(ctx, err)
		s.p.deliverCatchUpScanRes(s.r, roachpb.NewError(err))
	} else {
		s.p.deliverCatchUpScanRes(s.r, nil)
	}
}

func (s *catchUpScan) iterateAndSend(ctx context.Context) error {
	startKey := engine.MakeMVCCMetadataKey(s.r.span.Key)
	endKey := engine.MakeMVCCMetadataKey(s.r.span.EndKey)

	// Iterate though all keys using Next. We want to publish all committed
	// versions of each key that are after the registration's startTS, so we
	// can't use NextKey.
	var meta enginepb.MVCCMetadata
	for s.it.Seek(startKey); ; s.it.Next() {
		if ok, err := s.it.Valid(); err != nil {
			return err
		} else if !ok || !s.it.UnsafeKey().Less(endKey) {
			break
		}

		unsafeKey := s.it.UnsafeKey()
		unsafeVal := s.it.UnsafeValue()
		if !unsafeKey.IsValue() {
			// Found a metadata key.
			if err := protoutil.Unmarshal(unsafeVal, &meta); err != nil {
				return errors.Wrapf(err, "unmarshaling mvcc meta: %v", unsafeKey)
			}
			if !meta.IsInline() {
				// Not an inline value. Ignore.
				continue
			}

			// If write is inline, it doesn't have a timestamp so we don't
			// filter on the registration's starting timestamp. Instead, we
			// return all inline writes.
			unsafeVal = meta.RawBytes
		} else if !s.r.startTS.Less(unsafeKey.Timestamp) {
			// At or before the registration's exclusive starting timestamp.
			// Ignore.
			continue
		}

		var event roachpb.RangeFeedEvent
		event.MustSetValue(&roachpb.RangeFeedValue{
			Key: unsafeKey.Key,
			Value: roachpb.Value{
				RawBytes:  unsafeVal,
				Timestamp: unsafeKey.Timestamp,
			},
		})
		if err := s.r.stream.Send(&event); err != nil {
			return err
		}
	}
	return nil
}

func (s *catchUpScan) Cancel() {
	s.it.Close()
}
