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

// A Snapshot is an atomic view of all MVCCKeys within a key range.
type Snapshot interface {
	// Iterate scans from the start key to the end key, invoking the function f
	// on each key value pair at or above the specified timestamp. If f returns
	// an error or if the scan itself encounters an error, the iteration will
	// stop and return the error. If the first result of f is true, the
	// iteration stops and returns a nil error.
	Iterate(start, end roachpb.Key, f func(engine.MVCCKeyValue) (bool, error)) error
	// Close closes the snapshot, freeing up any outstanding resources.
	Close()
}

// A runnable can be run as an async task.
type runnable interface {
	// Run executes the runnable. Cannot be called multiple times.
	Run(context.Context)
	// Must be called if runnable is not Run.
	Cancel()
}

// initResolvedTSScan scans over all keys in the provided Snapshot and informs
// the rangefeed Processor of any intents. This allows the Processor to backfill
// its unresolvedIntentQueue with any intents that were written before the
// Processor was started and hooked up to a stream of logical operations. The
// Processor can initialize its resolvedTimestamp once the scan completes
// because it knows it is now tracking all intents in its key range.
//
// Snapshot Contract:
//   The provided Snapshot must observe all intents in the Processor's keyspan.
//   An important implication of this is that if the Snapshot uses a
//   TimeBoundIterator, its MinTimestamp cannot be above the keyspan's largest
//   known resolved timestamp, if one has ever been recorded.
//
type initResolvedTSScan struct {
	p    *Processor
	snap Snapshot
}

func makeInitResolvedTSScan(p *Processor, snap Snapshot) runnable {
	return &initResolvedTSScan{p: p, snap: snap}
}

func (s *initResolvedTSScan) Run(ctx context.Context) {
	defer s.snap.Close()

	var meta enginepb.MVCCMetadata
	sp := s.p.Span.AsRawSpanWithNoLocals()
	err := s.snap.Iterate(sp.Key, sp.EndKey, func(kv engine.MVCCKeyValue) (bool, error) {
		if !kv.Key.IsValue() {
			// Found a metadata key. Inform the Processor if it's an intent.
			if err := protoutil.Unmarshal(kv.Value, &meta); err != nil {
				return false, errors.Wrapf(err, "unmarshaling mvcc meta: %v", kv)
			}

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
		return false, nil
	})

	if err != nil {
		err = errors.Wrap(err, "initial resolved timestamp scan failed")
		log.Error(ctx, err)
		s.p.StopWithErr(roachpb.NewError(err))
	} else {
		// Inform the processor that its resolved timestamp can be initialized.
		s.p.setResolvedTSInitialized()
	}
}

func (s *initResolvedTSScan) Cancel() {
	s.snap.Close()
}
