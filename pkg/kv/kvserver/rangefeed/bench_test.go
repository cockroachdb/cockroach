// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/future"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

type benchmarkRangefeedOpts struct {
	opType           opType
	numRegistrations int
	budget           int64
}

type opType string

const (
	writeOpType    opType = "write"  // individual 1PC writes
	commitOpType   opType = "commit" // intent + commit writes, 1 per txn
	closedTSOpType opType = "closedts"
)

// BenchmarkRangefeed benchmarks the processor and registrations, by submitting
// a set of events and waiting until they are all emitted.
func BenchmarkRangefeed(b *testing.B) {
	for _, opType := range []opType{writeOpType, commitOpType, closedTSOpType} {
		for _, numRegistrations := range []int{1, 10, 100} {
			name := fmt.Sprintf("opType=%s/numRegs=%d", opType, numRegistrations)
			b.Run(name, func(b *testing.B) {
				runBenchmarkRangefeed(b, benchmarkRangefeedOpts{
					opType:           opType,
					numRegistrations: numRegistrations,
					budget:           math.MaxInt64,
				})
			})
		}
	}
}

// BenchmarkRangefeedBudget benchmarks the effect of enabling/disabling the
// processor budget. It sets up a single processor and registration, and
// processes a set of events.
func BenchmarkRangefeedBudget(b *testing.B) {
	for _, budget := range []bool{false, true} {
		b.Run(fmt.Sprintf("budget=%t", budget), func(b *testing.B) {
			var budgetSize int64
			if budget {
				budgetSize = math.MaxInt64
			}
			runBenchmarkRangefeed(b, benchmarkRangefeedOpts{
				opType:           writeOpType,
				numRegistrations: 1,
				budget:           budgetSize,
			})
		})
	}
}

// runBenchmarkRangefeed runs a rangefeed benchmark, emitting b.N events across
// a rangefeed.
func runBenchmarkRangefeed(b *testing.B, opts benchmarkRangefeedOpts) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	var budget *FeedBudget
	if opts.budget > 0 {
		budget = newTestBudget(opts.budget)
	}
	span := roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("z")}

	// Set up processor.
	p := NewProcessor(Config{
		AmbientContext:   log.MakeTestingAmbientContext(nil),
		Clock:            hlc.NewClockForTesting(nil),
		Metrics:          NewMetrics(),
		Span:             span,
		MemBudget:        budget,
		EventChanCap:     b.N,
		EventChanTimeout: time.Hour,
	})
	require.NoError(b, p.Start(stopper, nil))

	// Add registrations.
	streams := make([]*noopStream, opts.numRegistrations)
	futures := make([]*future.ErrorFuture, opts.numRegistrations)
	for i := 0; i < opts.numRegistrations; i++ {
		// withDiff does not matter for these benchmarks, since the previous value
		// is fetched and populated during Raft application.
		const withDiff = false
		streams[i] = &noopStream{ctx: ctx}
		futures[i] = &future.ErrorFuture{}
		ok, _ := p.Register(span, hlc.MinTimestamp, nil, withDiff, streams[i], nil, futures[i])
		require.True(b, ok)
	}

	// Construct b.N events beforehand -- we don't want to measure this cost.
	var (
		logicalOps       []enginepb.MVCCLogicalOp
		closedTimestamps []hlc.Timestamp
		prefix           = roachpb.Key("key-")
		value            = roachpb.MakeValueFromString("a few bytes of data").RawBytes
	)
	switch opts.opType {
	case writeOpType:
		logicalOps = make([]enginepb.MVCCLogicalOp, 0, b.N)
		for i := 0; i < b.N; i++ {
			key := append(prefix, make([]byte, 4)...)
			binary.BigEndian.PutUint32(key[len(prefix):], uint32(i))
			ts := hlc.Timestamp{WallTime: int64(i + 1)}
			logicalOps = append(logicalOps, makeLogicalOp(&enginepb.MVCCWriteValueOp{
				Key:       key,
				Timestamp: ts,
				Value:     value,
			}))
		}

	case commitOpType:
		logicalOps = make([]enginepb.MVCCLogicalOp, 2*b.N)
		// Write all intents first, then all commits. Txns are tracked in an
		// internal heap, and we want to cover some of this cost, even though we
		// write and commit them incrementally.
		for i := 0; i < b.N; i++ {
			var txnID uuid.UUID
			txnID.DeterministicV4(uint64(i), uint64(b.N))
			key := append(prefix, make([]byte, 4)...)
			binary.BigEndian.PutUint32(key[len(prefix):], uint32(i))
			ts := hlc.Timestamp{WallTime: int64(i + 1)}
			logicalOps[i] = writeIntentOpWithKey(txnID, key, isolation.Serializable, ts)
			logicalOps[b.N+i] = commitIntentOpWithKV(txnID, key, ts, value)
		}

	case closedTSOpType:
		closedTimestamps = make([]hlc.Timestamp, 0, b.N)
		for i := 0; i < b.N; i++ {
			closedTimestamps = append(closedTimestamps, hlc.Timestamp{WallTime: int64(i + 1)})
		}

	default:
		b.Fatalf("unknown operation type %q", opts.opType)
	}

	// Wait for catchup scans and flush checkpoint events.
	syncEventAndRegistrations(p)

	// Run the benchmark. We accounted for b.N when constructing events.
	b.ResetTimer()

	for _, logicalOp := range logicalOps {
		if !p.ConsumeLogicalOps(ctx, logicalOp) {
			b.Fatal("failed to submit logical operation")
		}
	}
	for _, closedTS := range closedTimestamps {
		if !p.ForwardClosedTS(ctx, closedTS) {
			b.Fatal("failed to forward closed timestamp")
		}
	}
	syncEventAndRegistrations(p)

	// Check that all registrations ended successfully, and emitted the expected
	// number of events.
	b.StopTimer()
	p.Stop()

	for i, f := range futures {
		regErr, err := future.Wait(ctx, f)
		require.NoError(b, err)
		require.NoError(b, regErr)
		require.Equal(b, b.N, streams[i].events-1) // ignore checkpoint after catchup
	}
}

// noopStream is a stream that does nothing, except count events.
type noopStream struct {
	ctx    context.Context
	events int
}

func (s *noopStream) Context() context.Context {
	return s.ctx
}

func (s *noopStream) Send(*kvpb.RangeFeedEvent) error {
	s.events++
	return nil
}
