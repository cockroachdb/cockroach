// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/intentresolver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type mockRange struct {
	desc         *roachpb.RangeDescriptor
	intents      []roachpb.Intent
	localIntents []roachpb.Intent
}

type mockRangeIterator struct {
	ranges []mockRange
	idx    int
	err    error
}

func (m *mockRangeIterator) Desc() *roachpb.RangeDescriptor {
	if m.idx >= len(m.ranges) || m.idx < 0 {
		return nil
	}
	return m.ranges[m.idx].desc
}

func (m *mockRangeIterator) Error() error {
	return m.err
}

func (m *mockRangeIterator) NeedAnother(rs roachpb.RSpan) bool {
	return true
}

func (m *mockRangeIterator) Next(ctx context.Context) {
	m.idx++
}

func (m *mockRangeIterator) Seek(
	ctx context.Context, key roachpb.RKey, scanDir kvcoord.ScanDirection,
) {
	m.idx = 0
}

func (m *mockRangeIterator) Valid() bool {
	return m.idx < len(m.ranges) && m.idx >= 0
}

type mockIntentResolver struct {
	syncutil.Mutex

	pushedTxns      []uuid.UUID
	resolvedIntents []roachpb.Key
}

func (m *mockIntentResolver) PushTransaction(
	ctx context.Context, pushTxn *enginepb.TxnMeta, h roachpb.Header, pushType roachpb.PushTxnType,
) (*roachpb.Transaction, *roachpb.Error) {
	m.Lock()
	m.pushedTxns = append(m.pushedTxns, pushTxn.ID)
	m.Unlock()
	return &roachpb.Transaction{
		TxnMeta: *pushTxn,
	}, nil
}

func (m *mockIntentResolver) ResolveIntents(
	ctx context.Context, intents []roachpb.LockUpdate, opts intentresolver.ResolveOptions,
) (pErr *roachpb.Error) {
	m.Lock()
	defer m.Unlock()
	for i := range intents {
		keyCopy := append([]byte(nil), intents[i].Key...)
		m.resolvedIntents = append(m.resolvedIntents, keyCopy)
	}
	return nil
}

func parseKey(key string) roachpb.Key {
	if strings.HasPrefix(key, "/tsd/") {
		result := make([]byte, 0, len(key))
		result = append(result, keys.TimeseriesPrefix...)
		result = append(result, []byte(strings.TrimPrefix(key, "/tsd/"))...)
		return result
	}
	return []byte(key)
}

func TestRunSeparatedIntentsMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	ri := &mockRangeIterator{}
	ir := &mockIntentResolver{}
	hlcClock := hlc.NewClock(hlc.UnixNano, base.DefaultMaxClockOffset)
	var numIntentsPerResumeSpan, errorPerNCalls int
	var barrierCalls, scanIntentsCalls int64

	mockSender := kv.MakeMockTxnSenderFactoryWithNonTxnSender(nil,
		func(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			br := ba.CreateReply()
			for i, req := range ba.Requests {
				args := req.GetInner()
				switch request := args.(type) {
				case *roachpb.BarrierRequest:
					newVal := atomic.AddInt64(&barrierCalls, 1)
					if errorPerNCalls > 0 && int(newVal)%errorPerNCalls == 0 {
						return nil, roachpb.NewError(errors.New("injected"))
					}
					br.Responses[i].MustSetInner(&roachpb.BarrierResponse{
						ResponseHeader: roachpb.ResponseHeader{},
						Timestamp:      hlcClock.Now(),
					})
				case *roachpb.ScanInterleavedIntentsRequest:
					newVal := atomic.AddInt64(&scanIntentsCalls, 1)
					if errorPerNCalls > 0 && int(newVal)%errorPerNCalls == 0 {
						return nil, roachpb.NewError(errors.New("injected"))
					}

					var resumeSpan *roachpb.Span
					var r *mockRange
					var intentsToScan []roachpb.Intent
					for i := range ri.ranges {
						if bytes.HasPrefix(request.Key, keys.LocalRangePrefix) {
							rangeKeyRange := roachpb.Span{
								Key:    keys.MakeRangeKeyPrefix(ri.ranges[i].desc.StartKey),
								EndKey: keys.MakeRangeKeyPrefix(ri.ranges[i].desc.EndKey),
							}
							if rangeKeyRange.Contains(request.Span()) {
								r = &ri.ranges[i]
								intentsToScan = r.localIntents
								break
							}
						}
						if ri.ranges[i].desc.ContainsKeyRange(roachpb.RKey(request.Key), roachpb.RKey(request.EndKey)) {
							r = &ri.ranges[i]
							intentsToScan = r.intents
							break
						}
					}
					if r == nil {
						t.Fatal("ScanInterleavedIntents request issued outside range bounds")
					}
					startIdx := sort.Search(len(intentsToScan), func(i int) bool {
						return bytes.Compare(request.Key, intentsToScan[i].Key) <= 0
					})
					endIdx := sort.Search(len(intentsToScan), func(i int) bool {
						return bytes.Compare(request.EndKey, intentsToScan[i].Key) < 0
					})
					if endIdx-startIdx > numIntentsPerResumeSpan && numIntentsPerResumeSpan > 0 {
						endIdx = startIdx + numIntentsPerResumeSpan
						// Set the resume span.
						if endIdx < len(intentsToScan) {
							resumeSpan = &roachpb.Span{
								Key:    intentsToScan[endIdx].Key,
								EndKey: request.EndKey,
							}
						}
					}
					br.Responses[i].MustSetInner(&roachpb.ScanInterleavedIntentsResponse{
						ResponseHeader: roachpb.ResponseHeader{ResumeSpan: resumeSpan},
						Intents:        intentsToScan[startIdx:endIdx],
					})
				default:
					t.Fatalf("unexpected request type: %T", args)
				}
			}
			return br, nil
		})

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	db := kv.NewDB(log.AmbientContext{Tracer: tracing.NewTracer()}, mockSender, hlcClock, stopper)

	datadriven.RunTest(t, "testdata/separated_intents",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "add-range":
				var startKey, endKey string
				var rangeID int
				d.ScanArgs(t, "id", &rangeID)
				d.ScanArgs(t, "key", &startKey)
				d.ScanArgs(t, "endkey", &endKey)

				desc := &roachpb.RangeDescriptor{
					RangeID:  roachpb.RangeID(rangeID),
					StartKey: roachpb.RKey(parseKey(startKey)),
					EndKey:   roachpb.RKey(parseKey(endKey)),
				}
				var intents, localIntents []roachpb.Intent
				localKeys := false
				for _, line := range strings.Split(d.Input, "\n") {
					if line == "local" {
						localKeys = true
						continue
					}
					fields := strings.Fields(line)
					key := parseKey(fields[0])
					txnIDInt, err := strconv.Atoi(fields[1])
					require.NoError(t, err)
					txnID := uuid.FromUint128(uint128.FromInts(0, uint64(txnIDInt)))
					intentKey := key
					if localKeys {
						intentKey = keys.MakeRangeKey(desc.StartKey, keys.LocalQueueLastProcessedSuffix, roachpb.RKey(key))
					}
					intent := roachpb.MakeIntent(&enginepb.TxnMeta{ID: txnID}, intentKey)
					if localKeys {
						localIntents = append(localIntents, intent)
					} else {
						intents = append(intents, intent)
					}
				}
				ri.ranges = append(ri.ranges, mockRange{
					desc:         desc,
					intents:      intents,
					localIntents: localIntents,
				})
			case "set-max-intent-count":
				var err error
				numIntentsPerResumeSpan, err = strconv.Atoi(d.Input)
				require.NoError(t, err)
			case "error-per-n-calls":
				var err error
				errorPerNCalls, err = strconv.Atoi(d.Input)
				require.NoError(t, err)
			case "run-migration":
				err := runSeparatedIntentsMigration(ctx, hlcClock, stopper, db, ri, ir)
				if err == nil {
					return "ok"
				}
				return err.Error()
			case "pushed-txns":
				var builder strings.Builder
				sort.Slice(ir.pushedTxns, func(i, j int) bool {
					return ir.pushedTxns[i].ToUint128().Compare(ir.pushedTxns[j].ToUint128()) < 0
				})
				for i := range ir.pushedTxns {
					fmt.Fprintf(&builder, "%d\n", ir.pushedTxns[i].ToUint128().Lo)
				}
				return builder.String()
			case "resolved-intents":
				var builder strings.Builder
				sort.Slice(ir.resolvedIntents, func(i, j int) bool {
					return ir.resolvedIntents[i].Compare(ir.resolvedIntents[j]) < 0
				})
				for i := range ir.resolvedIntents {
					fmt.Fprintf(&builder, "%s\n", ir.resolvedIntents[i].String())
				}
				return builder.String()
			case "reset":
				ri.ranges = nil
				ri.idx = 0
				*ir = mockIntentResolver{}
				barrierCalls = 0
				scanIntentsCalls = 0
				numIntentsPerResumeSpan = 0
				errorPerNCalls = 0
			case "count-calls":
				return fmt.Sprintf("barrier: %d\nscanInterleavedIntents: %d\n", barrierCalls, scanIntentsCalls)
			default:
				return "unexpected command"
			}
			return ""
		})
}
