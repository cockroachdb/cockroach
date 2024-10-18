// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestSnapshotQueue is a datadriven with test data in testdata/snapshot_queue.
func TestSnapshotQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var q *SnapshotQueue
	closeFn := func() {
		if q != nil {
			q.close()
		}
	}
	defer closeFn()

	var tg *testGranter
	var buf builderWithMu
	var wrkMap workMap
	initialTime := timeutil.FromUnixMicros(int64(0))
	registry := metric.NewRegistry()
	metrics := makeSnapshotQueueMetrics(registry)

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "snapshot_queue"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				closeFn()
				tg = &testGranter{gk: token, buf: &buf}
				q = makeSnapshotQueue(tg, metrics)
				q.ts = timeutil.NewManualTime(initialTime)
				tg.r = q
				wrkMap.resetMap()
				return ""

			case "admit":
				var id int
				var createTime int
				var count int
				d.ScanArgs(t, "id", &id)
				if _, ok := wrkMap.get(id); ok {
					panic(fmt.Sprintf("id %d is already used", id))
				}
				d.ScanArgs(t, "count", &count)
				d.ScanArgs(t, "create-time-millis", &createTime)
				q.ts.(*timeutil.ManualTime).AdvanceTo(timeutil.FromUnixNanos(int64(createTime) * time.Millisecond.Nanoseconds()))
				ctx, cancel := context.WithCancel(context.Background())
				wrkMap.set(id, &testWork{cancel: cancel})
				go func(ctx context.Context, id int, count int) {
					err := q.Admit(ctx, int64(count))
					if err != nil {
						buf.printf("id %d: admit failed", id)
						wrkMap.delete(id)
					} else {
						buf.printf("id %d: admit succeeded", id)
						wrkMap.setAdmitted(id, StoreWorkHandle{})
					}
				}(ctx, id, count)
				// Need deterministic output, and this is racing with the goroutine
				// which is trying to get admitted. Retry to let it get scheduled.
				maybeRetryWithWait(t, d.Expected, d.Rewrite, buf.String)
				return buf.stringAndReset()

			case "set-try-get-return-value":
				var v bool
				d.ScanArgs(t, "v", &v)
				tg.returnValueFromTryGet = v
				return ""

			case "granted":
				rv := tg.r.granted(noGrantChain)
				if rv > 0 {
					// Need deterministic output, and this is racing with the goroutine that was
					// admitted. Retry a few times.
					maybeRetryWithWait(t, d.Expected, d.Rewrite, buf.String)
				}
				tg.buf.printf("granted: returned %d", rv)
				return buf.stringAndReset()

			case "cancel-work":
				var id int
				d.ScanArgs(t, "id", &id)
				work, ok := wrkMap.get(id)
				if !ok {
					return fmt.Sprintf("unknown id: %d", id)
				}
				if work.admitted {
					return fmt.Sprintf("work already admitted id: %d", id)
				}
				work.cancel()
				// Need deterministic output, and this is racing with the goroutine
				// whose work is canceled. Retry to let it get scheduled.
				maybeRetryWithWait(t, d.Expected, d.Rewrite, buf.String)
				return buf.stringAndReset()

			case "empty":
				// Need deterministic output, and this is racing with the goroutine
				// whose work is canceled. Retry to let it get scheduled.
				maybeRetryWithWait(t, d.Expected, d.Rewrite, func() string {
					return strconv.FormatBool(q.empty())
				})
				return strconv.FormatBool(q.empty())

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestSnapshotPacer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var pacer *SnapshotPacer = nil
	// Should not panic on nil pacer.
	require.NoError(t, pacer.Pace(ctx, 1, false))

	q := &testingSnapshotQueue{}
	pacer = NewSnapshotPacer(q)

	// Should not ask for admission since write bytes = burst size.
	writeBytes := int64(SnapshotBurstSize)
	require.NoError(t, pacer.Pace(ctx, writeBytes, false))
	require.False(t, q.admitted)
	require.Equal(t, writeBytes, pacer.intWriteBytes)
	require.Equal(t, int64(0), q.admitCount)

	// Do another write, should go over threshold and seek admission.
	require.NoError(t, pacer.Pace(ctx, 1, false))
	require.True(t, q.admitted)
	require.Equal(t, int64(0), pacer.intWriteBytes)
	require.Equal(t, writeBytes+1, q.admitCount)

	// Not enough bytes since last admission. Should not ask for admission.
	q.admitted = false
	q.admitCount = 0
	require.NoError(t, pacer.Pace(ctx, 5, false))
	require.False(t, q.admitted)
	require.Equal(t, int64(5), pacer.intWriteBytes)
	require.Equal(t, int64(0), q.admitCount)

	// We now go above the threshold again. Should ask for admission.
	require.NoError(t, pacer.Pace(ctx, writeBytes, false))
	require.True(t, q.admitted)
	require.Equal(t, writeBytes+5, q.admitCount)
	require.Equal(t, int64(0), pacer.intWriteBytes)

	// Do few more writes.
	q.admitted = false
	q.admitCount = 0
	require.NoError(t, pacer.Pace(ctx, 10, false))
	require.False(t, q.admitted)
	require.Equal(t, int64(10), pacer.intWriteBytes)
	require.Equal(t, int64(0), q.admitCount)

	// If final call to pacer, we should admit regardless of size. It should flush
	// all intWriteBytes.
	require.NoError(t, pacer.Pace(ctx, -1, true))
	require.True(t, q.admitted)
	require.Equal(t, int64(9), q.admitCount)
}

// testingSnapshotQueue is used to test SnapshotPacer.
type testingSnapshotQueue struct {
	admitted   bool
	admitCount int64
}

var _ snapshotRequester = &testingSnapshotQueue{}

func (ts *testingSnapshotQueue) Admit(ctx context.Context, count int64) error {
	ts.admitted = true
	ts.admitCount = count
	return nil
}
