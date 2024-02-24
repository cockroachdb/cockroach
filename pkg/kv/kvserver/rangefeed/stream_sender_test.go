package rangefeed

import (
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func mkEvent(key string) *kvpb.RangeFeedEvent {
	ev := new(kvpb.RangeFeedEvent)
	ev.MustSetValue(&kvpb.RangeFeedValue{
		Key: roachpb.Key(key), Value: roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}},
	})
	return ev
}

func TestBufferedSender(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng, _ := randutil.NewTestRand()

	numEvents := 100
	expectedEvents := make([]*kvpb.RangeFeedEvent, 0, numEvents)
	for i := 0; i < numEvents; i++ {
		expectedEvents = append(expectedEvents, mkEvent(strconv.Itoa(i)))
	}

	t.Run("send synchronous", func(t *testing.T) {
		ts := newTestStream()
		bs, err := NewBufferedSender(ts.Context(), GenericStream[*kvpb.RangeFeedEvent](ts))
		require.NoError(t, err)
		defer bs.Close()

		for i := 0; i < numEvents; i++ {
			err := bs.Send(expectedEvents[i])
			require.NoError(t, err)
		}
		actualEvents := ts.Events()
		for i := 0; i < numEvents; i++ {
			require.Equal(t, expectedEvents[i].Val.Key, actualEvents[i].Val.Key)
		}
	})

	t.Run("send asynchronous", func(t *testing.T) {
		ts := newTestStream()

		bs, err := NewBufferedSender(ts.Context(), GenericStream[*kvpb.RangeFeedEvent](ts))
		require.NoError(t, err)
		defer bs.Close()

		done := make(chan struct{})
		for i := 0; i < numEvents; i++ {
			callback := func(error) {}
			if i == numEvents-1 {
				callback = func(err error) {
					close(done)
				}
			}
			ev := &REventWithAlloc{
				event:    expectedEvents[i],
				alloc:    nil,
				callback: callback,
			}
			bs.BufferedSend(ev)
		}

		select {
		case <-time.After(30 * time.Second):
			t.Fatal("timed out waiting for flush")
		case <-done:
		}

		actualEvents := ts.Events()
		for i := 0; i < numEvents; i++ {
			require.Equal(t, expectedEvents[i].Val.Key, actualEvents[i].Val.Key)
		}
	})

	t.Run("send mixed", func(t *testing.T) {
		ts := newTestStream()
		bs, err := NewBufferedSender(ts.Context(), GenericStream[*kvpb.RangeFeedEvent](ts))
		require.NoError(t, err)
		defer bs.Close()

		emitted := atomic.Int64{}
		callback := func(error) {
			emitted.Add(1)
		}
		for i := 0; i < numEvents; i++ {
			if rng.Intn(2) == 0 {
				ev := &REventWithAlloc{
					event:    expectedEvents[i],
					alloc:    nil,
					callback: callback,
				}
				bs.BufferedSend(ev)
			} else {
				err := bs.Send(expectedEvents[i])
				require.NoError(t, err)
				emitted.Add(1)
			}
		}

		testutils.SucceedsSoon(t, func() error {
			if emitted.Load() != int64(numEvents) {
				return errors.New("did not emit expected events")
			}
			return nil
		})

		actualEvents := ts.Events()
		// Mixing sync and async sends means that events may be reordered.
		sort.Slice(actualEvents, func(i, j int) bool {
			v1, err := strconv.Atoi(strings.Trim(actualEvents[i].Val.Key.String(), "\""))
			require.NoError(t, err)
			v2, err := strconv.Atoi(strings.Trim(actualEvents[j].Val.Key.String(), "\""))
			require.NoError(t, err)
			return v1 < v2
		})
		for i := 0; i < numEvents; i++ {
			require.Equal(t, expectedEvents[i].Val.Key, actualEvents[i].Val.Key)
		}
	})
}
