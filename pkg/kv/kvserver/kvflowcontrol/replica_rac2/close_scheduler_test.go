// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replica_rac2

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

type testingRaftScheduler struct {
	clock   timeutil.TimeSource
	history []scheduledCloseEvent
}

func (t *testingRaftScheduler) EnqueueRaftReady(id roachpb.RangeID) {
	t.history = append(t.history, scheduledCloseEvent{rangeID: id, at: t.clock.Now()})
}

func (t *testingRaftScheduler) String() string {
	var buf strings.Builder
	buf.WriteString("complete:\n")
	for _, e := range t.history {
		// The history is already sorted by completion time, so we don't need to
		// sort it again here for deterministic output.
		buf.WriteString(fmt.Sprintf("  %vs: range_id=%v\n", e.at.Unix(), e.rangeID))
	}
	return buf.String()
}

func TestStreamCloseScheduler(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	var (
		closeScheduler *streamCloseScheduler
		raftScheduler  *testingRaftScheduler
		clock          *timeutil.ManualTime
		stopper        *stop.Stopper
	)

	formatCloseScheduler := func() string {
		closeScheduler.mu.Lock()
		defer closeScheduler.mu.Unlock()

		var buf strings.Builder
		// Sort the items for deterministic output.
		items := []scheduledCloseEvent{}
		items = append(items, closeScheduler.mu.scheduled.items...)
		sort.Slice(items, func(i, j int) bool {
			return items[i].Less(items[j])
		})

		buf.WriteString("waiting=[")
		for i, e := range items {
			if i > 0 {
				buf.WriteString(",")
			}
			fmt.Fprintf(&buf, "(r%v,t%vs)", e.rangeID, e.at.Unix())
		}
		buf.WriteString("]")
		return buf.String()
	}

	datadriven.RunTest(t, "testdata/close_scheduler", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "init":
			stopper = stop.NewStopper()
			clock = timeutil.NewManualTime(timeutil.UnixEpoch)
			raftScheduler = &testingRaftScheduler{clock: clock}
			closeScheduler = NewStreamCloseScheduler(clock, raftScheduler)
			require.NoError(t, closeScheduler.Start(ctx, stopper))
			return fmt.Sprintf("now=%vs", clock.Now().Unix())

		case "schedule":
			var buf strings.Builder
			now := clock.Now()

			fmt.Fprintf(&buf, "submitted now=%vs\n", now.Unix())
			for _, line := range strings.Split(d.Input, "\n") {
				var rangeID int

				parts := strings.Fields(line)
				parts[0] = strings.TrimSpace(parts[0])
				require.True(t, strings.HasPrefix(parts[0], "range_id="))
				parts[0] = strings.TrimPrefix(parts[0], "range_id=")
				rangeID, err := strconv.Atoi(parts[0])
				require.NoError(t, err)

				parts[1] = strings.TrimSpace(parts[1])
				require.True(t, strings.HasPrefix(parts[1], "delay="))
				parts[1] = strings.TrimPrefix(parts[1], "delay=")
				delay, err := time.ParseDuration(parts[1])
				require.NoError(t, err)

				// Schedule the event and record the time it was scheduled at and for.
				closeScheduler.ScheduleSendStreamCloseRaftMuLocked(
					ctx, roachpb.RangeID(rangeID), delay)
				time.Sleep(20 * time.Millisecond)
				fmt.Fprintf(&buf, "  range_id=%v @ %vs (%vs+%vs)\n", rangeID,
					now.Add(delay).Unix(), now.Unix(), delay.Seconds())
			}
			return buf.String()

		case "tick":
			var durationStr string
			d.ScanArgs(t, "duration", &durationStr)
			duration, err := time.ParseDuration(durationStr)
			require.NoError(t, err)
			clock.Advance(duration)
			// Delay to allow the channel selects to fire.
			time.Sleep(20 * time.Millisecond)
			return fmt.Sprintf("now=%vs %v\n%v",
				clock.Now().Unix(), formatCloseScheduler(), raftScheduler.String())

		case "stop":
			stopper.Stop(ctx)
			return ""

		default:
			panic(fmt.Sprintf("unknown command: %s", d.Cmd))
		}
	})
}
