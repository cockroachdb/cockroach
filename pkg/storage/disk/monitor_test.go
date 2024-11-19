// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package disk

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

type spyCollector struct {
	collectCount int
}

func (s *spyCollector) collect(disks []*monitoredDisk) error {
	s.collectCount++
	return nil
}

func TestMonitorManager_monitorDisks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	manager := NewMonitorManager(vfs.NewMem())
	testDisk := &monitoredDisk{
		manager: manager,
		deviceID: DeviceID{
			major: 0,
			minor: 0,
		},
	}
	manager.mu.disks = []*monitoredDisk{testDisk}

	testCollector := &spyCollector{}
	stop := make(chan struct{})
	go manager.monitorDisks(testCollector, stop)

	time.Sleep(2 * DefaultDiskStatsPollingInterval)
	stop <- struct{}{}
	require.Greater(t, testCollector.collectCount, 0)
}

func TestMonitor_StatsWindow(t *testing.T) {
	window := StatsWindow{
		Stats: []Stats{
			{
				ReadsCount:      1,
				InProgressCount: 3,
			},
			{
				ReadsCount:      4,
				InProgressCount: 1,
			},
			{
				ReadsCount:      9,
				InProgressCount: 7,
			},
		},
	}

	maxStats := window.Max()
	expectedMaxStats := Stats{ReadsCount: 5, InProgressCount: 7}
	require.Equal(t, expectedMaxStats, maxStats)

	latestStats := window.Latest()
	expectedLatestStats := Stats{ReadsCount: 9, InProgressCount: 7}
	require.Equal(t, expectedLatestStats, latestStats)
}

func TestMonitor_IncrementalStats(t *testing.T) {
	now := time.Now()
	tracer := newMonitorTracer(4)
	events := []traceEvent{
		{
			time:  now.Add(-4 * time.Minute),
			stats: Stats{ReadsCount: 1, InProgressCount: 7},
			err:   nil,
		},
		{
			time:  now.Add(-2 * time.Minute),
			stats: Stats{ReadsCount: 4, InProgressCount: 5},
			err:   nil,
		},
		{
			time:  now.Add(-time.Minute),
			stats: Stats{ReadsCount: 9, InProgressCount: 1},
			err:   nil,
		},
		{
			time:  now,
			stats: Stats{},
			err:   errors.New("failed to collect disk stats"),
		},
	}
	for _, event := range events {
		tracer.RecordEvent(event)
	}
	monitor := Monitor{
		monitoredDisk: &monitoredDisk{tracer: tracer},
	}
	monitor.mu.lastIncrementedAt = now.Add(-3 * time.Minute)

	rollingWindow := monitor.IncrementalStats()
	// Skip the event collected 4 minutes ago since we last incremented 3 minutes ago.
	expectedWindow := StatsWindow{
		Stats: []Stats{
			{ReadsCount: 4, InProgressCount: 5},
			{ReadsCount: 9, InProgressCount: 1},
		},
	}
	require.Equal(t, expectedWindow, rollingWindow)
}

func TestMonitor_Close(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	manager := NewMonitorManager(vfs.NewMem())
	testDisk := &monitoredDisk{
		manager: manager,
		deviceID: DeviceID{
			major: 0,
			minor: 0,
		},
		refCount: 2,
	}
	stop := make(chan struct{})
	manager.mu.stop = stop
	manager.mu.disks = []*monitoredDisk{testDisk}
	monitor1 := Monitor{monitoredDisk: testDisk}
	monitor2 := Monitor{monitoredDisk: testDisk}

	monitor1.Close()
	require.Equal(t, 1, testDisk.refCount)

	monitor1.Close()
	// Subsequent calls to a closed monitor should not reduce refCount.
	require.Equal(t, 1, testDisk.refCount)

	go monitor2.Close()
	// If there are no monitors, stop the stat polling loop.
	select {
	case <-stop:
	case <-time.After(time.Second):
		t.Fatal("Failed to receive stop signal")
	}
	require.Equal(t, 0, testDisk.refCount)
}
