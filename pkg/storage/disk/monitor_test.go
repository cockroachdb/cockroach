// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package disk

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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

	time.Sleep(2 * defaultDiskStatsPollingInterval)
	stop <- struct{}{}
	require.Greater(t, testCollector.collectCount, 0)
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
	require.Equal(t, testDisk.refCount, 1)

	monitor1.Close()
	// Subsequent calls to a closed monitor should not reduce refCount.
	require.Equal(t, testDisk.refCount, 1)

	go monitor2.Close()
	// If there are no monitors, stop the stat polling loop.
	select {
	case <-stop:
	case <-time.After(time.Second):
		t.Fatal("Failed to receive stop signal")
	}
	require.Equal(t, testDisk.refCount, 0)
}

func TestMonitor_IncrementalStats(t *testing.T) {
	testDisk := &monitoredDisk{
		stats: struct {
			syncutil.Mutex
			err             error
			lastMeasurement Stats
		}{
			lastMeasurement: Stats{
				ReadsCount:      1,
				InProgressCount: 3,
			},
		},
	}
	monitor := Monitor{monitoredDisk: testDisk}

	// First attempt at getting incremental stats should return empty stats.
	stats, err := monitor.IncrementalStats()
	require.NoError(t, err)
	require.Equal(t, stats, Stats{})

	testDisk.stats.lastMeasurement = Stats{
		ReadsCount:      2,
		InProgressCount: 2,
	}
	wantIncremental := Stats{
		ReadsCount: 1,
		// InProgressCount is a gauge so the increment should not be computed.
		InProgressCount: 2,
	}

	stats, err = monitor.IncrementalStats()
	require.NoError(t, err)
	require.Equal(t, stats, wantIncremental)
}
