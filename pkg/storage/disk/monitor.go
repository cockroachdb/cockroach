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
	"fmt"
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
)

var defaultDiskStatsPollingInterval = envutil.EnvOrDefaultDuration("COCKROACH_DISK_STATS_POLLING_INTERVAL", 100*time.Millisecond)

// DeviceID uniquely identifies block devices.
type DeviceID struct {
	major uint32
	minor uint32
}

// String returns the string representation of the device ID.
func (d DeviceID) String() string {
	return fmt.Sprintf("%d:%d", d.major, d.minor)
}

// MonitorManager provides observability into a pool of disks.
type MonitorManager struct {
	fs vfs.FS

	mu struct {
		syncutil.Mutex
		stop  chan struct{}
		disks []*monitoredDisk
	}
}

func NewMonitorManager(fs vfs.FS) *MonitorManager {
	return &MonitorManager{fs: fs}
}

// Monitor identifies the device underlying the file/directory at the
// provided path. If the device is not already being monitored it spawns a
// goroutine to track its disk stats, otherwise it returns a Monitor handle
// to access the stats.
func (m *MonitorManager) Monitor(path string) (*Monitor, error) {
	finfo, err := m.fs.Stat(path)
	if err != nil {
		return nil, errors.Wrapf(err, "fstat(%s)", path)
	}
	dev := deviceIDFromFileInfo(finfo)

	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if the disk is already being monitored.
	var disk *monitoredDisk
	for i := 0; i < len(m.mu.disks); i++ {
		if m.mu.disks[i].deviceID == dev {
			disk = m.mu.disks[i]
			break
		}
	}

	if disk == nil {
		disk = &monitoredDisk{manager: m, deviceID: dev}
		collector, err := newStatsCollector(m.fs)
		if err != nil {
			return nil, err
		}
		// Ensure that stats are recorded for the new disk before returning to provide
		// callers with an initial measurement for computing incremental stats.
		if err := collector.collect([]*monitoredDisk{disk}); err != nil {
			return nil, err
		}
		m.mu.disks = append(m.mu.disks, disk)

		// The design maintains the invariant that the disk stat polling loop
		// is always running unless there are no disks being monitored.
		if m.mu.stop == nil {
			m.mu.stop = make(chan struct{})
			go m.monitorDisks(collector, m.mu.stop)
		}
	}
	disk.refCount++

	return &Monitor{monitoredDisk: disk}, nil
}

func (m *MonitorManager) unrefDisk(disk *monitoredDisk) {
	m.mu.Lock()
	disk.refCount--
	var stop chan struct{}
	//nolint:deferunlockcheck
	if disk.refCount == 0 {
		// Since no one is monitoring this disk we remove it from the slice of monitored disks.
		i := slices.Index(m.mu.disks, disk)
		if i == -1 {
			panic(errors.AssertionFailedf("disk %s had one ref, but is not monitored", disk.deviceID))
		}
		// Instead of mutating in place we create a new slice in case the disk stat polling loop
		// is reading the old slice.
		m.mu.disks = append(slices.Clone(m.mu.disks[:i]), m.mu.disks[i+1:]...)

		// If the MonitorManager has no disks left to monitor, the disk stat polling loop can
		// be stopped.
		if len(m.mu.disks) == 0 {
			stop = m.mu.stop
			m.mu.stop = nil
		}
	}
	//nolint:deferunlockcheck
	m.mu.Unlock()

	if stop != nil {
		stop <- struct{}{}
	}
}

type statsCollector interface {
	collect(disks []*monitoredDisk) error
}

// monitorDisks runs a loop collecting disk stats for all monitored disks.
//
// NB: A stop channel must be passed down to ensure that the function terminates during the
// race where the MonitorManager creates a new stop channel after unrefDisk sends a message
// across the old stop channel.
func (m *MonitorManager) monitorDisks(collector statsCollector, stop chan struct{}) {
	ticker := time.NewTicker(defaultDiskStatsPollingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			close(stop)
			return
		case <-ticker.C:
			m.mu.Lock()
			disks := m.mu.disks
			m.mu.Unlock()

			if err := collector.collect(disks); err != nil {
				for i := range disks {
					disks[i].stats.Lock()
					disks[i].stats.err = err
					disks[i].stats.Unlock()
				}
			}
		}
	}
}

type monitoredDisk struct {
	manager  *MonitorManager
	deviceID DeviceID
	// Tracks the number of Monitors observing stats on this disk. Once
	// the count is zero, the MonitorManager no longer needs to collect stats
	// for this device.
	refCount int // protected by manager.mu

	stats struct {
		syncutil.Mutex
		err             error
		lastMeasurement Stats
	}
}

func (m *monitoredDisk) recordStats(t time.Time, stats Stats) {
	m.stats.Lock()
	defer m.stats.Unlock()
	m.stats.lastMeasurement = stats
	m.stats.err = nil
}

// Monitor provides statistics for an individual disk.
type Monitor struct {
	*monitoredDisk
}

func (m *Monitor) Close() {
	if m.monitoredDisk != nil {
		m.manager.unrefDisk(m.monitoredDisk)
		m.monitoredDisk = nil
	}
}

func (m *Monitor) LatestStats() (Stats, error) {
	m.stats.Lock()
	defer m.stats.Unlock()
	if m.stats.err != nil {
		return Stats{}, m.stats.err
	}
	return m.stats.lastMeasurement, nil
}
