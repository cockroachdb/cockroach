// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package disk

import (
	"fmt"
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
)

var DefaultDiskStatsPollingInterval = envutil.EnvOrDefaultDuration("COCKROACH_DISK_STATS_POLLING_INTERVAL", 100*time.Millisecond)
var defaultDiskTracePeriod = envutil.EnvOrDefaultDuration("COCKROACH_DISK_TRACE_PERIOD", 30*time.Second)

// DeviceID uniquely identifies block devices.
type DeviceID struct {
	major uint32
	minor uint32
}

// String returns the string representation of the device ID.
func (d DeviceID) String() string {
	return fmt.Sprintf("%d:%d", d.major, d.minor)
}

// MonitorManager provides observability into a pool of disks by sampling disk stats
// at a high frequency. To do this efficiently, MonitorManager implements a pub/sub
// mechanism to avoid redundantly reading disk stats or reading stats for unmonitored
// disks. The subscription abstraction is implemented via a Monitor that provides
// callers the flexibility to consume the latest disk stats at different sampling
// frequencies while enforcing that the monitoredDisk is a singleton.
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
	dev, err := getDeviceIDFromPath(m.fs, path)
	if err != nil {
		return nil, errors.Wrapf(err, "fstat(%s)", path)
	}

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
		disk = &monitoredDisk{
			manager:  m,
			tracer:   newMonitorTracer(int(defaultDiskTracePeriod / DefaultDiskStatsPollingInterval)),
			deviceID: dev,
		}
		m.mu.disks = append(m.mu.disks, disk)

		// The design maintains the invariant that the disk stat polling loop
		// is always running unless there are no disks being monitored.
		if m.mu.stop == nil {
			collector, err := newStatsCollector(m.fs)
			if err != nil {
				return nil, err
			}
			m.mu.stop = make(chan struct{})
			go m.monitorDisks(collector, m.mu.stop)
		}
	}
	disk.refCount++

	return &Monitor{monitoredDisk: disk}, nil
}

func (m *MonitorManager) unrefDisk(disk *monitoredDisk) {
	var stop chan struct{}
	func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		disk.refCount--
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
	}()

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
	ticker := time.NewTicker(DefaultDiskStatsPollingInterval)
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
					disks[i].tracer.RecordEvent(traceEvent{
						time:  timeutil.Now(),
						stats: Stats{},
						err:   err,
					})
				}
			}
		}
	}
}

type monitoredDisk struct {
	manager  *MonitorManager
	tracer   *monitorTracer
	deviceID DeviceID
	// Tracks the number of Monitors observing stats on this disk. Once
	// the count is zero, the MonitorManager no longer needs to collect stats
	// for this device.
	// refCount is protected by manager.mu since the MonitorManager is responsible
	// for ensuring that the monitoredDisk is a singleton which relies on refCount
	// being modified atomically.
	refCount int
}

func (m *monitoredDisk) recordStats(t time.Time, stats Stats) {
	m.tracer.RecordEvent(traceEvent{
		time:  t,
		stats: stats,
		err:   nil,
	})
}

// StatsWindow is a wrapper around a rolling window of disk stats, used to
// apply common rudimentary computations or custom aggregation functions.
type StatsWindow struct {
	Stats []Stats
}

// Max returns the maximum change in stats for each field across the StatsWindow.
func (s StatsWindow) Max() Stats {
	var maxStats Stats
	if len(s.Stats) > 0 {
		// Since we compute diffs starting from index 1, the IOPS in progress count
		// at index 0 would be lost.
		maxStats = Stats{InProgressCount: s.Stats[0].InProgressCount}
	}
	var deltaStats Stats
	for i := 1; i < len(s.Stats); i++ {
		deltaStats = s.Stats[i].delta(&s.Stats[i-1])
		maxStats = deltaStats.max(&maxStats)
	}
	return maxStats
}

// Latest returns the last stat collected in the StatsWindow.
func (s StatsWindow) Latest() Stats {
	n := len(s.Stats)
	if n == 0 {
		return Stats{}
	}
	return s.Stats[n-1]
}

// Monitor provides statistics for an individual disk. Note that an individual
// monitor is not thread-safe, however, it can be cloned to be used in parallel.
type Monitor struct {
	*monitoredDisk

	mu struct {
		syncutil.Mutex
		// Tracks the time of the last invocation of IncrementalStats.
		lastIncrementedAt time.Time
	}
}

// CumulativeStats returns the most-recent stats observed.
func (m *Monitor) CumulativeStats() (Stats, error) {
	if event := m.tracer.Latest(); event.err != nil {
		return Stats{}, event.err
	} else {
		return event.stats, nil
	}
}

// updateLastIncrementedAt sets lastIncrementedAt to the current time and
// returns the previous value.
func (m *Monitor) swapLastIncrementedAt() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := m.mu.lastIncrementedAt
	m.mu.lastIncrementedAt = timeutil.Now()
	return result
}

// IncrementalStats returns all stats observed since its previous invocation.
// Note that the tracer has a bounded capacity and the caller must invoke this
// method at least as frequently as every COCKROACH_DISK_TRACE_PERIOD to avoid
// missing events.
func (m *Monitor) IncrementalStats() StatsWindow {
	lastIncrementedAt := m.swapLastIncrementedAt()
	if lastIncrementedAt.IsZero() {
		return StatsWindow{}
	}

	events := m.tracer.RollingWindow(lastIncrementedAt)
	stats := make([]Stats, 0, len(events))
	for _, event := range events {
		// Ignore events where we were unable to collect disk stats.
		if event.err == nil {
			stats = append(stats, event.stats)
		}
	}
	return StatsWindow{stats}
}

// Clone returns a new monitor that monitors the same disk.
func (m *Monitor) Clone() *Monitor {
	m.manager.mu.Lock()
	defer m.manager.mu.Unlock()
	m.refCount++
	return &Monitor{monitoredDisk: m.monitoredDisk}
}

func (m *Monitor) LogTrace() string {
	return m.tracer.String()
}

func (m *Monitor) Close() {
	if m.monitoredDisk != nil {
		m.manager.unrefDisk(m.monitoredDisk)
		m.monitoredDisk = nil
	}
}

func getDeviceIDFromPath(fs vfs.FS, path string) (DeviceID, error) {
	finfo, err := fs.Stat(path)
	if err != nil {
		return DeviceID{}, errors.Wrapf(err, "fstat(%s)", path)
	}
	return deviceIDFromFileInfo(finfo), nil
}
