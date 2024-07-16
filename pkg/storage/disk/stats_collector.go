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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
)

// WriteStatsManager is wrapper that holds disk write stats collectors.
type WriteStatsManager interface {
	// GetOrCreateCollector returns the stats collector associated with the physical
	// disk of the path provided. It creates a new one if one does not exist.
	GetOrCreateCollector(path string) (*vfs.DiskWriteStatsCollector, error)
	// GetAllStatsCollectors returns all stats collectors in the map.
	GetAllStatsCollectors() []*vfs.DiskWriteStatsCollector
}

// StatsManager provides a mapping from OS-level DeviceID to the
// vfs.DiskWriteStatsCollector.
type StatsManager struct {
	fs vfs.FS
	mu struct {
		syncutil.Mutex
		categorizedStatsCollectors map[DeviceID]*vfs.DiskWriteStatsCollector
	}
}

// NewWriteStatsManager initializes a StatsManager with the vfs.FS provided.
func NewWriteStatsManager(fs vfs.FS) WriteStatsManager {
	sm := &StatsManager{fs: fs}
	sm.mu.categorizedStatsCollectors = make(map[DeviceID]*vfs.DiskWriteStatsCollector)
	return sm
}

// GetOrCreateCollector implements WriteStatsManager.
func (sm *StatsManager) GetOrCreateCollector(path string) (*vfs.DiskWriteStatsCollector, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	deviceID, err := getDeviceIDFromPath(sm.fs, path)
	if err != nil {
		return nil, errors.Wrap(err, "failed to map path to device ID")
	}
	if _, ok := sm.mu.categorizedStatsCollectors[deviceID]; !ok {
		sm.mu.categorizedStatsCollectors[deviceID] = vfs.NewDiskWriteStatsCollector()
	}
	return sm.mu.categorizedStatsCollectors[deviceID], nil
}

// GetAllStatsCollectors implements WriteStatsManager.
func (sm *StatsManager) GetAllStatsCollectors() []*vfs.DiskWriteStatsCollector {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	var stats []*vfs.DiskWriteStatsCollector
	for _, c := range sm.mu.categorizedStatsCollectors {
		stats = append(stats, c)
	}
	return stats
}

// TestingStatsManager is used for tests. It maps path to
// vfs.DiskWriteStatsCollector instead of DeviceID.
type TestingStatsManager struct {
	fs vfs.FS
	mu struct {
		syncutil.Mutex
		categorizedStatsCollectors map[string]*vfs.DiskWriteStatsCollector
	}
}

// NewTestingStatsManager initializes a TestingStatsManager used for testing purposes.
func NewTestingStatsManager(fs vfs.FS) WriteStatsManager {
	sm := &TestingStatsManager{fs: fs}
	sm.mu.categorizedStatsCollectors = make(map[string]*vfs.DiskWriteStatsCollector)
	return sm
}

// GetOrCreateCollector implements WriteStatsManager.
func (sm *TestingStatsManager) GetOrCreateCollector(
	path string,
) (*vfs.DiskWriteStatsCollector, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if _, ok := sm.mu.categorizedStatsCollectors[path]; !ok {
		sm.mu.categorizedStatsCollectors[path] = vfs.NewDiskWriteStatsCollector()
	}
	return sm.mu.categorizedStatsCollectors[path], nil
}

// GetAllStatsCollectors implements WriteStatsManager.
func (sm *TestingStatsManager) GetAllStatsCollectors() []*vfs.DiskWriteStatsCollector {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	var stats []*vfs.DiskWriteStatsCollector
	for _, c := range sm.mu.categorizedStatsCollectors {
		stats = append(stats, c)
	}
	return stats
}
