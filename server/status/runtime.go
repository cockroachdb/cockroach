// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Matt Tracy (matt.r.tracy@gmail.com)

package status

import (
	"fmt"
	"runtime"
	"syscall"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
)

// RuntimeStatRecorder is used to periodically persist useful runtime statistics
// as time series data. "Runtime statistics" include OS-level statistics (such as
// memory and CPU usage) and Go runtime statistics (e.g. count of Goroutines).
type RuntimeStatRecorder struct {
	nodeID        proto.NodeID
	clock         *hlc.Clock
	lastDataCount int

	// The last recorded values of some statistics are kept to compute
	// derivative statistics.
	lastNow       int64
	lastUtime     int64
	lastStime     int64
	lastPauseTime uint64
	lastCgoCall   int64
	lastNumGC     uint32
}

// NewRuntimeStatRecorder instantiates a runtime status recorder for the
// supplied node ID.
func NewRuntimeStatRecorder(nodeID proto.NodeID, clock *hlc.Clock) *RuntimeStatRecorder {
	return &RuntimeStatRecorder{
		nodeID: nodeID,
		clock:  clock,
	}
}

// recordFloat records a single float64 value recorded from a runtime statistic as a
// proto.TimeSeriesData object.
func (rsr *RuntimeStatRecorder) record(timestampNanos int64, name string,
	data float64) proto.TimeSeriesData {
	return proto.TimeSeriesData{
		Name: fmt.Sprintf(runtimeStatTimeSeriesNameFmt, name, rsr.nodeID),
		Datapoints: []*proto.TimeSeriesDatapoint{
			{
				TimestampNanos: timestampNanos,
				Value:          data,
			},
		},
	}
}

// GetTimeSeriesData returns a slice of TimeSeriesData updates based on current
// runtime statistics.
//
// Calling this method will query various system packages for runtime statistics
// and convert the information to time series data. This is currently done in
// one method because it is convenient; however, in the future querying and
// recording can be easily separated, similar to the way that NodeStatus is
// separated into a monitor and a recorder.
func (rsr *RuntimeStatRecorder) GetTimeSeriesData() []proto.TimeSeriesData {
	data := make([]proto.TimeSeriesData, 0, rsr.lastDataCount)

	// Record memory and call stats from the runtime package.
	// TODO(mrtracy): memory statistics will not include usage from RocksDB.
	// Determine an appropriate way to compute total memory usage.
	numCgoCall := runtime.NumCgoCall()
	numGoroutine := runtime.NumGoroutine()
	ms := runtime.MemStats{}
	runtime.ReadMemStats(&ms)

	// Record CPU statistics using syscall package.
	ru := syscall.Rusage{}
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		log.Errorf("Getrusage failed: %v", err)
	}

	// Time statistics can be compared to the total elapsed time to create a
	// useful percentage of total CPU usage, which would be somewhat less accurate
	// if calculated later using downsampled time series data.
	now := rsr.clock.PhysicalNow()
	dur := float64(now - rsr.lastNow)
	newUtime := ru.Utime.Nano()
	newStime := ru.Stime.Nano()
	uPerc := float64(newUtime-rsr.lastUtime) / dur
	sPerc := float64(newStime-rsr.lastStime) / dur
	pausePerc := float64(ms.PauseTotalNs-rsr.lastPauseTime) / dur
	rsr.lastNow = now
	rsr.lastUtime = newUtime
	rsr.lastStime = newStime
	rsr.lastPauseTime = ms.PauseTotalNs

	// Log summary of statistics to console, if requested.
	if log.V(1) {
		activeMiB := float64(ms.Alloc) / (1 << 20)
		cgoRate := float64((numCgoCall-rsr.lastCgoCall)*int64(time.Second)) / dur
		log.Infof("runtime stats: %d goroutines, %.2fMiB active, %.2fcgo/sec, %.2f/%.2f %%(u/s)time, %.2f %%gc (%dx)",
			numGoroutine, activeMiB, cgoRate, uPerc, sPerc, pausePerc, ms.NumGC-rsr.lastNumGC)
		rsr.lastCgoCall = numCgoCall
		rsr.lastNumGC = ms.NumGC
	}

	data = append(data, rsr.record(now, "cgocalls", float64(numCgoCall)))
	data = append(data, rsr.record(now, "goroutines", float64(numGoroutine)))
	data = append(data, rsr.record(now, "allocbytes", float64(ms.Alloc)))
	data = append(data, rsr.record(now, "gc.count", float64(ms.NumGC)))
	data = append(data, rsr.record(now, "gc.pause.ns", float64(ms.PauseTotalNs)))
	data = append(data, rsr.record(now, "gc.pause.percent", pausePerc))
	data = append(data, rsr.record(now, "cpu.user.ns", float64(newUtime)))
	data = append(data, rsr.record(now, "cpu.user.percent", uPerc))
	data = append(data, rsr.record(now, "cpu.sys.ns", float64(newStime)))
	data = append(data, rsr.record(now, "cpu.sys.percent", sPerc))
	rsr.lastDataCount = len(data)
	return data
}
