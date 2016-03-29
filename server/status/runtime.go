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
// permissions and limitations under the License.
//
// Author: Matt Tracy (matt.r.tracy@gmail.com)

package status

import (
	"os"
	"runtime"
	"time"

	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/metric"

	"github.com/dustin/go-humanize"
	"github.com/elastic/gosigar"
)

const (
	nameCgoCalls       = "cgocalls"
	nameGoroutines     = "goroutines"
	nameAllocBytes     = "allocbytes"
	nameSysBytes       = "sysbytes"
	nameGCCount        = "gc.count"
	nameGCPauseNS      = "gc.pause.ns"
	nameGCPausePercent = "gc.pause.percent"
	nameCPUUserNS      = "cpu.user.ns"
	nameCPUUserPercent = "cpu.user.percent"
	nameCPUSysNS       = "cpu.sys.ns"
	nameCPUSysPercent  = "cpu.sys.percent"
	nameRSS            = "rss"
)

// logOSStats is a function that logs OS-specific stats. We will not necessarily
// have implementations for all OSes.
var logOSStats func()

// RuntimeStatSampler is used to periodically sample the runtime environment
// for useful statistics, performing some rudimentary calculations and storing
// the resulting information in a format that can be easily consumed by status
// logging systems.
type RuntimeStatSampler struct {
	clock    *hlc.Clock
	registry *metric.Registry

	// The last sampled values of some statistics are kept only to compute
	// derivative statistics.
	lastNow       int64
	lastUtime     int64
	lastStime     int64
	lastPauseTime uint64
	lastCgoCall   int64
	lastNumGC     uint32

	// Metric gauges maintained by the sampler.
	cgoCalls       *metric.Gauge
	goroutines     *metric.Gauge
	allocBytes     *metric.Gauge
	sysBytes       *metric.Gauge
	gcCount        *metric.Gauge
	gcPauseNS      *metric.Gauge
	gcPausePercent *metric.GaugeFloat64
	cpuUserNS      *metric.Gauge
	cpuUserPercent *metric.GaugeFloat64
	cpuSysNS       *metric.Gauge
	cpuSysPercent  *metric.GaugeFloat64
	rss            *metric.Gauge
}

// MakeRuntimeStatSampler constructs a new RuntimeStatSampler object.
func MakeRuntimeStatSampler(clock *hlc.Clock) RuntimeStatSampler {
	reg := metric.NewRegistry()
	return RuntimeStatSampler{
		registry:       reg,
		clock:          clock,
		cgoCalls:       reg.Gauge(nameCgoCalls),
		goroutines:     reg.Gauge(nameGoroutines),
		allocBytes:     reg.Gauge(nameAllocBytes),
		sysBytes:       reg.Gauge(nameSysBytes),
		gcCount:        reg.Gauge(nameGCCount),
		gcPauseNS:      reg.Gauge(nameGCPauseNS),
		gcPausePercent: reg.GaugeFloat64(nameGCPausePercent),
		cpuUserNS:      reg.Gauge(nameCPUUserNS),
		cpuUserPercent: reg.GaugeFloat64(nameCPUUserPercent),
		cpuSysNS:       reg.Gauge(nameCPUSysNS),
		cpuSysPercent:  reg.GaugeFloat64(nameCPUSysPercent),
		rss:            reg.Gauge(nameRSS),
	}
}

// Registry returns the metric.Registry object in which the runtime recorder
// stores its metric gauges.
func (rsr RuntimeStatSampler) Registry() *metric.Registry {
	return rsr.registry
}

// SampleEnvironment queries the runtime system for various interesting metrics,
// storing the resulting values in the set of metric gauges maintained by
// RuntimeStatSampler. This makes runtime statistics more convenient for
// consumption by the time series and status systems.
//
// This method should be called periodically by a higher level system in order
// to keep runtime statistics current.
func (rsr *RuntimeStatSampler) SampleEnvironment() {
	// Record memory and call stats from the runtime package.
	// TODO(mrtracy): memory statistics will not include usage from RocksDB.
	// Determine an appropriate way to compute total memory usage.
	numCgoCall := runtime.NumCgoCall()
	numGoroutine := runtime.NumGoroutine()

	// It might be useful to call ReadMemStats() more often, but it stops the
	// world while collecting stats so shouldn't be called too often.
	ms := runtime.MemStats{}
	runtime.ReadMemStats(&ms)

	// Retrieve Mem and CPU statistics.
	pid := os.Getpid()
	mem := gosigar.ProcMem{}
	if err := mem.Get(pid); err != nil {
		log.Errorf("unable to get mem usage: %v", err)
	}
	cpu := gosigar.ProcTime{}
	if err := cpu.Get(pid); err != nil {
		log.Errorf("unable to get cpu usage: %v", err)
	}

	// Time statistics can be compared to the total elapsed time to create a
	// useful percentage of total CPU usage, which would be somewhat less accurate
	// if calculated later using downsampled time series data.
	now := rsr.clock.PhysicalNow()
	dur := float64(now - rsr.lastNow)
	// cpu.{User,Sys} are in milliseconds, convert to nanoseconds.
	newUtime := int64(cpu.User) * 1e6
	newStime := int64(cpu.Sys) * 1e6
	uPerc := float64(newUtime-rsr.lastUtime) / dur
	sPerc := float64(newStime-rsr.lastStime) / dur
	pausePerc := float64(ms.PauseTotalNs-rsr.lastPauseTime) / dur
	rsr.lastNow = now
	rsr.lastUtime = newUtime
	rsr.lastStime = newStime
	rsr.lastPauseTime = ms.PauseTotalNs

	// Log summary of statistics to console.
	cgoRate := float64((numCgoCall-rsr.lastCgoCall)*int64(time.Second)) / dur
	log.Infof("runtime stats: %s RSS, %d goroutines, %s active, %.2fcgo/sec, %.2f/%.2f %%(u/s)time, %.2f %%gc (%dx)",
		humanize.IBytes(mem.Resident), numGoroutine, humanize.IBytes(ms.Alloc),
		cgoRate, uPerc, sPerc, pausePerc, ms.NumGC-rsr.lastNumGC)
	if log.V(2) {
		log.Infof("memstats: %+v", ms)
	}
	if logOSStats != nil {
		logOSStats()
	}
	rsr.lastCgoCall = numCgoCall
	rsr.lastNumGC = ms.NumGC

	rsr.cgoCalls.Update(numCgoCall)
	rsr.goroutines.Update(int64(numGoroutine))
	rsr.allocBytes.Update(int64(ms.Alloc))
	rsr.sysBytes.Update(int64(ms.Sys))
	rsr.gcCount.Update(int64(ms.NumGC))
	rsr.gcPauseNS.Update(int64(ms.PauseTotalNs))
	rsr.gcPausePercent.Update(pausePerc)
	rsr.cpuUserNS.Update(newUtime)
	rsr.cpuUserPercent.Update(uPerc)
	rsr.cpuSysNS.Update(newStime)
	rsr.cpuSysPercent.Update(sPerc)
	rsr.rss.Update(int64(mem.Resident))
}
