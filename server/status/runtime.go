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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/metric"

	"github.com/dustin/go-humanize"
	"github.com/elastic/gosigar"
)

var (
	metaCgoCalls       = metric.MetricMetadata{"sys.cgocalls", "Number of cgo calls"}
	metaGoroutines     = metric.MetricMetadata{"sys.goroutines", "Number of goroutines"}
	metaGoAllocBytes   = metric.MetricMetadata{"sys.go.allocbytes", ""}
	metaGoTotalBytes   = metric.MetricMetadata{"sys.go.totalbytes", ""}
	metaCgoAllocBytes  = metric.MetricMetadata{"sys.cgo.allocbytes", ""}
	metaCgoTotalBytes  = metric.MetricMetadata{"sys.cgo.totalbytes", ""}
	metaGCCount        = metric.MetricMetadata{"sys.gc.count", ""}
	metaGCPauseNS      = metric.MetricMetadata{"sys.gc.pause.ns", ""}
	metaGCPausePercent = metric.MetricMetadata{"sys.gc.pause.percent", ""}
	metaCPUUserNS      = metric.MetricMetadata{"sys.cpu.user.ns", ""}
	metaCPUUserPercent = metric.MetricMetadata{"sys.cpu.user.percent", ""}
	metaCPUSysNS       = metric.MetricMetadata{"sys.cpu.sys.ns", ""}
	metaCPUSysPercent  = metric.MetricMetadata{"sys.cpu.sys.percent", ""}
	metaRSS            = metric.MetricMetadata{"sys.rss", ""}
)

// getCgoMemStats is a function that fetches stats for the C++ portion of the code.
// We will not necessarily have implementations for all builds, so check for nil first.
// Returns the following:
// allocated uint64: bytes allocated by application
// total     uint64: total bytes requested from system
// error           : any issues fetching stats. This should be a warning only.
var getCgoMemStats func() (uint64, uint64, error)

// RuntimeStatSampler is used to periodically sample the runtime environment
// for useful statistics, performing some rudimentary calculations and storing
// the resulting information in a format that can be easily consumed by status
// logging systems.
type RuntimeStatSampler struct {
	clock *hlc.Clock

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
	goAllocBytes   *metric.Gauge
	goTotalBytes   *metric.Gauge
	cgoAllocBytes  *metric.Gauge
	cgoTotalBytes  *metric.Gauge
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
func MakeRuntimeStatSampler(clock *hlc.Clock, reg *metric.Registry) RuntimeStatSampler {
	r := RuntimeStatSampler{
		clock:          clock,
		cgoCalls:       metric.NewGauge(metaCgoCalls),
		goroutines:     metric.NewGauge(metaGoroutines),
		goAllocBytes:   metric.NewGauge(metaGoAllocBytes),
		goTotalBytes:   metric.NewGauge(metaGoTotalBytes),
		cgoAllocBytes:  metric.NewGauge(metaCgoAllocBytes),
		cgoTotalBytes:  metric.NewGauge(metaCgoTotalBytes),
		gcCount:        metric.NewGauge(metaGCCount),
		gcPauseNS:      metric.NewGauge(metaGCPauseNS),
		gcPausePercent: metric.NewGaugeFloat64(metaGCPausePercent),
		cpuUserNS:      metric.NewGauge(metaCPUUserNS),
		cpuUserPercent: metric.NewGaugeFloat64(metaCPUUserPercent),
		cpuSysNS:       metric.NewGauge(metaCPUSysNS),
		cpuSysPercent:  metric.NewGaugeFloat64(metaCPUSysPercent),
		rss:            metric.NewGauge(metaRSS),
	}

	reg.AddMetric(r.cgoCalls)
	reg.AddMetric(r.goroutines)
	reg.AddMetric(r.goAllocBytes)
	reg.AddMetric(r.goTotalBytes)
	reg.AddMetric(r.cgoAllocBytes)
	reg.AddMetric(r.cgoTotalBytes)
	reg.AddMetric(r.gcCount)
	reg.AddMetric(r.gcPauseNS)
	reg.AddMetric(r.gcPausePercent)
	reg.AddMetric(r.cpuUserNS)
	reg.AddMetric(r.cpuUserPercent)
	reg.AddMetric(r.cpuSysNS)
	reg.AddMetric(r.cpuSysPercent)
	reg.AddMetric(r.rss)

	return r
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
	// NOTE: the MemStats fields do not get decremented when memory is released,
	// to get accurate numbers, be sure to subtract. eg: ms.Sys - ms.HeapReleased for
	// current memory reserved.
	ms := runtime.MemStats{}
	runtime.ReadMemStats(&ms)

	// Retrieve Mem and CPU statistics.
	pid := os.Getpid()
	mem := gosigar.ProcMem{}
	if err := mem.Get(pid); err != nil {
		log.Errorf(context.TODO(), "unable to get mem usage: %v", err)
	}
	cpu := gosigar.ProcTime{}
	if err := cpu.Get(pid); err != nil {
		log.Errorf(context.TODO(), "unable to get cpu usage: %v", err)
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

	var cgoAllocated, cgoTotal uint64
	if getCgoMemStats != nil {
		var err error
		cgoAllocated, cgoTotal, err = getCgoMemStats()
		if err != nil {
			log.Warningf(context.TODO(), "problem fetching CGO memory stats: %s, CGO stats will be empty.", err)
		}
	}

	goAllocated := ms.Alloc
	goTotal := ms.Sys - ms.HeapReleased

	// Log summary of statistics to console.
	cgoRate := float64((numCgoCall-rsr.lastCgoCall)*int64(time.Second)) / dur
	log.Infof(context.TODO(), "runtime stats: %s RSS, %d goroutines, %s/%s/%s GO alloc/idle/total, %s/%s CGO alloc/total, %.2fcgo/sec, %.2f/%.2f %%(u/s)time, %.2f %%gc (%dx)",
		humanize.IBytes(mem.Resident), numGoroutine,
		humanize.IBytes(goAllocated), humanize.IBytes(ms.HeapIdle-ms.HeapReleased), humanize.IBytes(goTotal),
		humanize.IBytes(cgoAllocated), humanize.IBytes(cgoTotal),
		cgoRate, uPerc, sPerc, pausePerc, ms.NumGC-rsr.lastNumGC)
	if log.V(2) {
		log.Infof(context.TODO(), "memstats: %+v", ms)
	}
	rsr.lastCgoCall = numCgoCall
	rsr.lastNumGC = ms.NumGC

	rsr.cgoCalls.Update(numCgoCall)
	rsr.goroutines.Update(int64(numGoroutine))
	rsr.goAllocBytes.Update(int64(goAllocated))
	rsr.goTotalBytes.Update(int64(goTotal))
	rsr.cgoAllocBytes.Update(int64(cgoAllocated))
	rsr.cgoTotalBytes.Update(int64(cgoTotal))
	rsr.gcCount.Update(int64(ms.NumGC))
	rsr.gcPauseNS.Update(int64(ms.PauseTotalNs))
	rsr.gcPausePercent.Update(pausePerc)
	rsr.cpuUserNS.Update(newUtime)
	rsr.cpuUserPercent.Update(uPerc)
	rsr.cpuSysNS.Update(newStime)
	rsr.cpuSysPercent.Update(sPerc)
	rsr.rss.Update(int64(mem.Resident))
}
