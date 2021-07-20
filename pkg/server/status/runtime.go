// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package status

import (
	"context"
	"os"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/util/cgroups"
	"github.com/cockroachdb/cockroach/pkg/util/goschedstats"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/elastic/gosigar"
	"github.com/shirou/gopsutil/net"
)

var (
	metaCgoCalls = metric.Metadata{
		Name:        "sys.cgocalls",
		Help:        "Total number of cgo calls",
		Measurement: "cgo Calls",
		Unit:        metric.Unit_COUNT,
	}
	metaGoroutines = metric.Metadata{
		Name:        "sys.goroutines",
		Help:        "Current number of goroutines",
		Measurement: "goroutines",
		Unit:        metric.Unit_COUNT,
	}
	metaRunnableGoroutinesPerCPU = metric.Metadata{
		Name:        "sys.runnable.goroutines.per.cpu",
		Help:        "Average number of goroutines that are waiting to run, normalized by number of cores",
		Measurement: "goroutines",
		Unit:        metric.Unit_COUNT,
	}
	metaGoAllocBytes = metric.Metadata{
		Name:        "sys.go.allocbytes",
		Help:        "Current bytes of memory allocated by go",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaGoTotalBytes = metric.Metadata{
		Name:        "sys.go.totalbytes",
		Help:        "Total bytes of memory allocated by go, but not released",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaCgoAllocBytes = metric.Metadata{
		Name:        "sys.cgo.allocbytes",
		Help:        "Current bytes of memory allocated by cgo",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaCgoTotalBytes = metric.Metadata{
		Name:        "sys.cgo.totalbytes",
		Help:        "Total bytes of memory allocated by cgo, but not released",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaGCCount = metric.Metadata{
		Name:        "sys.gc.count",
		Help:        "Total number of GC runs",
		Measurement: "GC Runs",
		Unit:        metric.Unit_COUNT,
	}
	metaGCPauseNS = metric.Metadata{
		Name:        "sys.gc.pause.ns",
		Help:        "Total GC pause",
		Measurement: "GC Pause",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaGCPausePercent = metric.Metadata{
		Name:        "sys.gc.pause.percent",
		Help:        "Current GC pause percentage",
		Measurement: "GC Pause",
		Unit:        metric.Unit_PERCENT,
	}
	metaCPUUserNS = metric.Metadata{
		Name:        "sys.cpu.user.ns",
		Help:        "Total user cpu time",
		Measurement: "CPU Time",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaCPUUserPercent = metric.Metadata{
		Name:        "sys.cpu.user.percent",
		Help:        "Current user cpu percentage",
		Measurement: "CPU Time",
		Unit:        metric.Unit_PERCENT,
	}
	metaCPUSysNS = metric.Metadata{
		Name:        "sys.cpu.sys.ns",
		Help:        "Total system cpu time",
		Measurement: "CPU Time",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaCPUSysPercent = metric.Metadata{
		Name:        "sys.cpu.sys.percent",
		Help:        "Current system cpu percentage",
		Measurement: "CPU Time",
		Unit:        metric.Unit_PERCENT,
	}
	metaCPUCombinedPercentNorm = metric.Metadata{
		Name:        "sys.cpu.combined.percent-normalized",
		Help:        "Current user+system cpu percentage, normalized 0-1 by number of cores",
		Measurement: "CPU Time",
		Unit:        metric.Unit_PERCENT,
	}
	metaRSSBytes = metric.Metadata{
		Name:        "sys.rss",
		Help:        "Current process RSS",
		Measurement: "RSS",
		Unit:        metric.Unit_BYTES,
	}
	metaFDOpen = metric.Metadata{
		Name:        "sys.fd.open",
		Help:        "Process open file descriptors",
		Measurement: "File Descriptors",
		Unit:        metric.Unit_COUNT,
	}
	metaFDSoftLimit = metric.Metadata{
		Name:        "sys.fd.softlimit",
		Help:        "Process open FD soft limit",
		Measurement: "File Descriptors",
		Unit:        metric.Unit_COUNT,
	}
	metaUptime = metric.Metadata{
		Name:        "sys.uptime",
		Help:        "Process uptime",
		Measurement: "Uptime",
		Unit:        metric.Unit_SECONDS,
	}

	// These disk and network stats are counters of the number of operations, packets, bytes, and
	// cumulative time of the disk and net IO that has been done across the whole host *since this
	// Cockroach process started up*. By taking the derivatives of these metrics, we can see the
	// IO throughput.
	metaHostDiskReadCount = metric.Metadata{
		Name:        "sys.host.disk.read.count",
		Unit:        metric.Unit_COUNT,
		Measurement: "Operations",
		Help:        "Disk read operations across all disks since this process started",
	}
	metaHostDiskReadBytes = metric.Metadata{
		Name:        "sys.host.disk.read.bytes",
		Unit:        metric.Unit_BYTES,
		Measurement: "Bytes",
		Help:        "Bytes read from all disks since this process started",
	}
	metaHostDiskReadTime = metric.Metadata{
		Name:        "sys.host.disk.read.time",
		Unit:        metric.Unit_NANOSECONDS,
		Measurement: "Time",
		Help:        "Time spent reading from all disks since this process started",
	}
	metaHostDiskWriteCount = metric.Metadata{
		Name:        "sys.host.disk.write.count",
		Unit:        metric.Unit_COUNT,
		Measurement: "Operations",
		Help:        "Disk write operations across all disks since this process started",
	}
	metaHostDiskWriteBytes = metric.Metadata{
		Name:        "sys.host.disk.write.bytes",
		Unit:        metric.Unit_BYTES,
		Measurement: "Bytes",
		Help:        "Bytes written to all disks since this process started",
	}
	metaHostDiskWriteTime = metric.Metadata{
		Name:        "sys.host.disk.write.time",
		Unit:        metric.Unit_NANOSECONDS,
		Measurement: "Time",
		Help:        "Time spent writing to all disks since this process started",
	}
	metaHostDiskIOTime = metric.Metadata{
		Name:        "sys.host.disk.io.time",
		Unit:        metric.Unit_NANOSECONDS,
		Measurement: "Time",
		Help:        "Time spent reading from or writing to all disks since this process started",
	}
	metaHostDiskWeightedIOTime = metric.Metadata{
		Name:        "sys.host.disk.weightedio.time",
		Unit:        metric.Unit_NANOSECONDS,
		Measurement: "Time",
		Help:        "Weighted time spent reading from or writing to to all disks since this process started",
	}
	metaHostIopsInProgress = metric.Metadata{
		Name:        "sys.host.disk.iopsinprogress",
		Unit:        metric.Unit_COUNT,
		Measurement: "Operations",
		Help:        "IO operations currently in progress on this host",
	}
	metaHostNetRecvBytes = metric.Metadata{
		Name:        "sys.host.net.recv.bytes",
		Unit:        metric.Unit_BYTES,
		Measurement: "Bytes",
		Help:        "Bytes received on all network interfaces since this process started",
	}
	metaHostNetRecvPackets = metric.Metadata{
		Name:        "sys.host.net.recv.packets",
		Unit:        metric.Unit_COUNT,
		Measurement: "Packets",
		Help:        "Packets received on all network interfaces since this process started",
	}
	metaHostNetSendBytes = metric.Metadata{
		Name:        "sys.host.net.send.bytes",
		Unit:        metric.Unit_BYTES,
		Measurement: "Bytes",
		Help:        "Bytes sent on all network interfaces since this process started",
	}
	metaHostNetSendPackets = metric.Metadata{
		Name:        "sys.host.net.send.packets",
		Unit:        metric.Unit_COUNT,
		Measurement: "Packets",
		Help:        "Packets sent on all network interfaces since this process started",
	}
)

// getCgoMemStats is a function that fetches stats for the C++ portion of the code.
// We will not necessarily have implementations for all builds, so check for nil first.
// Returns the following:
// allocated uint: bytes allocated by application
// total     uint: total bytes requested from system
// error           : any issues fetching stats. This should be a warning only.
var getCgoMemStats func(context.Context) (uint, uint, error)

// RuntimeStatSampler is used to periodically sample the runtime environment
// for useful statistics, performing some rudimentary calculations and storing
// the resulting information in a format that can be easily consumed by status
// logging systems.
type RuntimeStatSampler struct {
	clock *hlc.Clock

	startTimeNanos int64
	// The last sampled values of some statistics are kept only to compute
	// derivative statistics.
	last struct {
		now         int64
		utime       int64
		stime       int64
		cgoCall     int64
		gcCount     int64
		gcPauseTime uint64
		disk        diskStats
		net         net.IOCountersStat
		runnableSum float64
	}

	initialDiskCounters diskStats
	initialNetCounters  net.IOCountersStat

	// Only show "not implemented" errors once, we don't need the log spam.
	fdUsageNotImplemented bool

	// Metric gauges maintained by the sampler.
	// Go runtime stats.
	CgoCalls                 *metric.Gauge
	Goroutines               *metric.Gauge
	RunnableGoroutinesPerCPU *metric.GaugeFloat64
	GoAllocBytes             *metric.Gauge
	GoTotalBytes             *metric.Gauge
	CgoAllocBytes            *metric.Gauge
	CgoTotalBytes            *metric.Gauge
	GcCount                  *metric.Gauge
	GcPauseNS                *metric.Gauge
	GcPausePercent           *metric.GaugeFloat64
	// CPU stats.
	CPUUserNS              *metric.Gauge
	CPUUserPercent         *metric.GaugeFloat64
	CPUSysNS               *metric.Gauge
	CPUSysPercent          *metric.GaugeFloat64
	CPUCombinedPercentNorm *metric.GaugeFloat64
	// Memory stats.
	RSSBytes *metric.Gauge
	// File descriptor stats.
	FDOpen      *metric.Gauge
	FDSoftLimit *metric.Gauge
	// Disk and network stats.
	HostDiskReadBytes      *metric.Gauge
	HostDiskReadCount      *metric.Gauge
	HostDiskReadTime       *metric.Gauge
	HostDiskWriteBytes     *metric.Gauge
	HostDiskWriteCount     *metric.Gauge
	HostDiskWriteTime      *metric.Gauge
	HostDiskIOTime         *metric.Gauge
	HostDiskWeightedIOTime *metric.Gauge
	IopsInProgress         *metric.Gauge
	HostNetRecvBytes       *metric.Gauge
	HostNetRecvPackets     *metric.Gauge
	HostNetSendBytes       *metric.Gauge
	HostNetSendPackets     *metric.Gauge
	// Uptime and build.
	Uptime         *metric.Gauge // We use a gauge to be able to call Update.
	BuildTimestamp *metric.Gauge
}

// NewRuntimeStatSampler constructs a new RuntimeStatSampler object.
func NewRuntimeStatSampler(ctx context.Context, clock *hlc.Clock) *RuntimeStatSampler {
	// Construct the build info metric. It is constant.
	// We first build set the labels on the metadata.
	info := build.GetInfo()
	timestamp, err := info.Timestamp()
	if err != nil {
		// We can't panic here, tests don't have a build timestamp.
		log.Warningf(ctx, "could not parse build timestamp: %v", err)
	}

	// Build information.
	metaBuildTimestamp := metric.Metadata{
		Name:        "build.timestamp",
		Help:        "Build information",
		Measurement: "Build Time",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaBuildTimestamp.AddLabel("tag", info.Tag)
	metaBuildTimestamp.AddLabel("go_version", info.GoVersion)

	buildTimestamp := metric.NewGauge(metaBuildTimestamp)
	buildTimestamp.Update(timestamp)

	diskCounters, err := getSummedDiskCounters(ctx)
	if err != nil {
		log.Ops.Errorf(ctx, "could not get initial disk IO counters: %v", err)
	}
	netCounters, err := getSummedNetStats(ctx)
	if err != nil {
		log.Ops.Errorf(ctx, "could not get initial disk IO counters: %v", err)
	}

	rsr := &RuntimeStatSampler{
		clock:                    clock,
		startTimeNanos:           clock.PhysicalNow(),
		initialNetCounters:       netCounters,
		initialDiskCounters:      diskCounters,
		CgoCalls:                 metric.NewGauge(metaCgoCalls),
		Goroutines:               metric.NewGauge(metaGoroutines),
		RunnableGoroutinesPerCPU: metric.NewGaugeFloat64(metaRunnableGoroutinesPerCPU),
		GoAllocBytes:             metric.NewGauge(metaGoAllocBytes),
		GoTotalBytes:             metric.NewGauge(metaGoTotalBytes),
		CgoAllocBytes:            metric.NewGauge(metaCgoAllocBytes),
		CgoTotalBytes:            metric.NewGauge(metaCgoTotalBytes),
		GcCount:                  metric.NewGauge(metaGCCount),
		GcPauseNS:                metric.NewGauge(metaGCPauseNS),
		GcPausePercent:           metric.NewGaugeFloat64(metaGCPausePercent),
		CPUUserNS:                metric.NewGauge(metaCPUUserNS),
		CPUUserPercent:           metric.NewGaugeFloat64(metaCPUUserPercent),
		CPUSysNS:                 metric.NewGauge(metaCPUSysNS),
		CPUSysPercent:            metric.NewGaugeFloat64(metaCPUSysPercent),
		CPUCombinedPercentNorm:   metric.NewGaugeFloat64(metaCPUCombinedPercentNorm),
		RSSBytes:                 metric.NewGauge(metaRSSBytes),
		HostDiskReadBytes:        metric.NewGauge(metaHostDiskReadBytes),
		HostDiskReadCount:        metric.NewGauge(metaHostDiskReadCount),
		HostDiskReadTime:         metric.NewGauge(metaHostDiskReadTime),
		HostDiskWriteBytes:       metric.NewGauge(metaHostDiskWriteBytes),
		HostDiskWriteCount:       metric.NewGauge(metaHostDiskWriteCount),
		HostDiskWriteTime:        metric.NewGauge(metaHostDiskWriteTime),
		HostDiskIOTime:           metric.NewGauge(metaHostDiskIOTime),
		HostDiskWeightedIOTime:   metric.NewGauge(metaHostDiskWeightedIOTime),
		IopsInProgress:           metric.NewGauge(metaHostIopsInProgress),
		HostNetRecvBytes:         metric.NewGauge(metaHostNetRecvBytes),
		HostNetRecvPackets:       metric.NewGauge(metaHostNetRecvPackets),
		HostNetSendBytes:         metric.NewGauge(metaHostNetSendBytes),
		HostNetSendPackets:       metric.NewGauge(metaHostNetSendPackets),
		FDOpen:                   metric.NewGauge(metaFDOpen),
		FDSoftLimit:              metric.NewGauge(metaFDSoftLimit),
		Uptime:                   metric.NewGauge(metaUptime),
		BuildTimestamp:           buildTimestamp,
	}
	rsr.last.disk = rsr.initialDiskCounters
	rsr.last.net = rsr.initialNetCounters
	return rsr
}

// GoMemStats groups a runtime.MemStats structure with the timestamp when it
// was collected.
type GoMemStats struct {
	runtime.MemStats
	// Collected is the timestamp at which these values were collected.
	Collected time.Time
}

// CGoMemStats reports what has been allocated outside of Go.
type CGoMemStats struct {
	// CGoAllocated represents allocated bytes.
	CGoAllocatedBytes uint64
	// CGoTotal represents total bytes (allocated + metadata etc).
	CGoTotalBytes uint64
}

// GetCGoMemStats collects non-Go memory statistics.
func GetCGoMemStats(ctx context.Context) *CGoMemStats {
	var cgoAllocated, cgoTotal uint
	if getCgoMemStats != nil {
		var err error
		cgoAllocated, cgoTotal, err = getCgoMemStats(ctx)
		if err != nil {
			log.Warningf(ctx, "problem fetching CGO memory stats: %s; CGO stats will be empty.", err)
		}
	}
	return &CGoMemStats{
		CGoAllocatedBytes: uint64(cgoAllocated),
		CGoTotalBytes:     uint64(cgoTotal),
	}
}

// SampleEnvironment queries the runtime system for various interesting metrics,
// storing the resulting values in the set of metric gauges maintained by
// RuntimeStatSampler. This makes runtime statistics more convenient for
// consumption by the time series and status systems.
//
// This method should be called periodically by a higher level system in order
// to keep runtime statistics current.
//
// SampleEnvironment takes GoMemStats as input because that is collected
// separately, on a different schedule.
// The CGoMemStats should be provided via GetCGoMemStats().
func (rsr *RuntimeStatSampler) SampleEnvironment(
	ctx context.Context, ms *GoMemStats, cs *CGoMemStats,
) {
	// Note that debug.ReadGCStats() does not suffer the same problem as
	// runtime.ReadMemStats(). The only way you can know that is by reading the
	// source.
	gc := &debug.GCStats{}
	debug.ReadGCStats(gc)

	numCgoCall := runtime.NumCgoCall()
	numGoroutine := runtime.NumGoroutine()

	// Retrieve Mem and CPU statistics.
	pid := os.Getpid()
	mem := gosigar.ProcMem{}
	if err := mem.Get(pid); err != nil {
		log.Ops.Errorf(ctx, "unable to get mem usage: %v", err)
	}
	cpuTime := gosigar.ProcTime{}
	if err := cpuTime.Get(pid); err != nil {
		log.Ops.Errorf(ctx, "unable to get cpu usage: %v", err)
	}
	cgroupCPU, _ := cgroups.GetCgroupCPU()
	cpuShare := cgroupCPU.CPUShares()

	fds := gosigar.ProcFDUsage{}
	if err := fds.Get(pid); err != nil {
		if gosigar.IsNotImplemented(err) {
			if !rsr.fdUsageNotImplemented {
				rsr.fdUsageNotImplemented = true
				log.Ops.Warningf(ctx, "unable to get file descriptor usage (will not try again): %s", err)
			}
		} else {
			log.Ops.Errorf(ctx, "unable to get file descriptor usage: %s", err)
		}
	}

	var deltaDisk diskStats
	diskCounters, err := getSummedDiskCounters(ctx)
	if err != nil {
		log.Ops.Warningf(ctx, "problem fetching disk stats: %s; disk stats will be empty.", err)
	} else {
		deltaDisk = diskCounters
		subtractDiskCounters(&deltaDisk, rsr.last.disk)
		rsr.last.disk = diskCounters
		subtractDiskCounters(&diskCounters, rsr.initialDiskCounters)

		rsr.HostDiskReadBytes.Update(diskCounters.readBytes)
		rsr.HostDiskReadCount.Update(diskCounters.readCount)
		rsr.HostDiskReadTime.Update(int64(diskCounters.readTime))
		rsr.HostDiskWriteBytes.Update(diskCounters.writeBytes)
		rsr.HostDiskWriteCount.Update(diskCounters.writeCount)
		rsr.HostDiskWriteTime.Update(int64(diskCounters.writeTime))
		rsr.HostDiskIOTime.Update(int64(diskCounters.ioTime))
		rsr.HostDiskWeightedIOTime.Update(int64(diskCounters.weightedIOTime))
		rsr.IopsInProgress.Update(diskCounters.iopsInProgress)
	}

	var deltaNet net.IOCountersStat
	netCounters, err := getSummedNetStats(ctx)
	if err != nil {
		log.Ops.Warningf(ctx, "problem fetching net stats: %s; net stats will be empty.", err)
	} else {
		deltaNet = netCounters
		subtractNetworkCounters(&deltaNet, rsr.last.net)
		rsr.last.net = netCounters
		subtractNetworkCounters(&netCounters, rsr.initialNetCounters)

		rsr.HostNetSendBytes.Update(int64(netCounters.BytesSent))
		rsr.HostNetSendPackets.Update(int64(netCounters.PacketsSent))
		rsr.HostNetRecvBytes.Update(int64(netCounters.BytesRecv))
		rsr.HostNetRecvPackets.Update(int64(netCounters.PacketsRecv))
	}

	// Time statistics can be compared to the total elapsed time to create a
	// useful percentage of total CPU usage, which would be somewhat less accurate
	// if calculated later using downsampled time series data.
	now := rsr.clock.PhysicalNow()
	dur := float64(now - rsr.last.now)
	// cpuTime.{User,Sys} are in milliseconds, convert to nanoseconds.
	utime := int64(cpuTime.User) * 1e6
	stime := int64(cpuTime.Sys) * 1e6
	urate := float64(utime-rsr.last.utime) / dur
	srate := float64(stime-rsr.last.stime) / dur
	combinedNormalizedPerc := (srate + urate) / cpuShare
	gcPauseRatio := float64(uint64(gc.PauseTotal)-rsr.last.gcPauseTime) / dur
	runnableSum := goschedstats.CumulativeNormalizedRunnableGoroutines()
	// The number of runnable goroutines per CPU is a count, but it can vary
	// quickly. We don't just want to get a current snapshot of it, we want the
	// average value since the last sampling.
	runnableAvg := (runnableSum - rsr.last.runnableSum) * 1e9 / dur
	rsr.last.now = now
	rsr.last.utime = utime
	rsr.last.stime = stime
	rsr.last.gcPauseTime = uint64(gc.PauseTotal)
	rsr.last.runnableSum = runnableSum

	// Log summary of statistics to console.
	cgoRate := float64((numCgoCall-rsr.last.cgoCall)*int64(time.Second)) / dur
	goStatsStaleness := float32(timeutil.Now().Sub(ms.Collected)) / float32(time.Second)
	goTotal := ms.Sys - ms.HeapReleased

	log.StructuredEvent(ctx, &eventpb.RuntimeStats{
		MemRSSBytes:       mem.Resident,
		GoroutineCount:    uint64(numGoroutine),
		MemStackSysBytes:  ms.StackSys,
		GoAllocBytes:      ms.HeapAlloc,
		GoTotalBytes:      goTotal,
		GoStatsStaleness:  goStatsStaleness,
		HeapFragmentBytes: ms.HeapInuse - ms.HeapAlloc,
		HeapReservedBytes: ms.HeapIdle - ms.HeapReleased,
		HeapReleasedBytes: ms.HeapReleased,
		CGoAllocBytes:     cs.CGoAllocatedBytes,
		CGoTotalBytes:     cs.CGoTotalBytes,
		CGoCallRate:       float32(cgoRate),
		CPUUserPercent:    float32(urate) * 100,
		CPUSysPercent:     float32(srate) * 100,
		GCPausePercent:    float32(gcPauseRatio) * 100,
		GCRunCount:        uint64(gc.NumGC),
		NetHostRecvBytes:  deltaNet.BytesRecv,
		NetHostSendBytes:  deltaNet.BytesSent,
	})

	rsr.last.cgoCall = numCgoCall
	rsr.last.gcCount = gc.NumGC

	rsr.GoAllocBytes.Update(int64(ms.HeapAlloc))
	rsr.GoTotalBytes.Update(int64(goTotal))
	rsr.CgoCalls.Update(numCgoCall)
	rsr.Goroutines.Update(int64(numGoroutine))
	rsr.RunnableGoroutinesPerCPU.Update(runnableAvg)
	rsr.CgoAllocBytes.Update(int64(cs.CGoAllocatedBytes))
	rsr.CgoTotalBytes.Update(int64(cs.CGoTotalBytes))
	rsr.GcCount.Update(gc.NumGC)
	rsr.GcPauseNS.Update(int64(gc.PauseTotal))
	rsr.GcPausePercent.Update(gcPauseRatio)
	rsr.CPUUserNS.Update(utime)
	rsr.CPUUserPercent.Update(urate)
	rsr.CPUSysNS.Update(stime)
	rsr.CPUSysPercent.Update(srate)
	rsr.CPUCombinedPercentNorm.Update(combinedNormalizedPerc)
	rsr.FDOpen.Update(int64(fds.Open))
	rsr.FDSoftLimit.Update(int64(fds.SoftLimit))
	rsr.RSSBytes.Update(int64(mem.Resident))
	rsr.Uptime.Update((now - rsr.startTimeNanos) / 1e9)
}

// GetCPUCombinedPercentNorm is part of the rowexec.RuntimeStats interface.
func (rsr *RuntimeStatSampler) GetCPUCombinedPercentNorm() float64 {
	return rsr.CPUCombinedPercentNorm.Value()
}

// diskStats contains the disk statistics returned by the operating
// system. Interpretation of some of these stats varies by platform,
// although as much as possible they are normalized to the semantics
// used by linux's diskstats interface.
//
// Except for iopsInProgress, these metrics act like counters (always
// increasing, and best interpreted as a rate).
type diskStats struct {
	readBytes int64
	readCount int64

	// readTime (and writeTime) may increase more than 1s per second if
	// access to storage is parallelized.
	readTime time.Duration

	writeBytes int64
	writeCount int64
	writeTime  time.Duration

	// ioTime is the amount of time that iopsInProgress is non-zero (so
	// its increase is capped at 1s/s). Only available on linux.
	ioTime time.Duration

	// weightedIOTime is a linux-specific metric that attempts to
	// represent "an easy measure of both I/O completion time and the
	// backlog that may be accumulating."
	weightedIOTime time.Duration

	// iopsInProgress is a gauge of the number of pending IO operations.
	// Not available on macOS.
	iopsInProgress int64
}

func getSummedDiskCounters(ctx context.Context) (diskStats, error) {
	diskCounters, err := getDiskCounters(ctx)
	if err != nil {
		return diskStats{}, err
	}

	return sumDiskCounters(diskCounters), nil
}

func getSummedNetStats(ctx context.Context) (net.IOCountersStat, error) {
	netCounters, err := net.IOCountersWithContext(ctx, true /* per NIC */)
	if err != nil {
		return net.IOCountersStat{}, err
	}

	return sumNetworkCounters(netCounters), nil
}

// sumDiskCounters returns a new disk.IOCountersStat whose values are the sum of the
// values in the slice of disk.IOCountersStats passed in.
func sumDiskCounters(disksStats []diskStats) diskStats {
	output := diskStats{}
	for _, stats := range disksStats {
		output.readBytes += stats.readBytes
		output.readCount += stats.readCount
		output.readTime += stats.readTime

		output.writeBytes += stats.writeBytes
		output.writeCount += stats.writeCount
		output.writeTime += stats.writeTime

		output.ioTime += stats.ioTime
		output.weightedIOTime += stats.weightedIOTime

		output.iopsInProgress += stats.iopsInProgress
	}
	return output
}

// subtractDiskCounters subtracts the counters in `sub` from the counters in `from`,
// saving the results in `from`.
func subtractDiskCounters(from *diskStats, sub diskStats) {
	from.writeCount -= sub.writeCount
	from.writeBytes -= sub.writeBytes
	from.writeTime -= sub.writeTime

	from.readCount -= sub.readCount
	from.readBytes -= sub.readBytes
	from.readTime -= sub.readTime

	from.ioTime -= sub.ioTime
	from.weightedIOTime -= sub.weightedIOTime
}

// sumNetworkCounters returns a new net.IOCountersStat whose values are the sum of the
// values in the slice of net.IOCountersStats passed in.
func sumNetworkCounters(netCounters []net.IOCountersStat) net.IOCountersStat {
	output := net.IOCountersStat{}
	for _, counter := range netCounters {
		output.BytesRecv += counter.BytesRecv
		output.BytesSent += counter.BytesSent
		output.PacketsRecv += counter.PacketsRecv
		output.PacketsSent += counter.PacketsSent
	}
	return output
}

// subtractNetworkCounters subtracts the counters in `sub` from the counters in `from`,
// saving the results in `from`.
func subtractNetworkCounters(from *net.IOCountersStat, sub net.IOCountersStat) {
	from.BytesRecv -= sub.BytesRecv
	from.BytesSent -= sub.BytesSent
	from.PacketsRecv -= sub.PacketsRecv
	from.PacketsSent -= sub.PacketsSent
}
