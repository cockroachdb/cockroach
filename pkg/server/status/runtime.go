// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package status

import (
	"context"
	"fmt"
	"math"
	"os"
	"regexp"
	"runtime"
	"runtime/metrics"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/util/cgroups"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/goschedstats"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/elastic/gosigar"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/net"
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
	metaGoMemStackSysBytes = metric.Metadata{
		Name:        "sys.go.stack.systembytes",
		Help:        "Stack memory obtained from the OS.",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaGoHeapFragmentBytes = metric.Metadata{
		Name:        "sys.go.heap.heapfragmentbytes",
		Help:        "Total heap fragmentation bytes, derived from bytes in in-use spans minus bytes allocated",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaGoHeapReservedBytes = metric.Metadata{
		Name:        "sys.go.heap.heapreservedbytes",
		Help:        "Total bytes reserved by heap, derived from bytes in idle (unused) spans subtracts bytes returned to the OS",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaGoHeapReleasedBytes = metric.Metadata{
		Name:        "sys.go.heap.heapreleasedbytes",
		Help:        "Total bytes returned to the OS from heap.",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaGoTotalAllocBytes = metric.Metadata{
		Name:        "sys.go.heap.allocbytes",
		Help:        "Cumulative bytes allocated for heap objects.",
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
	metaGCStopNS = metric.Metadata{
		Name:        "sys.gc.stop.ns",
		Help:        "Estimated GC stop-the-world stopping latencies",
		Measurement: "GC Stopping",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaGCPausePercent = metric.Metadata{
		Name:        "sys.gc.pause.percent",
		Help:        "Current GC pause percentage",
		Measurement: "GC Pause",
		Unit:        metric.Unit_PERCENT,
	}
	metaGCAssistNS = metric.Metadata{
		Name:        "sys.gc.assist.ns",
		Help:        "Estimated total CPU time user goroutines spent to assist the GC process",
		Measurement: "CPU Time",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaNonGCPauseNS = metric.Metadata{
		Name:        "sys.go.pause.other.ns",
		Help:        "Estimated non-GC-related total pause time",
		Measurement: "Non-GC Pause",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaNonGCStopNS = metric.Metadata{
		Name:        "sys.go.stop.other.ns",
		Help:        "Estimated non-GC-related stop-the-world stopping latencies",
		Measurement: "Non-GC Stopping",
		Unit:        metric.Unit_NANOSECONDS,
	}

	metaCPUUserNS = metric.Metadata{
		Name:        "sys.cpu.user.ns",
		Help:        "Total user cpu time consumed by the CRDB process",
		Measurement: "CPU Time",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaCPUUserPercent = metric.Metadata{
		Name:        "sys.cpu.user.percent",
		Help:        "Current user cpu percentage consumed by the CRDB process",
		Measurement: "CPU Time",
		Unit:        metric.Unit_PERCENT,
	}
	metaCPUSysNS = metric.Metadata{
		Name:        "sys.cpu.sys.ns",
		Help:        "Total system cpu time consumed by the CRDB process",
		Measurement: "CPU Time",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaCPUSysPercent = metric.Metadata{
		Name:        "sys.cpu.sys.percent",
		Help:        "Current system cpu percentage consumed by the CRDB process",
		Measurement: "CPU Time",
		Unit:        metric.Unit_PERCENT,
	}
	metaCPUCombinedPercentNorm = metric.Metadata{
		Name:        "sys.cpu.combined.percent-normalized",
		Help:        "Current user+system cpu percentage consumed by the CRDB process, normalized 0-1 by number of cores",
		Measurement: "CPU Time",
		Unit:        metric.Unit_PERCENT,
	}
	metaCPUNowNS = metric.Metadata{
		Name:        "sys.cpu.now.ns",
		Help:        "The time when CPU measurements were taken, as nanoseconds since epoch",
		Measurement: "CPU Time",
		Unit:        metric.Unit_NANOSECONDS,
	}

	metaHostCPUCombinedPercentNorm = metric.Metadata{
		Name:        "sys.cpu.host.combined.percent-normalized",
		Help:        "Current user+system cpu percentage across the whole machine, normalized 0-1 by number of cores",
		Measurement: "CPU Time",
		Unit:        metric.Unit_PERCENT,
	}

	metaRSSBytes = metric.Metadata{
		Name:        "sys.rss",
		Help:        "Current process RSS",
		Measurement: "RSS",
		Unit:        metric.Unit_BYTES,
	}
	metaTotalMemBytes = metric.Metadata{
		Name:        "sys.totalmem",
		Help:        "Total memory (both free and used)",
		Measurement: "Memory",
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
		Help:        "Disk read operations across all disks since this process started (as reported by the OS)",
	}
	metaHostDiskReadBytes = metric.Metadata{
		Name:        "sys.host.disk.read.bytes",
		Unit:        metric.Unit_BYTES,
		Measurement: "Bytes",
		Help:        "Bytes read from all disks since this process started (as reported by the OS)",
	}
	metaHostDiskReadTime = metric.Metadata{
		Name:        "sys.host.disk.read.time",
		Unit:        metric.Unit_NANOSECONDS,
		Measurement: "Time",
		Help:        "Time spent reading from all disks since this process started (as reported by the OS)",
	}
	metaHostDiskWriteCount = metric.Metadata{
		Name:        "sys.host.disk.write.count",
		Unit:        metric.Unit_COUNT,
		Measurement: "Operations",
		Help:        "Disk write operations across all disks since this process started (as reported by the OS)",
	}
	metaHostDiskWriteBytes = metric.Metadata{
		Name:        "sys.host.disk.write.bytes",
		Unit:        metric.Unit_BYTES,
		Measurement: "Bytes",
		Help:        "Bytes written to all disks since this process started (as reported by the OS)",
	}
	metaHostDiskWriteTime = metric.Metadata{
		Name:        "sys.host.disk.write.time",
		Unit:        metric.Unit_NANOSECONDS,
		Measurement: "Time",
		Help:        "Time spent writing to all disks since this process started (as reported by the OS)",
	}
	metaHostDiskIOTime = metric.Metadata{
		Name:        "sys.host.disk.io.time",
		Unit:        metric.Unit_NANOSECONDS,
		Measurement: "Time",
		Help:        "Time spent reading from or writing to all disks since this process started (as reported by the OS)",
	}
	metaHostDiskWeightedIOTime = metric.Metadata{
		Name:        "sys.host.disk.weightedio.time",
		Unit:        metric.Unit_NANOSECONDS,
		Measurement: "Time",
		Help:        "Weighted time spent reading from or writing to all disks since this process started (as reported by the OS)",
	}
	metaHostIopsInProgress = metric.Metadata{
		Name:        "sys.host.disk.iopsinprogress",
		Unit:        metric.Unit_COUNT,
		Measurement: "Operations",
		Help:        "IO operations currently in progress on this host (as reported by the OS)",
	}
	metaHostNetRecvBytes = metric.Metadata{
		Name:        "sys.host.net.recv.bytes",
		Unit:        metric.Unit_BYTES,
		Measurement: "Bytes",
		Help:        "Bytes received on all network interfaces since this process started (as reported by the OS)",
	}
	metaHostNetRecvPackets = metric.Metadata{
		Name:        "sys.host.net.recv.packets",
		Unit:        metric.Unit_COUNT,
		Measurement: "Packets",
		Help:        "Packets received on all network interfaces since this process started (as reported by the OS)",
	}
	metaHostNetRecvErr = metric.Metadata{
		Name:        "sys.host.net.recv.err",
		Unit:        metric.Unit_COUNT,
		Measurement: "Packets",
		Help:        "Error receiving packets on all network interfaces since this process started (as reported by the OS)",
	}
	metaHostNetRecvDrop = metric.Metadata{
		Name:        "sys.host.net.recv.drop",
		Unit:        metric.Unit_COUNT,
		Measurement: "Packets",
		Help:        "Receiving packets that got dropped on all network interfaces since this process started (as reported by the OS)",
	}
	metaHostNetSendBytes = metric.Metadata{
		Name:        "sys.host.net.send.bytes",
		Unit:        metric.Unit_BYTES,
		Measurement: "Bytes",
		Help:        "Bytes sent on all network interfaces since this process started (as reported by the OS)",
	}
	metaHostNetSendPackets = metric.Metadata{
		Name:        "sys.host.net.send.packets",
		Unit:        metric.Unit_COUNT,
		Measurement: "Packets",
		Help:        "Packets sent on all network interfaces since this process started (as reported by the OS)",
	}
	metaHostNetSendErr = metric.Metadata{
		Name:        "sys.host.net.send.err",
		Unit:        metric.Unit_COUNT,
		Measurement: "Packets",
		Help:        "Error on sending packets on all network interfaces since this process started (as reported by the OS)",
	}
	metaHostNetSendDrop = metric.Metadata{
		Name:        "sys.host.net.send.drop",
		Unit:        metric.Unit_COUNT,
		Measurement: "Packets",
		Help:        "Sending packets that got dropped on all network interfaces since this process started (as reported by the OS)",
	}
)

// diskMetricsIgnoredDevices is a regex that matches any block devices that must be
// ignored for disk metrics (eg. sys.host.disk.write.bytes), as those devices
// have likely been counted elsewhere. This prevents us from double-counting,
// for instance, RAID volumes under both the logical volume and under the
// physical volume(s).
var diskMetricsIgnoredDevices = envutil.EnvOrDefaultString("COCKROACH_DISK_METRICS_IGNORED_DEVICES", getDefaultIgnoredDevices())

// getCgoMemStats is a function that fetches stats for the C++ portion of the code.
// We will not necessarily have implementations for all builds, so check for nil first.
// Returns the following:
// allocated uint: bytes allocated by application
// total     uint: total bytes requested from system
// error           : any issues fetching stats. This should be a warning only.
var getCgoMemStats func(context.Context) (cGoAlloc uint, cGoTotal uint, _ error)

// cgoMemMaybePurge checks if the current jemalloc overhead (relative to
// cgoAllocMem or cgoTargetMem, whichever is higher) is above overheadPercent;
// if it is, a purge of all arenas is performed. We perform at most a purge per
// minPeriod.
var cgoMemMaybePurge func(
	ctx context.Context,
	cgoAllocMem, cgoTotalMem, cgoTargetMem uint64,
	overheadPercent int,
	minPeriod time.Duration,
)

// Distribution of individual GC-related stop-the-world pause
// latencies. This is the time from deciding to stop the world
// until the world is started again. Some of this time is spent
// getting all threads to stop (this is measured directly in
// /sched/pauses/stopping/gc:seconds), during which some threads
// may still be running. Bucket counts increase monotonically.
const runtimeMetricGCPauseTotal = "/sched/pauses/total/gc:seconds"

// Distribution of individual GC-related stop-the-world stopping
// latencies. This is the time it takes from deciding to stop the
// world until all Ps are stopped. This is a subset of the total
// GC-related stop-the-world time (/sched/pauses/total/gc:seconds).
// During this time, some threads may be executing. Bucket counts
// increase monotonically.
const runtimeMetricGCStopTotal = "/sched/pauses/stopping/gc:seconds"

// Estimated total CPU time goroutines spent performing GC tasks to assist the
// GC and prevent it from falling behind the application. This metric is an
// overestimate, and not directly comparable to system CPU time measurements.
// Compare only with other /cpu/classes metrics.
const runtimeMetricGCAssist = "/cpu/classes/gc/mark/assist:cpu-seconds"

// Distribution of individual non-GC-related stop-the-world
// pause latencies. This is the time from deciding to stop the
// world until the world is started again. Some of this time
// is spent getting all threads to stop (measured directly in
// /sched/pauses/stopping/other:seconds). Bucket counts increase
// monotonically.
const runtimeMetricNonGCPauseTotal = "/sched/pauses/total/other:seconds"

// Distribution of individual non-GC-related stop-the-world
// stopping latencies. This is the time it takes from deciding
// to stop the world until all Ps are stopped. This is a
// subset of the total non-GC-related stop-the-world time
// (/sched/pauses/total/other:seconds). During this time, some
// threads may be executing. Bucket counts increase monotonically.
const runtimeMetricNonGCStopTotal = "/sched/pauses/stopping/other:seconds"

// Memory occupied by live objects and dead objects that have not
// yet been marked free by the garbage collector.
const runtimeMetricHeapAlloc = "/memory/classes/heap/objects:bytes"

// Cumulative sum of memory allocated to the heap by the
// application.
const runtimeMetricCumulativeAlloc = "/gc/heap/allocs:bytes"

// Memory that is reserved for heap objects but is not currently
// used to hold heap objects.
const runtimeMetricHeapFragmentBytes = "/memory/classes/heap/unused:bytes"

// Memory that is completely free and eligible to be returned to
// the underlying system, but has not been. This metric is the
// runtime's estimate of free address space that is backed by
// physical memory.
const runtimeMetricHeapReservedBytes = "/memory/classes/heap/free:bytes"

// Memory that is completely free and has been returned to the
// underlying system. This metric is the runtime's estimate of free
// address space that is still mapped into the process, but is not
// backed by physical memory.
const runtimeMetricHeapReleasedBytes = "/memory/classes/heap/released:bytes"

// Memory allocated from the heap that is reserved for stack space,
// whether or not it is currently in-use. Currently, this
// represents all stack memory for goroutines. It also includes all
// OS thread stacks in non-cgo programs. Note that stacks may be
// allocated differently in the future, and this may change.
const runtimeMetricMemStackHeapBytes = "/memory/classes/heap/stacks:bytes"

// Stack memory allocated by the underlying operating system.
// In non-cgo programs this metric is currently zero. This may
// change in the future. In cgo programs this metric includes
// OS thread stacks allocated directly from the OS. Currently,
// this only accounts for one stack in c-shared and c-archive build
// modes, and other sources of stacks from the OS are not measured.
// This too may change in the future.
const runtimeMetricMemStackOSBytes = "/memory/classes/os-stacks:bytes"

// All memory mapped by the Go runtime into the current process
// as read-write. Note that this does not include memory mapped
// by code called via cgo or via the syscall package. Sum of all
// metrics in /memory/classes.
const runtimeMetricGoTotal = "/memory/classes/total:bytes"

// Count of all completed GC cycles.
const runtimeMetricGCCount = "/gc/cycles/total:gc-cycles"

var runtimeMetrics = []string{
	runtimeMetricGCAssist,
	runtimeMetricGoTotal,
	runtimeMetricHeapAlloc,
	runtimeMetricHeapFragmentBytes,
	runtimeMetricHeapReservedBytes,
	runtimeMetricHeapReleasedBytes,
	runtimeMetricMemStackHeapBytes,
	runtimeMetricMemStackOSBytes,
	runtimeMetricCumulativeAlloc,
	runtimeMetricGCCount,
	runtimeMetricGCPauseTotal,
	runtimeMetricNonGCPauseTotal,
	runtimeMetricGCStopTotal,
	runtimeMetricNonGCStopTotal,
}

// GoRuntimeSampler are a collection of metrics to sample from golang's runtime environment and
// runtime metrics metadata. It fetches go runtime metrics and provides read access.
// https://pkg.go.dev/runtime/metrics
type GoRuntimeSampler struct {
	// The collection of metrics we want to sample.
	metricSamples []metrics.Sample
	// The mapping to find metric slot in metricSamples by name.
	metricIndexes map[string]int
}

// getIndex finds the position of metrics in the sample array by name.
func (grm *GoRuntimeSampler) getIndex(name string) int {
	i, found := grm.metricIndexes[name]
	if !found {
		panic(fmt.Sprintf("unsampled metric: %s", name))
	}
	return i
}

// uint64 gets the sampled value by metrics name as uint64.
// N.B. This method will panic if the metrics value is not metrics.KindUint64.
func (grm *GoRuntimeSampler) uint64(name string) uint64 {
	i := grm.getIndex(name)
	return grm.metricSamples[i].Value.Uint64()
}

// float64 gets the sampled value by metrics name as float64.
// N.B. This method will panic if the metrics value is not metrics.KindFloat64.
func (grm *GoRuntimeSampler) float64(name string) float64 {
	i := grm.getIndex(name)
	return grm.metricSamples[i].Value.Float64()
}

// float64Histogram gets the sampled value by metrics name as *metrics.Float64Histogram.
// N.B. This method will panic if the metrics value is not
// metrics.KindFloat64Histogram.
func (grm *GoRuntimeSampler) float64Histogram(name string) *metrics.Float64Histogram {
	i := grm.getIndex(name)
	return grm.metricSamples[i].Value.Float64Histogram()
}

// float64HistogramSum performs an estimated sum to the float64histogram.
// The sum is estimated by taking the average of the bucket boundaries *
// their count. Buckets with extreme boundary values such as -inf or inf
// will be normalized to the other non infinity boundary value.
func float64HistogramSum(h *metrics.Float64Histogram) float64 {
	estSum := 0.0
	if len(h.Buckets) == 2 && math.IsInf(h.Buckets[0], -1) && math.IsInf(h.Buckets[1], 1) {
		panic("unable to estimate from histogram with boundary: [-inf, inf]")
	}
	var start, end float64 // start and end of current bucket
	for i := 0; i <= len(h.Counts)-1; i++ {
		start, end = h.Buckets[i], h.Buckets[i+1]
		if math.IsInf(start, -1) { // -Inf
			// Avoid interpolating with infinity by replacing it with
			// the right boundary value.
			start = end
		}
		if math.IsInf(end, 1) { // +Inf
			// Avoid interpolating with infinity by replacing it with
			// the left boundary value.
			end = start
		}
		estBucketValue := start
		if end != start {
			estBucketValue += (end - start) / 2.0
		}
		estSum += estBucketValue * float64(h.Counts[i])
	}
	return estSum
}

// sampleRuntimeMetrics reads from metrics.Read api and fill in the value
// in the metricSamples field.
// Benchmark results on 12 core Apple M3 Pro:
// goos: darwin
// goarch: arm64
// pkg: github.com/cockroachdb/cockroach/pkg/server/status
// BenchmarkGoRuntimeSampler
// BenchmarkGoRuntimeSampler-12    	28886398	        40.03 ns/op
//
//	func BenchmarkGoRuntimeSampler(b *testing.B) {
//		 s := NewGoRuntimeSampler([]string{runtimeMetricGCAssist})
//		 for n := 0; n < b.N; n++ {
//			 s.sampleRuntimeMetrics()
//		 }
//	}
func (grm *GoRuntimeSampler) sampleRuntimeMetrics() {
	metrics.Read(grm.metricSamples)
}

// NewGoRuntimeSampler constructs a new GoRuntimeSampler object.
// This method will panic on invalid metrics names provided.
func NewGoRuntimeSampler(metricNames []string) *GoRuntimeSampler {
	m := metrics.All()
	metricTypes := make(map[string]metrics.ValueKind, len(m))
	for _, desc := range m {
		metricTypes[desc.Name] = desc.Kind
	}
	metricSamples := make([]metrics.Sample, len(metricNames))
	metricIndexes := make(map[string]int, len(metricNames))
	for i, n := range metricNames {
		_, hasDesc := metricTypes[n]
		if !hasDesc {
			panic(fmt.Sprintf("unexpected metric: %s", n))
		}
		metricSamples[i] = metrics.Sample{Name: n}
		metricIndexes[n] = i
	}

	grm := &GoRuntimeSampler{
		metricSamples: metricSamples,
		metricIndexes: metricIndexes,
	}
	return grm
}

// RuntimeStatSampler is used to periodically sample the runtime environment
// for useful statistics, performing some rudimentary calculations and storing
// the resulting information in a format that can be easily consumed by status
// logging systems.
type RuntimeStatSampler struct {
	clock hlc.WallClock

	startTimeNanos int64
	// The last sampled values of some statistics are kept only to compute
	// derivative statistics.
	last struct {
		now int64

		// CPU usage for the CRDB process.
		procUtime int64
		procStime int64
		// CPU usage for the whole system.
		hostUtime int64
		hostStime int64

		cgoCall     int64
		gcCount     int64
		gcPauseTime uint64
		disk        DiskStats
		net         net.IOCountersStat
		runnableSum float64
	}

	initialDiskCounters DiskStats
	initialNetCounters  net.IOCountersStat

	// Only show "not implemented" errors once, we don't need the log spam.
	fdUsageNotImplemented bool

	goRuntimeSampler *GoRuntimeSampler

	// Metric gauges maintained by the sampler.
	// Go runtime stats.
	CgoCalls                 *metric.Counter
	Goroutines               *metric.Gauge
	RunnableGoroutinesPerCPU *metric.GaugeFloat64
	GoAllocBytes             *metric.Gauge
	GoTotalBytes             *metric.Gauge
	GoMemStackSysBytes       *metric.Gauge
	GoHeapFragmentBytes      *metric.Gauge
	GoHeapReservedBytes      *metric.Gauge
	GoHeapReleasedBytes      *metric.Gauge
	GoTotalAllocBytes        *metric.Counter
	CgoAllocBytes            *metric.Gauge
	CgoTotalBytes            *metric.Gauge
	GcCount                  *metric.Counter
	GcPauseNS                *metric.Counter
	NonGcPauseNS             *metric.Gauge
	GcStopNS                 *metric.Gauge
	NonGcStopNS              *metric.Gauge
	GcPausePercent           *metric.GaugeFloat64
	GcAssistNS               *metric.Counter
	// CPU stats for the CRDB process usage.
	CPUUserNS              *metric.Counter
	CPUUserPercent         *metric.GaugeFloat64
	CPUSysNS               *metric.Counter
	CPUSysPercent          *metric.GaugeFloat64
	CPUCombinedPercentNorm *metric.GaugeFloat64
	CPUNowNS               *metric.Counter
	// CPU stats for the CRDB process usage.
	HostCPUCombinedPercentNorm *metric.GaugeFloat64
	// Memory stats.
	RSSBytes      *metric.Gauge
	TotalMemBytes *metric.Gauge
	// File descriptor stats.
	FDOpen      *metric.Gauge
	FDSoftLimit *metric.Gauge
	// Disk and network stats.
	HostDiskReadBytes      *metric.Counter
	HostDiskReadCount      *metric.Counter
	HostDiskReadTime       *metric.Counter
	HostDiskWriteBytes     *metric.Counter
	HostDiskWriteCount     *metric.Counter
	HostDiskWriteTime      *metric.Counter
	HostDiskIOTime         *metric.Counter
	HostDiskWeightedIOTime *metric.Counter
	IopsInProgress         *metric.Gauge
	HostNetRecvBytes       *metric.Counter
	HostNetRecvPackets     *metric.Counter
	HostNetRecvErr         *metric.Counter
	HostNetRecvDrop        *metric.Counter
	HostNetSendBytes       *metric.Counter
	HostNetSendPackets     *metric.Counter
	HostNetSendErr         *metric.Counter
	HostNetSendDrop        *metric.Counter
	// Uptime and build.
	Uptime         *metric.Counter
	BuildTimestamp *metric.Gauge
}

// NewRuntimeStatSampler constructs a new RuntimeStatSampler object.
func NewRuntimeStatSampler(ctx context.Context, clock hlc.WallClock) *RuntimeStatSampler {
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
		log.Ops.Errorf(ctx, "could not get initial network stat counters: %v", err)
	}

	rsr := &RuntimeStatSampler{
		clock:                    clock,
		startTimeNanos:           clock.Now().UnixNano(),
		initialNetCounters:       netCounters,
		initialDiskCounters:      diskCounters,
		goRuntimeSampler:         NewGoRuntimeSampler(runtimeMetrics),
		CgoCalls:                 metric.NewCounter(metaCgoCalls),
		Goroutines:               metric.NewGauge(metaGoroutines),
		RunnableGoroutinesPerCPU: metric.NewGaugeFloat64(metaRunnableGoroutinesPerCPU),
		GoAllocBytes:             metric.NewGauge(metaGoAllocBytes),
		GoTotalBytes:             metric.NewGauge(metaGoTotalBytes),
		GoMemStackSysBytes:       metric.NewGauge(metaGoMemStackSysBytes),
		GoHeapFragmentBytes:      metric.NewGauge(metaGoHeapFragmentBytes),
		GoHeapReservedBytes:      metric.NewGauge(metaGoHeapReservedBytes),
		GoHeapReleasedBytes:      metric.NewGauge(metaGoHeapReleasedBytes),
		GoTotalAllocBytes:        metric.NewCounter(metaGoTotalAllocBytes),
		CgoAllocBytes:            metric.NewGauge(metaCgoAllocBytes),
		CgoTotalBytes:            metric.NewGauge(metaCgoTotalBytes),
		GcCount:                  metric.NewCounter(metaGCCount),
		GcPauseNS:                metric.NewCounter(metaGCPauseNS),
		GcStopNS:                 metric.NewGauge(metaGCStopNS),
		GcPausePercent:           metric.NewGaugeFloat64(metaGCPausePercent),
		GcAssistNS:               metric.NewCounter(metaGCAssistNS),
		NonGcPauseNS:             metric.NewGauge(metaNonGCPauseNS),
		NonGcStopNS:              metric.NewGauge(metaNonGCStopNS),

		CPUUserNS:              metric.NewCounter(metaCPUUserNS),
		CPUUserPercent:         metric.NewGaugeFloat64(metaCPUUserPercent),
		CPUSysNS:               metric.NewCounter(metaCPUSysNS),
		CPUSysPercent:          metric.NewGaugeFloat64(metaCPUSysPercent),
		CPUCombinedPercentNorm: metric.NewGaugeFloat64(metaCPUCombinedPercentNorm),
		CPUNowNS:               metric.NewCounter(metaCPUNowNS),

		HostCPUCombinedPercentNorm: metric.NewGaugeFloat64(metaHostCPUCombinedPercentNorm),

		RSSBytes:               metric.NewGauge(metaRSSBytes),
		TotalMemBytes:          metric.NewGauge(metaTotalMemBytes),
		HostDiskReadBytes:      metric.NewCounter(metaHostDiskReadBytes),
		HostDiskReadCount:      metric.NewCounter(metaHostDiskReadCount),
		HostDiskReadTime:       metric.NewCounter(metaHostDiskReadTime),
		HostDiskWriteBytes:     metric.NewCounter(metaHostDiskWriteBytes),
		HostDiskWriteCount:     metric.NewCounter(metaHostDiskWriteCount),
		HostDiskWriteTime:      metric.NewCounter(metaHostDiskWriteTime),
		HostDiskIOTime:         metric.NewCounter(metaHostDiskIOTime),
		HostDiskWeightedIOTime: metric.NewCounter(metaHostDiskWeightedIOTime),
		IopsInProgress:         metric.NewGauge(metaHostIopsInProgress),
		HostNetRecvBytes:       metric.NewCounter(metaHostNetRecvBytes),
		HostNetRecvPackets:     metric.NewCounter(metaHostNetRecvPackets),
		HostNetRecvErr:         metric.NewCounter(metaHostNetRecvErr),
		HostNetRecvDrop:        metric.NewCounter(metaHostNetRecvDrop),
		HostNetSendBytes:       metric.NewCounter(metaHostNetSendBytes),
		HostNetSendPackets:     metric.NewCounter(metaHostNetSendPackets),
		HostNetSendErr:         metric.NewCounter(metaHostNetSendErr),
		HostNetSendDrop:        metric.NewCounter(metaHostNetSendDrop),
		FDOpen:                 metric.NewGauge(metaFDOpen),
		FDSoftLimit:            metric.NewGauge(metaFDSoftLimit),
		Uptime:                 metric.NewCounter(metaUptime),
		BuildTimestamp:         buildTimestamp,
	}
	rsr.last.disk = rsr.initialDiskCounters
	rsr.last.net = rsr.initialNetCounters
	return rsr
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

// CGoMemMaybePurge checks if the current allocator overhead (relative to
// cgoAllocMem or cgoTargetMem, whichever is higher) is above overheadPercent;
// if it is, a purge of all arenas is performed. We perform at most a purge per
// minPeriod.
func CGoMemMaybePurge(
	ctx context.Context,
	cgoAllocMem, cgoTotalMem, cgoTargetMem uint64,
	overheadPercent int,
	minPeriod time.Duration,
) {
	if cgoMemMaybePurge != nil {
		cgoMemMaybePurge(ctx, cgoAllocMem, cgoTotalMem, cgoTargetMem, overheadPercent, minPeriod)
	}
}

var netstatEvery = log.Every(time.Minute)

// SampleEnvironment queries the runtime system for various interesting metrics,
// storing the resulting values in the set of metric gauges maintained by
// RuntimeStatSampler. This makes runtime statistics more convenient for
// consumption by the time series and status systems.
//
// This method should be called periodically by a higher level system in order
// to keep runtime statistics current.
// The CGoMemStats should be provided via GetCGoMemStats().
func (rsr *RuntimeStatSampler) SampleEnvironment(ctx context.Context, cs *CGoMemStats) {
	rsr.goRuntimeSampler.sampleRuntimeMetrics()

	numCgoCall := runtime.NumCgoCall()
	numGoroutine := runtime.NumGoroutine()

	// Retrieve Mem and CPU statistics.
	pid := os.Getpid()
	mem := gosigar.ProcMem{}
	if err := mem.Get(pid); err != nil {
		log.Ops.Errorf(ctx, "unable to get mem usage: %v", err)
	}
	userTimeMillis, sysTimeMillis, err := GetProcCPUTime(ctx)
	if err != nil {
		log.Ops.Errorf(ctx, "unable to get process CPU usage: %v", err)
	}
	cpuCapacity := getCPUCapacity()
	cpuUsageStats, err := cpu.Times(false /* percpu */)
	if err != nil {
		log.Ops.Errorf(ctx, "unable to get system CPU usage: %v", err)
	}
	var cpuUsage cpu.TimesStat
	if len(cpuUsageStats) > 0 {
		cpuUsage = cpuUsageStats[0]
	}
	numHostCPUs, err := cpu.Counts(true /* logical */)
	if err != nil {
		log.Ops.Errorf(ctx, "unable to get system CPU details: %v", err)
	}

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

	diskCounters, err := getSummedDiskCounters(ctx)
	if err != nil {
		log.Ops.Warningf(ctx, "problem fetching disk stats: %s; disk stats will be empty.", err)
	} else {
		rsr.last.disk = diskCounters
		subtractDiskCounters(&diskCounters, rsr.initialDiskCounters)

		rsr.HostDiskReadBytes.Update(diskCounters.ReadBytes)
		rsr.HostDiskReadCount.Update(diskCounters.readCount)
		rsr.HostDiskReadTime.Update(int64(diskCounters.readTime))
		rsr.HostDiskWriteBytes.Update(diskCounters.WriteBytes)
		rsr.HostDiskWriteCount.Update(diskCounters.writeCount)
		rsr.HostDiskWriteTime.Update(int64(diskCounters.writeTime))
		rsr.HostDiskIOTime.Update(int64(diskCounters.ioTime))
		rsr.HostDiskWeightedIOTime.Update(int64(diskCounters.weightedIOTime))
		rsr.IopsInProgress.Update(diskCounters.iopsInProgress)
	}

	var deltaNet net.IOCountersStat
	netCounters, err := getSummedNetStats(ctx)
	if err != nil {
		if netstatEvery.ShouldLog() {
			log.Ops.Warningf(ctx, "problem fetching net stats: %s; net stats will be empty.", err)
		}
	} else {
		deltaNet = netCounters
		subtractNetworkCounters(&deltaNet, rsr.last.net)
		rsr.last.net = netCounters
		subtractNetworkCounters(&netCounters, rsr.initialNetCounters)
		rsr.HostNetRecvBytes.Update(int64(netCounters.BytesRecv))
		rsr.HostNetRecvPackets.Update(int64(netCounters.PacketsRecv))
		rsr.HostNetRecvErr.Update(int64(netCounters.Errin))
		rsr.HostNetRecvDrop.Update(int64(netCounters.Dropin))
		rsr.HostNetSendBytes.Update(int64(netCounters.BytesSent))
		rsr.HostNetSendPackets.Update(int64(netCounters.PacketsSent))
		rsr.HostNetSendErr.Update(int64(netCounters.Errout))
		rsr.HostNetSendDrop.Update(int64(netCounters.Dropout))
	}

	// Time statistics can be compared to the total elapsed time to create a
	// useful percentage of total CPU usage, which would be somewhat less accurate
	// if calculated later using downsampled time series data.
	now := rsr.clock.Now().UnixNano()
	dur := float64(now - rsr.last.now)
	// hostUtime.{User,Sys} are in milliseconds, convert to nanoseconds.
	procUtime := userTimeMillis * 1e6
	procStime := sysTimeMillis * 1e6
	// System CPU usage is in seconds, convert to nanoseconds.
	hostUtime := int64(cpuUsage.User * 1.e9)
	hostStime := int64(cpuUsage.System * 1.e9)

	var procUrate, procSrate, hostUrate, hostSrate float64
	if rsr.last.now != 0 { // We cannot compute these rates on the first iteration.
		procUrate = float64(procUtime-rsr.last.procUtime) / dur
		procSrate = float64(procStime-rsr.last.procStime) / dur
		hostUrate = float64(hostUtime-rsr.last.hostUtime) / dur
		hostSrate = float64(hostStime-rsr.last.hostStime) / dur
	}

	combinedNormalizedProcPerc := (procSrate + procUrate) / cpuCapacity
	combinedNormalizedHostPerc := (hostSrate + hostUrate) / float64(numHostCPUs)

	gcPauseTotal := float64HistogramSum(rsr.goRuntimeSampler.float64Histogram(runtimeMetricGCPauseTotal))
	nonGcPauseTotal := float64HistogramSum(rsr.goRuntimeSampler.float64Histogram(runtimeMetricNonGCPauseTotal))
	gcStopTotal := float64HistogramSum(rsr.goRuntimeSampler.float64Histogram(runtimeMetricGCStopTotal))
	nonGcStopTotal := float64HistogramSum(rsr.goRuntimeSampler.float64Histogram(runtimeMetricNonGCStopTotal))
	gcPauseTotalNs := uint64(gcPauseTotal * 1.e9)
	nonGcPauseTotalNs := int64(nonGcPauseTotal * 1.e9)
	gcStopTotalNs := int64(gcStopTotal * 1.e9)
	nonGcStopTotalNs := int64(nonGcStopTotal * 1.e9)
	gcCount := rsr.goRuntimeSampler.uint64(runtimeMetricGCCount)
	gcPauseRatio := float64(gcPauseTotalNs-rsr.last.gcPauseTime) / dur
	runnableSum := goschedstats.CumulativeNormalizedRunnableGoroutines()
	gcAssistSeconds := rsr.goRuntimeSampler.float64(runtimeMetricGCAssist)
	gcAssistNS := int64(gcAssistSeconds * 1e9)
	// The number of runnable goroutines per CPU is a count, but it can vary
	// quickly. We don't just want to get a current snapshot of it, we want the
	// average value since the last sampling.
	runnableAvg := (runnableSum - rsr.last.runnableSum) * 1e9 / dur
	rsr.last.now = now
	rsr.last.procUtime = procUtime
	rsr.last.procStime = procStime
	rsr.last.hostUtime = hostUtime
	rsr.last.hostStime = hostStime
	rsr.last.gcPauseTime = gcPauseTotalNs
	rsr.last.runnableSum = runnableSum

	// Log summary of statistics to console.
	osStackBytes := rsr.goRuntimeSampler.uint64(runtimeMetricMemStackOSBytes)
	cgoRate := float64((numCgoCall-rsr.last.cgoCall)*int64(time.Second)) / dur
	goAlloc := rsr.goRuntimeSampler.uint64(runtimeMetricHeapAlloc)
	goTotal := rsr.goRuntimeSampler.uint64(runtimeMetricGoTotal) -
		rsr.goRuntimeSampler.uint64(runtimeMetricHeapReleasedBytes)
	stackTotal := rsr.goRuntimeSampler.uint64(runtimeMetricMemStackHeapBytes) +
		osStackBytes
	heapFragmentBytes := rsr.goRuntimeSampler.uint64(runtimeMetricHeapFragmentBytes)
	heapReservedBytes := rsr.goRuntimeSampler.uint64(runtimeMetricHeapReservedBytes)
	heapReleasedBytes := rsr.goRuntimeSampler.uint64(runtimeMetricHeapReleasedBytes)
	stats := &eventpb.RuntimeStats{
		MemRSSBytes:       mem.Resident,
		GoroutineCount:    uint64(numGoroutine),
		MemStackSysBytes:  stackTotal,
		GoAllocBytes:      goAlloc,
		GoTotalBytes:      goTotal,
		HeapFragmentBytes: heapFragmentBytes,
		HeapReservedBytes: heapReservedBytes,
		HeapReleasedBytes: heapReleasedBytes,
		CGoAllocBytes:     cs.CGoAllocatedBytes,
		CGoTotalBytes:     cs.CGoTotalBytes,
		CGoCallRate:       float32(cgoRate),
		CPUUserPercent:    float32(procUrate) * 100,
		CPUSysPercent:     float32(procSrate) * 100,
		GCPausePercent:    float32(gcPauseRatio) * 100,
		GCRunCount:        gcCount,
		NetHostRecvBytes:  deltaNet.BytesRecv,
		NetHostSendBytes:  deltaNet.BytesSent,
	}

	logStats(ctx, stats)

	rsr.last.cgoCall = numCgoCall
	rsr.last.gcCount = int64(gcCount)

	rsr.GoAllocBytes.Update(int64(goAlloc))
	rsr.GoTotalBytes.Update(int64(goTotal))
	rsr.GoMemStackSysBytes.Update(int64(osStackBytes))
	rsr.GoHeapFragmentBytes.Update(int64(heapFragmentBytes))
	rsr.GoHeapReservedBytes.Update(int64(heapReservedBytes))
	rsr.GoHeapReleasedBytes.Update(int64(heapReleasedBytes))
	rsr.GoTotalAllocBytes.Update(int64(rsr.goRuntimeSampler.uint64(runtimeMetricCumulativeAlloc)))
	rsr.CgoCalls.Update(numCgoCall)
	rsr.Goroutines.Update(int64(numGoroutine))
	rsr.RunnableGoroutinesPerCPU.Update(runnableAvg)
	rsr.CgoAllocBytes.Update(int64(cs.CGoAllocatedBytes))
	rsr.CgoTotalBytes.Update(int64(cs.CGoTotalBytes))
	rsr.GcCount.Update(int64(gcCount))
	rsr.GcPauseNS.Update(int64(gcPauseTotalNs))
	rsr.GcStopNS.Update(gcStopTotalNs)
	rsr.GcPausePercent.Update(gcPauseRatio)
	rsr.GcAssistNS.Update(gcAssistNS)
	rsr.NonGcPauseNS.Update(nonGcPauseTotalNs)
	rsr.NonGcStopNS.Update(nonGcStopTotalNs)

	rsr.CPUUserNS.Update(procUtime)
	rsr.CPUUserPercent.Update(procUrate)
	rsr.CPUSysNS.Update(procStime)
	rsr.CPUSysPercent.Update(procSrate)
	rsr.CPUCombinedPercentNorm.Update(combinedNormalizedProcPerc)
	rsr.CPUNowNS.Update(now)
	rsr.HostCPUCombinedPercentNorm.Update(combinedNormalizedHostPerc)

	rsr.FDOpen.Update(int64(fds.Open))
	rsr.FDSoftLimit.Update(int64(fds.SoftLimit))
	rsr.RSSBytes.Update(int64(mem.Resident))
	totalMem, _, _ := GetTotalMemoryWithoutLogging()
	rsr.TotalMemBytes.Update(totalMem)
	rsr.Uptime.Update((now - rsr.startTimeNanos) / 1e9)
}

// GetCPUCombinedPercentNorm is part of the rowexec.RuntimeStats interface.
func (rsr *RuntimeStatSampler) GetCPUCombinedPercentNorm() float64 {
	return rsr.CPUCombinedPercentNorm.Value()
}

// DiskStats contains the disk statistics returned by the operating
// system. Interpretation of some of these stats varies by platform,
// although as much as possible they are normalized to the semantics
// used by linux's diskstats interface.
//
// Except for iopsInProgress, these metrics act like counters (always
// increasing, and best interpreted as a rate).
type DiskStats struct {
	// Name is the disk name.
	Name string
	// ReadBytes is the cumulative bytes read.
	ReadBytes int64
	readCount int64

	// readTime (and writeTime) may increase more than 1s per second if
	// access to storage is parallelized.
	readTime time.Duration

	// WriteBytes is the cumulative bytes written.
	WriteBytes int64
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

func getSummedDiskCounters(ctx context.Context) (DiskStats, error) {
	diskCounters, err := GetDiskCounters(ctx)
	if err != nil {
		return DiskStats{}, err
	}

	return sumAndFilterDiskCounters(diskCounters)
}

func getSummedNetStats(ctx context.Context) (net.IOCountersStat, error) {
	netCounters, err := net.IOCountersWithContext(ctx, true /* per NIC */)
	if err != nil {
		return net.IOCountersStat{}, err
	}

	return sumNetworkCounters(netCounters), nil
}

// sumAndFilterDiskCounters returns a new disk.IOCountersStat whose values are
// the sum of the values in the slice of disk.IOCountersStats passed in. It
// filters out any disk counters that are likely reflecting values already
// counted elsewhere, eg. md* logical volumes that are created out of RAIDing
// underlying drives. The filtering regex defaults to a platform-dependent one.
func sumAndFilterDiskCounters(disksStats []DiskStats) (DiskStats, error) {
	output := DiskStats{}
	var ignored *regexp.Regexp
	if diskMetricsIgnoredDevices != "" {
		var err error
		ignored, err = regexp.Compile(diskMetricsIgnoredDevices)
		if err != nil {
			return output, err
		}
	}

	for _, stats := range disksStats {
		if ignored != nil && ignored.MatchString(stats.Name) {
			continue
		}
		output.ReadBytes += stats.ReadBytes
		output.readCount += stats.readCount
		output.readTime += stats.readTime

		output.WriteBytes += stats.WriteBytes
		output.writeCount += stats.writeCount
		output.writeTime += stats.writeTime

		output.ioTime += stats.ioTime
		output.weightedIOTime += stats.weightedIOTime

		output.iopsInProgress += stats.iopsInProgress
	}
	return output, nil
}

// subtractDiskCounters subtracts the counters in `sub` from the counters in `from`,
// saving the results in `from`.
func subtractDiskCounters(from *DiskStats, sub DiskStats) {
	from.writeCount -= sub.writeCount
	from.WriteBytes -= sub.WriteBytes
	from.writeTime -= sub.writeTime

	from.readCount -= sub.readCount
	from.ReadBytes -= sub.ReadBytes
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
		output.PacketsRecv += counter.PacketsRecv
		output.Errin += counter.Errin
		output.Dropin += counter.Dropin
		output.BytesSent += counter.BytesSent
		output.PacketsSent += counter.PacketsSent
		output.Errout += counter.Errout
		output.Dropout += counter.Dropout
	}
	return output
}

// subtractNetworkCounters subtracts the counters in `sub` from the counters in `from`,
// saving the results in `from`.
func subtractNetworkCounters(from *net.IOCountersStat, sub net.IOCountersStat) {
	from.BytesRecv -= sub.BytesRecv
	from.PacketsRecv -= sub.PacketsRecv
	from.Errin -= sub.Errin
	from.Dropin -= sub.Dropin
	from.BytesSent -= sub.BytesSent
	from.PacketsSent -= sub.PacketsSent
	from.Errout -= sub.Errout
	from.Dropout -= sub.Dropout
}

// GetProcCPUTime returns the cumulative user/system time (in ms) since the process start.
func GetProcCPUTime(ctx context.Context) (userTimeMillis, sysTimeMillis int64, err error) {
	pid := os.Getpid()
	cpuTime := gosigar.ProcTime{}
	if err := cpuTime.Get(pid); err != nil {
		return 0, 0, err
	}
	return int64(cpuTime.User), int64(cpuTime.Sys), nil
}

// getCPUCapacity returns the number of logical CPU processors available for
// use by the process. The capacity accounts for cgroup constraints, GOMAXPROCS
// and the number of host processors.
func getCPUCapacity() float64 {
	numProcs := float64(runtime.GOMAXPROCS(0 /* read only */))
	cgroupCPU, err := cgroups.GetCgroupCPU()
	if err != nil {
		// Return the GOMAXPROCS value if unable to read the cgroup settings. This
		// can happen if cockroach is not running inside a CPU cgroup, which is a
		// supported deployment mode. We could log here, but we don't to avoid spam.
		return numProcs
	}
	cpuShare := cgroupCPU.CPUShares()
	// Take the minimum of the CPU shares and the GOMAXPROCS value. The most CPU
	// the process could use is the lesser of the two.
	if cpuShare > numProcs {
		return numProcs
	}
	return cpuShare
}
