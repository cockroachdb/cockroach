// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package status

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"regexp"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/util/cgroups"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/goschedstats"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/system"
	"github.com/cockroachdb/errors"
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
		Visibility:  metric.Metadata_SUPPORT,
	}
	metaRunnableGoroutinesPerCPU = metric.Metadata{
		Name:        "sys.runnable.goroutines.per.cpu",
		Help:        "Average number of goroutines that are waiting to run, normalized by number of cores",
		Measurement: "goroutines",
		Unit:        metric.Unit_COUNT,

		Visibility: metric.Metadata_ESSENTIAL,
		Category:   metric.Metadata_HARDWARE,
		HowToUse:   `If this metric has a value over 30, it indicates a CPU overload. If the condition lasts a short period of time (a few seconds), the database users are likely to experience inconsistent response times. If the condition persists for an extended period of time (tens of seconds, or minutes) the cluster may start developing stability issues. Review CPU planning.`}
	metaGoAllocBytes = metric.Metadata{
		Name:        "sys.go.allocbytes",
		Help:        "Current bytes of memory allocated by go",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
		Visibility:  metric.Metadata_SUPPORT,
	}
	metaGoTotalBytes = metric.Metadata{
		Name:        "sys.go.totalbytes",
		Help:        "Total bytes of memory allocated by go, but not released",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
		Visibility:  metric.Metadata_SUPPORT,
	}
	metaGoLimitBytes = metric.Metadata{
		Name:        "sys.go.limitbytes",
		Help:        "Go soft memory limit",
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
		Visibility:  metric.Metadata_SUPPORT,
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
		Visibility:  metric.Metadata_SUPPORT,
	}
	metaGoHeapObjects = metric.Metadata{
		Name:        "sys.go.heap.objects",
		Help:        "Number of live objects on the heap (live + unswept)",
		Measurement: "Objects",
		Unit:        metric.Unit_COUNT,
	}
	metaGoHeapLiveBytes = metric.Metadata{
		Name:        "sys.go.heap.livebytes",
		Help:        "Bytes of live heap objects marked by the previous GC",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaCgoAllocBytes = metric.Metadata{
		Name:        "sys.cgo.allocbytes",
		Help:        "Current bytes of memory allocated by cgo",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
		Visibility:  metric.Metadata_SUPPORT,
	}
	metaCgoTotalBytes = metric.Metadata{
		Name:        "sys.cgo.totalbytes",
		Help:        "Total bytes of memory allocated by cgo, but not released",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
		Visibility:  metric.Metadata_SUPPORT,
	}
	metaGCCount = metric.Metadata{
		Name:        "sys.gc.count",
		Help:        "Total number of GC runs",
		Measurement: "GC Runs",
		Unit:        metric.Unit_COUNT,
		Visibility:  metric.Metadata_SUPPORT,
	}
	metaGCPauseNS = metric.Metadata{
		Name:        "sys.gc.pause.ns",
		Help:        "Total GC pause",
		Measurement: "GC Pause",
		Unit:        metric.Unit_NANOSECONDS,
		Visibility:  metric.Metadata_SUPPORT,
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
		Visibility:  metric.Metadata_SUPPORT,
	}
	metaGCAssistNS = metric.Metadata{
		Name:        "sys.gc.assist.ns",
		Help:        "Estimated total CPU time user goroutines spent to assist the GC process",
		Measurement: "CPU Time",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaGCTotalNS = metric.Metadata{
		Name:        "sys.gc.total.ns",
		Help:        "Estimated total CPU time spent performing GC tasks",
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
		Visibility:  metric.Metadata_SUPPORT,
	}
	metaCPUUserPercent = metric.Metadata{
		Name:        "sys.cpu.user.percent",
		Help:        "Current user cpu percentage consumed by the CRDB process",
		Measurement: "CPU Time",
		Unit:        metric.Unit_PERCENT,
		Visibility:  metric.Metadata_ESSENTIAL,
		Category:    metric.Metadata_HARDWARE,
		HowToUse: `This metric gives the CPU usage percentage at the user
		level by the CockroachDB process only. This is similar to the Linux
		top command output. The metric value can be more than 1 (or 100%)
		on multi-core systems. It is best to combine user and system
		metrics.`,
	}
	metaCPUSysNS = metric.Metadata{
		Name:        "sys.cpu.sys.ns",
		Help:        "Total system cpu time consumed by the CRDB process",
		Measurement: "CPU Time",
		Unit:        metric.Unit_NANOSECONDS,
		Visibility:  metric.Metadata_SUPPORT,
	}
	metaCPUSysPercent = metric.Metadata{
		Name:        "sys.cpu.sys.percent",
		Help:        "Current system cpu percentage consumed by the CRDB process",
		Measurement: "CPU Time",
		Unit:        metric.Unit_PERCENT,
		Visibility:  metric.Metadata_ESSENTIAL,
		Category:    metric.Metadata_HARDWARE,
		HowToUse: `This metric gives the CPU usage percentage at the system
		(Linux kernel) level by the CockroachDB process only. This is
		similar to the Linux top command output. The metric value can be
		more than 1 (or 100%) on multi-core systems. It is best to combine
		user and system metrics.`,
	}
	metaCPUCombinedPercentNorm = metric.Metadata{
		Name:        "sys.cpu.combined.percent-normalized",
		Help:        "Current user+system cpu percentage consumed by the CRDB process, normalized 0-1 by number of cores",
		Measurement: "CPU Time",
		Unit:        metric.Unit_PERCENT,
		Visibility:  metric.Metadata_ESSENTIAL,
		Category:    metric.Metadata_HARDWARE,
		HowToUse: `This metric gives the CPU utilization percentage by the CockroachDB process. 
		If it is equal to 1 (or 100%), then the CPU is overloaded. The CockroachDB process should 
		not be running with over 80% utilization for extended periods of time (hours). This metric 
		is used in the DB Console CPU Percent graph.`,
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
		Visibility:  metric.Metadata_ESSENTIAL,
		Category:    metric.Metadata_HARDWARE,
		HowToUse: `This metric gives the CPU utilization percentage of the
		underlying server, virtual machine, or container hosting the
		CockroachDB process. It includes CPU usage from both CockroachDB
		and non-CockroachDB processes. It also accounts for time spent
		processing hardware (irq) and software (softirq) interrupts, as
		well as nice time, which represents low-priority user-mode
		activity.

    A value of 1 (or 100%) indicates that the CPU is overloaded. Avoid
    running the CockroachDB process in an environment where the CPU
    remains overloaded for extended periods (e.g. multiple hours). This
    metric appears in the DB Console on the Host CPU Percent graph.`,
	}

	metaRSSBytes = metric.Metadata{
		Name:        "sys.rss",
		Help:        "Current process RSS",
		Measurement: "RSS",
		Unit:        metric.Unit_BYTES,
		Visibility:  metric.Metadata_ESSENTIAL,
		Category:    metric.Metadata_HARDWARE,
		HowToUse: `This metric gives the amount of RAM used by the
		CockroachDB process. Persistently low values over an extended
		period of time suggest there is underutilized memory that can be
		put to work with adjusted settings for --cache or --max_sql_memory
		or both. Conversely, a high utilization, even if a temporary spike,
		indicates an increased risk of Out-of-memory (OOM) crash
		(particularly since the swap is generally disabled).`,
	}
	metaTotalMemBytes = metric.Metadata{
		Name:        "sys.totalmem",
		Help:        "Total memory (both free and used)",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
		Visibility:  metric.Metadata_SUPPORT,
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

		Visibility: metric.Metadata_ESSENTIAL,
		Category:   metric.Metadata_HARDWARE,
		HowToUse:   `This metric measures the length of time, in seconds, that the CockroachDB process has been running. Monitor this metric to detect events such as node restarts, which may require investigation or intervention.`,
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

		Visibility: metric.Metadata_ESSENTIAL,
		Category:   metric.Metadata_HARDWARE,
		HowToUse:   `This metric reports the effective storage device read IOPS rate. To confirm that storage is sufficiently provisioned, assess the I/O performance rates (IOPS and MBPS) in the context of the sys.host.disk.iopsinprogress metric.`,
	}
	metaHostDiskReadBytes = metric.Metadata{
		Name:        "sys.host.disk.read.bytes",
		Unit:        metric.Unit_BYTES,
		Measurement: "Bytes",
		Help:        "Bytes read from all disks since this process started (as reported by the OS)",

		Visibility: metric.Metadata_ESSENTIAL,
		Category:   metric.Metadata_HARDWARE,
		HowToUse:   `This metric reports the effective storage device read throughput (MB/s) rate. To confirm that storage is sufficiently provisioned, assess the I/O performance rates (IOPS and MBPS) in the context of the sys.host.disk.iopsinprogress metric.`,
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

		Visibility: metric.Metadata_ESSENTIAL,
		Category:   metric.Metadata_HARDWARE,
		HowToUse:   `This metric reports the effective storage device write IOPS rate. To confirm that storage is sufficiently provisioned, assess the I/O performance rates (IOPS and MBPS) in the context of the sys.host.disk.iopsinprogress metric.`,
	}
	metaHostDiskWriteBytes = metric.Metadata{
		Name:        "sys.host.disk.write.bytes",
		Unit:        metric.Unit_BYTES,
		Measurement: "Bytes",
		Help:        "Bytes written to all disks since this process started (as reported by the OS)",

		Visibility: metric.Metadata_ESSENTIAL,
		Category:   metric.Metadata_HARDWARE,
		HowToUse:   `This metric reports the effective storage device write throughput (MB/s) rate. To confirm that storage is sufficiently provisioned, assess the I/O performance rates (IOPS and MBPS) in the context of the sys.host.disk.iopsinprogress metric.`,
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

		Visibility: metric.Metadata_ESSENTIAL,
		Category:   metric.Metadata_HARDWARE,
		HowToUse:   `This metric gives the average queue length of the storage device. It characterizes the storage device's performance capability. All I/O performance metrics are Linux counters and correspond to the avgqu-sz in the Linux iostat command output. You need to view the device queue graph in the context of the actual read/write IOPS and MBPS metrics that show the actual device utilization. If the device is not keeping up, the queue will grow. Values over 10 are bad. Values around 5 mean the device is working hard trying to keep up. For internal (on chassis) NVMe devices, the queue values are typically 0. For network connected devices, such as AWS EBS volumes, the normal operating range of values is 1 to 2. Spikes in values are OK. They indicate an I/O spike where the device fell behind and then caught up. End users may experience inconsistent response times, but there should be no cluster stability issues. If the queue is greater than 5 for an extended period of time and IOPS or MBPS are low, then the storage is most likely not provisioned per Cockroach Labs guidance. In AWS EBS, it is commonly an EBS type, such as gp2, not suitable as database primary storage. If I/O is low and the queue is low, the most likely scenario is that the CPU is lacking and not driving I/O. One such case is a cluster with nodes with only 2 vcpus which is not supported sizing for production deployments. There are quite a few background processes in the database that take CPU away from the workload, so the workload is just not getting the CPU. Review storage and disk I/O.`,
	}
	metaHostNetRecvBytes = metric.Metadata{
		Name:        "sys.host.net.recv.bytes",
		Unit:        metric.Unit_BYTES,
		Measurement: "Bytes",
		Help:        "Bytes received on all network interfaces since this process started (as reported by the OS)",

		Visibility: metric.Metadata_ESSENTIAL,
		Category:   metric.Metadata_HARDWARE,
		HowToUse:   `This metric gives the node's ingress/egress network transfer rates for flat sections which may indicate insufficiently provisioned networking or high error rates. CockroachDB is using a reliable TCP/IP protocol, so errors result in delivery retries that create a "slow network" effect.`,
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

		Visibility: metric.Metadata_ESSENTIAL,
		Category:   metric.Metadata_HARDWARE,
		HowToUse:   `This metric gives the node's ingress/egress network transfer rates for flat sections which may indicate insufficiently provisioned networking or high error rates. CockroachDB is using a reliable TCP/IP protocol, so errors result in delivery retries that create a "slow network" effect.`,
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
	metaHostNetSendTCPRetransSegs = metric.Metadata{
		Name:        "sys.host.net.send.tcp.retrans_segs",
		Unit:        metric.Unit_COUNT,
		Measurement: "Segments",
		Category:    metric.Metadata_NETWORKING,
		Visibility:  metric.Metadata_ESSENTIAL,
		HowToUse: `
Phase changes, especially when occurring on groups of nodes, can indicate packet
loss in the network or a slow consumer of packets. On slow consumers, the
'sys.host.net.rcvd.drop' metric may be elevated; on overloaded senders, it
is worth checking the 'sys.host.net.send.drop' metric.
Additionally, the 'sys.host.net.send.tcp.*' may provide more insight into the
specific type of retransmission.
`,
		Help: `
The number of TCP segments retransmitted across all network interfaces.
This can indicate packet loss occurring in the network. However, it can
also be caused by recipient nodes not consuming packets in a timely manner,
or the local node overflowing its outgoing buffers, for example due to overload.

Retransmissions also occur in the absence of problems, as modern TCP stacks
err on the side of aggressively retransmitting segments.

The linux tool 'ss -i' can show the Linux kernel's smoothed view of round-trip
latency and variance on a per-connection basis.  Additionally, 'netstat -s'
shows all TCP counters maintained by the kernel.
`,
	}
	metaHostNetSendTCPFastRetrans = metric.Metadata{
		Name:        "sys.host.net.send.tcp.fast_retrans_segs",
		Unit:        metric.Unit_COUNT,
		Measurement: "Segments",
		Help: `Segments retransmitted due to the fast retransmission mechanism in TCP.
Fast retransmissions occur when the sender learns that intermediate segments have been lost.`,
		Category: metric.Metadata_NETWORKING,
	}
	metaHostNetSendTCPTimeouts = metric.Metadata{
		Name:        "sys.host.net.send.tcp_timeouts",
		Unit:        metric.Unit_COUNT,
		Measurement: "Timeouts",
		Help: `
Number of TCP retransmission timeouts. These typically imply that a packet has
not been acknowledged within at least 200ms.  Modern TCP stacks use
optimizations such as fast retransmissions and loss probes to avoid hitting
retransmission timeouts. Anecdotally, they still occasionally present themselves
even in supposedly healthy cloud environments.
`,
		Category: metric.Metadata_NETWORKING,
	}
	metaHostNetSendTCPSlowStartRetrans = metric.Metadata{
		Name:        "sys.host.net.send.tcp.slow_start_retrans",
		Unit:        metric.Unit_COUNT,
		Measurement: "Segments",
		Help: `
Number of TCP retransmissions in slow start. This can indicate that the network
is unable to support the initial fast ramp-up in window size, and can be a sign
of packet loss or congestion.
`,
		Category: metric.Metadata_NETWORKING,
	}
	metaHostNetSendTCPLossProbes = metric.Metadata{
		Name:        "sys.host.net.send.tcp.loss_probes",
		Unit:        metric.Unit_COUNT,
		Measurement: "Probes",
		Help: `
Number of TCP tail loss probes sent. Loss probes are an optimization to detect
loss of the last packet earlier than the retransmission timer, and can indicate
network issues. Tail loss probes are aggressive, so the base rate is often nonzero
even in healthy networks.`,
		Category: metric.Metadata_NETWORKING,
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

// Estimated total CPU time spent performing GC tasks.
const runtimeMetricGCTotal = "/cpu/classes/gc/total:cpu-seconds"

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

// Current soft memory limit (see debug.SetMemoryLimit).
const runtimeMetricGoLimit = "/gc/gomemlimit:bytes"

// Count of all completed GC cycles.
const runtimeMetricGCCount = "/gc/cycles/total:gc-cycles"

// Number of objects, live and unswept, occupying heap memory.
const runtimeMetricHeapObjects = "/gc/heap/objects:objects"

// Heap memory occupied by live objects that were marked by the
// previous GC.
const runtimeMetricHeapLiveBytes = "/gc/heap/live:bytes"

var runtimeMetrics = []string{
	runtimeMetricGCAssist,
	runtimeMetricGCTotal,
	runtimeMetricGoTotal,
	runtimeMetricHeapAlloc,
	runtimeMetricGoLimit,
	runtimeMetricHeapFragmentBytes,
	runtimeMetricHeapReservedBytes,
	runtimeMetricHeapReleasedBytes,
	runtimeMetricMemStackHeapBytes,
	runtimeMetricMemStackOSBytes,
	runtimeMetricCumulativeAlloc,
	runtimeMetricGCCount,
	runtimeMetricHeapObjects,
	runtimeMetricHeapLiveBytes,
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
		hostUtime       int64
		hostStime       int64
		hostIrqtime     int64
		hostSoftIrqtime int64
		hostNiceTime    int64

		cgoCall     int64
		gcCount     int64
		gcPauseTime uint64
		disk        DiskStats
		net         netCounters
		runnableSum float64
	}

	initialDiskCounters DiskStats
	initialNetCounters  netCounters

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
	GoLimitBytes             *metric.Gauge
	GoMemStackSysBytes       *metric.Gauge
	GoHeapFragmentBytes      *metric.Gauge
	GoHeapReservedBytes      *metric.Gauge
	GoHeapReleasedBytes      *metric.Gauge
	GoTotalAllocBytes        *metric.Counter
	GoHeapObjects            *metric.Gauge
	GoHeapLiveBytes          *metric.Gauge
	CgoAllocBytes            *metric.Gauge
	CgoTotalBytes            *metric.Gauge
	GcCount                  *metric.Counter
	GcPauseNS                *metric.Counter
	NonGcPauseNS             *metric.Gauge
	GcStopNS                 *metric.Gauge
	NonGcStopNS              *metric.Gauge
	GcPausePercent           *metric.GaugeFloat64
	GcAssistNS               *metric.Counter
	GcTotalNS                *metric.Counter
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
	HostDiskReadBytes              *metric.Counter
	HostDiskReadCount              *metric.Counter
	HostDiskReadTime               *metric.Counter
	HostDiskWriteBytes             *metric.Counter
	HostDiskWriteCount             *metric.Counter
	HostDiskWriteTime              *metric.Counter
	HostDiskIOTime                 *metric.Counter
	HostDiskWeightedIOTime         *metric.Counter
	IopsInProgress                 *metric.Gauge
	HostNetRecvBytes               *metric.Counter
	HostNetRecvPackets             *metric.Counter
	HostNetRecvErr                 *metric.Counter
	HostNetRecvDrop                *metric.Counter
	HostNetSendBytes               *metric.Counter
	HostNetSendPackets             *metric.Counter
	HostNetSendErr                 *metric.Counter
	HostNetSendDrop                *metric.Counter
	HostNetSendTCPRetransSegs      *metric.Counter
	HostNetSendTCPFastRetrans      *metric.Counter
	HostNetSendTCPTimeouts         *metric.Counter
	HostNetSendTCPSlowStartRetrans *metric.Counter
	HostNetSendTCPLossProbes       *metric.Counter
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
		log.Dev.Warningf(ctx, "could not parse build timestamp: %v", err)
	}

	// Build information. The labels on this metric enable Prometheus/Datadog
	// queries to filter or group by CockroachDB version — for example, to
	// identify which nodes are running a particular release series.
	year, release := build.BranchReleaseSeries()
	metaBuildTimestamp := metric.Metadata{
		Name:        "build.timestamp",
		Help:        "Build information",
		Measurement: "Build Time",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaBuildTimestamp.AddLabel("tag", info.Tag)
	metaBuildTimestamp.AddLabel("go_version", info.GoVersion)
	metaBuildTimestamp.AddLabel("major", strconv.Itoa(year))
	metaBuildTimestamp.AddLabel("minor", strconv.Itoa(release))

	buildTimestamp := metric.NewGauge(metaBuildTimestamp)
	buildTimestamp.Update(timestamp)

	diskCounters, err := getSummedDiskCounters(ctx)
	if err != nil {
		log.Ops.Errorf(ctx, "could not get initial disk IO counters: %v", err)
	}
	nc, err := getSummedNetStats(ctx)
	if err != nil {
		log.Ops.Errorf(ctx, "could not get initial network stat counters: %v", err)
	}

	rsr := &RuntimeStatSampler{
		clock:                    clock,
		startTimeNanos:           clock.Now().UnixNano(),
		initialNetCounters:       nc,
		initialDiskCounters:      diskCounters,
		goRuntimeSampler:         NewGoRuntimeSampler(runtimeMetrics),
		CgoCalls:                 metric.NewCounter(metaCgoCalls),
		Goroutines:               metric.NewGauge(metaGoroutines),
		RunnableGoroutinesPerCPU: metric.NewGaugeFloat64(metaRunnableGoroutinesPerCPU),
		GoAllocBytes:             metric.NewGauge(metaGoAllocBytes),
		GoTotalBytes:             metric.NewGauge(metaGoTotalBytes),
		GoLimitBytes:             metric.NewGauge(metaGoLimitBytes),
		GoMemStackSysBytes:       metric.NewGauge(metaGoMemStackSysBytes),
		GoHeapFragmentBytes:      metric.NewGauge(metaGoHeapFragmentBytes),
		GoHeapReservedBytes:      metric.NewGauge(metaGoHeapReservedBytes),
		GoHeapReleasedBytes:      metric.NewGauge(metaGoHeapReleasedBytes),
		GoTotalAllocBytes:        metric.NewCounter(metaGoTotalAllocBytes),
		GoHeapObjects:            metric.NewGauge(metaGoHeapObjects),
		GoHeapLiveBytes:          metric.NewGauge(metaGoHeapLiveBytes),
		CgoAllocBytes:            metric.NewGauge(metaCgoAllocBytes),
		CgoTotalBytes:            metric.NewGauge(metaCgoTotalBytes),
		GcCount:                  metric.NewCounter(metaGCCount),
		GcPauseNS:                metric.NewCounter(metaGCPauseNS),
		GcStopNS:                 metric.NewGauge(metaGCStopNS),
		GcPausePercent:           metric.NewGaugeFloat64(metaGCPausePercent),
		GcAssistNS:               metric.NewCounter(metaGCAssistNS),
		GcTotalNS:                metric.NewCounter(metaGCTotalNS),
		NonGcPauseNS:             metric.NewGauge(metaNonGCPauseNS),
		NonGcStopNS:              metric.NewGauge(metaNonGCStopNS),

		CPUUserNS:              metric.NewCounter(metaCPUUserNS),
		CPUUserPercent:         metric.NewGaugeFloat64(metaCPUUserPercent),
		CPUSysNS:               metric.NewCounter(metaCPUSysNS),
		CPUSysPercent:          metric.NewGaugeFloat64(metaCPUSysPercent),
		CPUCombinedPercentNorm: metric.NewGaugeFloat64(metaCPUCombinedPercentNorm),
		CPUNowNS:               metric.NewCounter(metaCPUNowNS),

		HostCPUCombinedPercentNorm: metric.NewGaugeFloat64(metaHostCPUCombinedPercentNorm),

		RSSBytes:                       metric.NewGauge(metaRSSBytes),
		TotalMemBytes:                  metric.NewGauge(metaTotalMemBytes),
		HostDiskReadBytes:              metric.NewCounter(metaHostDiskReadBytes),
		HostDiskReadCount:              metric.NewCounter(metaHostDiskReadCount),
		HostDiskReadTime:               metric.NewCounter(metaHostDiskReadTime),
		HostDiskWriteBytes:             metric.NewCounter(metaHostDiskWriteBytes),
		HostDiskWriteCount:             metric.NewCounter(metaHostDiskWriteCount),
		HostDiskWriteTime:              metric.NewCounter(metaHostDiskWriteTime),
		HostDiskIOTime:                 metric.NewCounter(metaHostDiskIOTime),
		HostDiskWeightedIOTime:         metric.NewCounter(metaHostDiskWeightedIOTime),
		IopsInProgress:                 metric.NewGauge(metaHostIopsInProgress),
		HostNetRecvBytes:               metric.NewCounter(metaHostNetRecvBytes),
		HostNetRecvPackets:             metric.NewCounter(metaHostNetRecvPackets),
		HostNetRecvErr:                 metric.NewCounter(metaHostNetRecvErr),
		HostNetRecvDrop:                metric.NewCounter(metaHostNetRecvDrop),
		HostNetSendBytes:               metric.NewCounter(metaHostNetSendBytes),
		HostNetSendPackets:             metric.NewCounter(metaHostNetSendPackets),
		HostNetSendErr:                 metric.NewCounter(metaHostNetSendErr),
		HostNetSendDrop:                metric.NewCounter(metaHostNetSendDrop),
		HostNetSendTCPRetransSegs:      metric.NewCounter(metaHostNetSendTCPRetransSegs),
		HostNetSendTCPFastRetrans:      metric.NewCounter(metaHostNetSendTCPFastRetrans),
		HostNetSendTCPTimeouts:         metric.NewCounter(metaHostNetSendTCPTimeouts),
		HostNetSendTCPSlowStartRetrans: metric.NewCounter(metaHostNetSendTCPSlowStartRetrans),
		HostNetSendTCPLossProbes:       metric.NewCounter(metaHostNetSendTCPLossProbes),
		FDOpen:                         metric.NewGauge(metaFDOpen),
		FDSoftLimit:                    metric.NewGauge(metaFDSoftLimit),
		Uptime:                         metric.NewCounter(metaUptime),
		BuildTimestamp:                 buildTimestamp,
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
			log.Dev.Warningf(ctx, "problem fetching CGO memory stats: %s; CGO stats will be empty.", err)
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
var vcpuCgroupEvery = log.Every(time.Hour)

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
	cpuCapacity := GetCPUCapacity()
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
		subtractDiskCounters(ctx, &diskCounters, &rsr.initialDiskCounters)

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

	var deltaNet netCounters
	nc, err := getSummedNetStats(ctx)
	if err != nil {
		if netstatEvery.ShouldLog() {
			log.Ops.Warningf(ctx, "problem fetching net stats: %s; net stats will be empty.", err)
		}
	} else {
		deltaNet = nc // delta since *last* scrape
		subtractNetworkCounters(ctx, &deltaNet, &rsr.last.net)
		rsr.last.net = nc

		// `nc` will now be the delta since *first* scrape.
		subtractNetworkCounters(ctx, &nc, &rsr.initialNetCounters)
		// TODO(tbg): this is awkward: we're computing the delta above,
		// why don't we increment the counters?
		rsr.HostNetRecvBytes.Update(int64(nc.IOCounters.BytesRecv))
		rsr.HostNetRecvPackets.Update(int64(nc.IOCounters.PacketsRecv))
		rsr.HostNetRecvErr.Update(int64(nc.IOCounters.Errin))
		rsr.HostNetRecvDrop.Update(int64(nc.IOCounters.Dropin))
		rsr.HostNetSendBytes.Update(int64(nc.IOCounters.BytesSent))
		rsr.HostNetSendPackets.Update(int64(nc.IOCounters.PacketsSent))
		rsr.HostNetSendErr.Update(int64(nc.IOCounters.Errout))
		rsr.HostNetSendDrop.Update(int64(nc.IOCounters.Dropout))
		rsr.HostNetSendTCPRetransSegs.Update(nc.TCPRetransSegs)
		rsr.HostNetSendTCPFastRetrans.Update(nc.TCPFastRetrans)
		rsr.HostNetSendTCPTimeouts.Update(nc.TCPTimeouts)
		rsr.HostNetSendTCPSlowStartRetrans.Update(nc.TCPSlowStartRetrans)
		rsr.HostNetSendTCPLossProbes.Update(nc.TCPLossProbes)
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
	hostIrqtime := int64(cpuUsage.Irq * 1.e9)
	hostSoftIrqtime := int64(cpuUsage.Softirq * 1.e9)
	hostNiceTime := int64(cpuUsage.Nice * 1.e9)

	var procUrate, procSrate, hostUrate, hostSrate, hostIrqrate, hostSoftIrqrate, hostNiceRate float64
	if rsr.last.now != 0 && dur > 0 { // We cannot compute these rates on the first iteration or if no time elapsed.
		procUrate = float64(procUtime-rsr.last.procUtime) / dur
		procSrate = float64(procStime-rsr.last.procStime) / dur
		hostUrate = float64(hostUtime-rsr.last.hostUtime) / dur
		hostSrate = float64(hostStime-rsr.last.hostStime) / dur
		hostIrqrate = float64(hostIrqtime-rsr.last.hostIrqtime) / dur
		hostSoftIrqrate = float64(hostSoftIrqtime-rsr.last.hostSoftIrqtime) / dur
		hostNiceRate = float64(hostNiceTime-rsr.last.hostNiceTime) / dur
	}

	combinedNormalizedProcPerc := (procSrate + procUrate) / cpuCapacity
	combinedNormalizedHostPerc := (hostSrate + hostUrate + hostIrqrate + hostSoftIrqrate + hostNiceRate) / float64(numHostCPUs)

	gcPauseTotal := float64HistogramSum(rsr.goRuntimeSampler.float64Histogram(runtimeMetricGCPauseTotal))
	nonGcPauseTotal := float64HistogramSum(rsr.goRuntimeSampler.float64Histogram(runtimeMetricNonGCPauseTotal))
	gcStopTotal := float64HistogramSum(rsr.goRuntimeSampler.float64Histogram(runtimeMetricGCStopTotal))
	nonGcStopTotal := float64HistogramSum(rsr.goRuntimeSampler.float64Histogram(runtimeMetricNonGCStopTotal))
	gcPauseTotalNs := uint64(gcPauseTotal * 1.e9)
	nonGcPauseTotalNs := int64(nonGcPauseTotal * 1.e9)
	gcStopTotalNs := int64(gcStopTotal * 1.e9)
	nonGcStopTotalNs := int64(nonGcStopTotal * 1.e9)
	gcCount := rsr.goRuntimeSampler.uint64(runtimeMetricGCCount)
	var gcPauseRatio float64
	if dur > 0 {
		gcPauseRatio = float64(gcPauseTotalNs-rsr.last.gcPauseTime) / dur
	}
	runnableSum := goschedstats.CumulativeNormalizedRunnableGoroutines()
	gcAssistSeconds := rsr.goRuntimeSampler.float64(runtimeMetricGCAssist)
	gcAssistNS := int64(gcAssistSeconds * 1e9)
	// The number of runnable goroutines per CPU is a count, but it can vary
	// quickly. We don't just want to get a current snapshot of it, we want the
	// average value since the last sampling.
	var runnableAvg float64
	if dur > 0 {
		runnableAvg = (runnableSum - rsr.last.runnableSum) * 1e9 / dur
	}
	rsr.last.now = now
	rsr.last.procUtime = procUtime
	rsr.last.procStime = procStime
	rsr.last.hostUtime = hostUtime
	rsr.last.hostStime = hostStime
	rsr.last.hostIrqtime = hostIrqtime
	rsr.last.hostSoftIrqtime = hostSoftIrqtime
	rsr.last.hostNiceTime = hostNiceTime
	rsr.last.gcPauseTime = gcPauseTotalNs
	rsr.last.runnableSum = runnableSum

	// Log summary of statistics to console.
	osStackBytes := rsr.goRuntimeSampler.uint64(runtimeMetricMemStackOSBytes)
	var cgoRate float64
	if dur > 0 {
		cgoRate = float64((numCgoCall-rsr.last.cgoCall)*int64(time.Second)) / dur
	}
	goAlloc := rsr.goRuntimeSampler.uint64(runtimeMetricHeapAlloc)
	goTotal := rsr.goRuntimeSampler.uint64(runtimeMetricGoTotal) -
		rsr.goRuntimeSampler.uint64(runtimeMetricHeapReleasedBytes)
	goLimit := rsr.goRuntimeSampler.uint64(runtimeMetricGoLimit)
	if goLimit == math.MaxInt64 {
		goLimit = 0
	}
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
		GoLimitBytes:      goLimit,
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
		NetHostRecvBytes:  deltaNet.IOCounters.BytesRecv,
		NetHostSendBytes:  deltaNet.IOCounters.BytesSent,
	}

	logStats(ctx, stats)

	rsr.last.cgoCall = numCgoCall
	rsr.last.gcCount = int64(gcCount)

	rsr.GoAllocBytes.Update(int64(goAlloc))
	rsr.GoTotalBytes.Update(int64(goTotal))
	rsr.GoLimitBytes.Update(int64(goLimit))
	rsr.GoMemStackSysBytes.Update(int64(osStackBytes))
	rsr.GoHeapFragmentBytes.Update(int64(heapFragmentBytes))
	rsr.GoHeapReservedBytes.Update(int64(heapReservedBytes))
	rsr.GoHeapReleasedBytes.Update(int64(heapReleasedBytes))
	rsr.GoTotalAllocBytes.Update(int64(rsr.goRuntimeSampler.uint64(runtimeMetricCumulativeAlloc)))
	rsr.GoHeapObjects.Update(int64(rsr.goRuntimeSampler.uint64(runtimeMetricHeapObjects)))
	rsr.GoHeapLiveBytes.Update(int64(rsr.goRuntimeSampler.uint64(runtimeMetricHeapLiveBytes)))
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
	rsr.GcTotalNS.Update(int64(rsr.goRuntimeSampler.float64(runtimeMetricGCTotal) * 1e9))
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

type netCounters struct {
	IOCounters          net.IOCountersStat
	TCPRetransSegs      int64
	TCPFastRetrans      int64
	TCPTimeouts         int64
	TCPSlowStartRetrans int64
	TCPLossProbes       int64
}

var mockableMaybeReadProcStatFile = maybeReadProcStatFile

func getSummedNetStats(ctx context.Context) (netCounters, error) {
	c, err := net.IOCountersWithContext(ctx, true /* per NIC */)
	if err != nil {
		log.Dev.VWarningf(ctx, 1, "error reading network IO counters: %v", err)
		c = nil
		// Continue. Empty slice c results in zero counters.
	}

	tcpRetransSegs := int64(-1)
	mTCP := func() map[string]int64 {
		pc, err := net.ProtoCountersWithContext(ctx, []string{"tcp"})
		if err != nil {
			log.Dev.VWarningf(ctx, 1, "error reading tcp counters: %v", err)
			return nil
		}
		return pc[0].Stats
	}()

	if n, ok := mTCP["RetransSegs"]; ok {
		tcpRetransSegs = n
	}

	tcpFastRetrans := int64(-1)
	tcpTimeouts := int64(-1)
	tcpSlowStartRetrans := int64(-1)
	tcpLossProbes := int64(-1)
	const netstatFile = "/proc/net/netstat"
	mTCPExt, err := mockableMaybeReadProcStatFile(ctx, "TcpExt", netstatFile)
	if err != nil {
		log.Dev.VWarningf(ctx, 1, "error reading %s: %v", netstatFile, err)
		mTCPExt = nil
		// Continue.
	}
	if n, ok := mTCPExt["TCPFastRetrans"]; ok {
		tcpFastRetrans = n
	}
	if n, ok := mTCPExt["TCPTimeouts"]; ok {
		tcpTimeouts = n
	}
	if n, ok := mTCPExt["TCPSlowStartRetrans"]; ok {
		tcpSlowStartRetrans = n
	}
	if n, ok := mTCPExt["TCPLossProbes"]; ok {
		tcpLossProbes = n
	}

	if log.V(3) {
		log.Dev.Infof(ctx, "tcp stats: Tcp: %+v TcpExt: %+v", mTCP, mTCPExt)
	}

	return netCounters{
		IOCounters:          sumNetworkCounters(c),
		TCPRetransSegs:      tcpRetransSegs,
		TCPFastRetrans:      tcpFastRetrans,
		TCPTimeouts:         tcpTimeouts,
		TCPSlowStartRetrans: tcpSlowStartRetrans,
		TCPLossProbes:       tcpLossProbes,
	}, nil
}

// maybeReadProcStatFile reads the provided file, which is assumed to be in
// linux procfs format as seen in /proc/net/snmp and /proc/net/netstat.
//
// When not on linux, returns an empty map (and no error).
func maybeReadProcStatFile(
	ctx context.Context, protocol string, path string,
) (map[string]int64, error) {
	if runtime.GOOS != "linux" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parseProcStatFile(ctx, protocol, data)
}

func parseProcStatFile(
	ctx context.Context, protocol string, data []byte,
) (map[string]int64, error) {
	var headers, values []string
	prefix := protocol + ":"
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, prefix) {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			if headers == nil {
				headers = fields[1:]
			} else {
				values = fields[1:]
				break
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, errors.Wrap(err, "failed to scan proc stat file")
	}

	if len(headers) == 0 {
		// NB: this is not an error. The requested protocol might not be present.
		return nil, nil
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("no values found for protocol %s", protocol)
	}
	if len(headers) != len(values) {
		return nil, fmt.Errorf("mismatch between headers and values for protocol %s: %d headers, %d values", protocol, len(headers), len(values))
	}

	stats := make(map[string]int64, len(headers))
	for i, h := range headers {
		v, err := strconv.ParseInt(values[i], 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "could not parse value %s:%q", headers[i], values[i])
		}
		stats[h] = v
	}
	return stats, nil
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

// subtractDiskStat subtracts base from stat. If stat < base (indicating a
// counter wrap, e.g. on s390x where /proc/diskstats counters are 32-bit
// unsigned), base is reset to stat and stat is zeroed.
func subtractDiskStat(ctx context.Context, stat, base *int64, name string) {
	if *stat < *base {
		log.Ops.Infof(ctx,
			"runtime: disk stats counter wrap detected for %s, resetting baseline", name)
		*base = *stat
		*stat = 0
	} else {
		*stat -= *base
	}
}

// subtractDiskStatDuration is like subtractDiskStat but for time.Duration
// fields.
func subtractDiskStatDuration(ctx context.Context, stat, base *time.Duration, name string) {
	if *stat < *base {
		log.Ops.Infof(ctx,
			"runtime: disk stats counter wrap detected for %s, resetting baseline", name)
		*base = *stat
		*stat = 0
	} else {
		*stat -= *base
	}
}

// subtractDiskCounters subtracts the counters in `baseline` from the
// counters in `stats`, saving the results in `stats`. Each counter is
// handled independently: if a counter in `stats` is lower than the
// corresponding counter in `baseline` (indicating a wrap on s390x
// where /proc/diskstats counters are 32-bit unsigned), that counter's
// baseline is reset to the current value and the result for that
// counter is zeroed. Counters that haven't wrapped subtract normally.
func subtractDiskCounters(ctx context.Context, stats *DiskStats, baseline *DiskStats) {
	subtractDiskStat(ctx, &stats.ReadBytes, &baseline.ReadBytes, "host-disk-read-bytes")
	subtractDiskStat(ctx, &stats.readCount, &baseline.readCount, "host-disk-read-count")
	subtractDiskStatDuration(ctx, &stats.readTime, &baseline.readTime, "host-disk-read-time")
	subtractDiskStat(ctx, &stats.WriteBytes, &baseline.WriteBytes, "host-disk-write-bytes")
	subtractDiskStat(ctx, &stats.writeCount, &baseline.writeCount, "host-disk-write-count")
	subtractDiskStatDuration(ctx, &stats.writeTime, &baseline.writeTime, "host-disk-write-time")
	subtractDiskStatDuration(ctx, &stats.ioTime, &baseline.ioTime, "host-disk-io-time")
	subtractDiskStatDuration(ctx, &stats.weightedIOTime, &baseline.weightedIOTime, "host-disk-weighted-io-time")
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

// subtractNetworkCounters subtracts the counters in `baseline`
// from the counters in `stats`, saving the results in `stats`. If
// any counter in `stats` is lower than the corresponding counter
// in `baseline` (indicating a reset), the value for all metrics in
// `baseline` is updated to the current value in `stats` to establish
// a new baseline.
func subtractNetworkCounters(ctx context.Context, stats *netCounters, baseline *netCounters) {
	if stats.IOCounters.BytesRecv < baseline.IOCounters.BytesRecv ||
		stats.IOCounters.PacketsRecv < baseline.IOCounters.PacketsRecv ||
		stats.IOCounters.Errin < baseline.IOCounters.Errin ||
		stats.IOCounters.Dropin < baseline.IOCounters.Dropin ||
		stats.IOCounters.BytesSent < baseline.IOCounters.BytesSent ||
		stats.IOCounters.PacketsSent < baseline.IOCounters.PacketsSent ||
		stats.IOCounters.Errout < baseline.IOCounters.Errout ||
		stats.IOCounters.Dropout < baseline.IOCounters.Dropout ||
		stats.TCPRetransSegs < baseline.TCPRetransSegs ||
		stats.TCPFastRetrans < baseline.TCPFastRetrans ||
		stats.TCPTimeouts < baseline.TCPTimeouts ||
		stats.TCPSlowStartRetrans < baseline.TCPSlowStartRetrans ||
		stats.TCPLossProbes < baseline.TCPLossProbes {
		*baseline = *stats
		*stats = netCounters{}
		log.Ops.Info(ctx, "runtime: new baseline in network stats from host. network metric counters have been reset.")
		return
	}

	// Perform normal subtraction
	stats.IOCounters.BytesRecv -= baseline.IOCounters.BytesRecv
	stats.IOCounters.PacketsRecv -= baseline.IOCounters.PacketsRecv
	stats.IOCounters.Errin -= baseline.IOCounters.Errin
	stats.IOCounters.Dropin -= baseline.IOCounters.Dropin
	stats.IOCounters.BytesSent -= baseline.IOCounters.BytesSent
	stats.IOCounters.PacketsSent -= baseline.IOCounters.PacketsSent
	stats.IOCounters.Errout -= baseline.IOCounters.Errout
	stats.IOCounters.Dropout -= baseline.IOCounters.Dropout
	stats.TCPRetransSegs -= baseline.TCPRetransSegs
	stats.TCPFastRetrans -= baseline.TCPFastRetrans
	stats.TCPTimeouts -= baseline.TCPTimeouts
	stats.TCPSlowStartRetrans -= baseline.TCPSlowStartRetrans
	stats.TCPLossProbes -= baseline.TCPLossProbes
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

// GetCPUCapacity returns the number of logical CPU processors available for
// use by the process. The capacity accounts for cgroup constraints, GOMAXPROCS
// and the number of host –processors.
func GetCPUCapacity() float64 {
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

// GetVCPUs returns the number of vCPUs allocated to the process as
// reported by cgroups. This falls back to the number of CPUs reported
// by the OS in case of error. Setting of GOMAXPROCS does not affect this
// value (as opposed to GetCPUCapacity above which is used for internal
// rebalancing decisions).
func GetVCPUs(ctx context.Context) float64 {
	cgroupCPU, err := cgroups.GetCgroupCPU()
	if err != nil {
		if vcpuCgroupEvery.ShouldLog() {
			log.Ops.Warningf(ctx, "unable to read cgroup CPU settings: %v; falling back to OS-reported CPU count", err)
		}
		// No cgroup limits configured. Fall back to the number of CPUs reported
		// by the operating system.
		return float64(system.NumCPU())
	}
	cpuShare := cgroupCPU.CPUShares()
	if cpuShare == 0 {
		// No CPU quota set in cgroup. Fall back to OS-reported CPU count.
		return float64(system.NumCPU())
	}
	return cpuShare
}
