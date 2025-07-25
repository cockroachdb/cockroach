# Understanding CPU Overload Handling in CockroachDB

CockroachDB employs a sophisticated system to manage CPU overload, ensuring that high-priority operations remain responsive while allowing background tasks to utilize spare capacity. This involves specific logging to provide visibility and a two-pronged admission control strategy to manage different types of work.

## CPU Overload Logging

The primary logging for CPU overload provides insights into how the admission control system is reacting to CPU load. The messages are typically under a verbose logging level (`V(1)`).

### Grant Coordinator Logging

The `GrantCoordinator` logs changes in the CPU load sampling period. This is significant because the admission controller's behavior, especially the dynamic adjustment of resources, depends on frequent CPU statistics. A change in the sampling period can indicate a change in the underlying system load.

This logging can be found in `pkg/util/admission/grant_coordinator.go`:
```go
if log.V(1) {
    if coord.lastCPULoadSamplePeriod != 0 && coord.lastCPULoadSamplePeriod != samplePeriod &&
        KVAdmissionControlEnabled.Get(&coord.settings.SV) {
        log.Infof(ctx, "CPULoad switching to period %s", samplePeriod.String())
    }
}
```

### Elastic CPU Granter Logging

For "elastic" (low-priority) work, such as backups or changefeeds, there is more specific logging that reveals the allocated CPU resources.

This logging, from `pkg/util/admission/elastic_cpu_granter.go`, shows the target CPU utilization for these background tasks:
```go
if log.V(1) {
    log.Infof(e.ctx, "elastic cpu granter refill rate = %0.4f cpu seconds per second (utilization across %d procs = %0.2f%%)",
        time.Duration(rate).Seconds(), runtime.GOMAXPROCS(0), utilizationLimit*100)
}
```
This log message is very informative, as it directly states the rate (as a percentage of total CPU capacity) at which background work is being permitted. A lower rate indicates that the system is throttling these tasks to reserve CPU for higher-priority work.

## Decision-Making for CPU-Based Admission Control

The strategy for limiting admission based on CPU load differs for regular, latency-sensitive work and low-priority, elastic work.

### 1. Regular (KV) Work

For normal, high-priority KV operations, admission control is managed by a slot-based system. The key component is the `kvSlotAdjuster`.

-   **Location:** `pkg/util/admission/kv_slot_adjuster.go`
-   **Signal:** The primary signal is the number of runnable goroutines per CPU core. The CPU is considered overloaded if this value exceeds the cluster setting `admission.kv_slot_adjuster.overload_threshold` (default: 32).
-   **Action:**
    -   When overloaded, the `kvSlotAdjuster` reduces the number of concurrent "slots" available for new KV work.
    -   When the CPU is underloaded, it increases the number of available slots.
-   **Instantaneous Feedback:** A critical feature of this system is the `isOverloaded()` method, which provides an *instantaneous* signal of overload if all available KV slots are currently in use. This allows the system to react immediately to bursts of traffic without waiting for the next periodic CPU load sample, preventing the goroutine scheduler from becoming overwhelmed.

### 2. Elastic (Low-Priority) Work

For background tasks that are not latency-sensitive, a different, more dynamic mechanism is used to ensure they don't interfere with foreground traffic.

-   **Location:** `pkg/util/admission/elastic_cpu_granter.go` and `pkg/util/admission/scheduler_latency_listener.go`
-   **Signal:** Instead of CPU utilization directly, this system monitors **scheduler latency**. If the time a goroutine has to wait in a runnable state before being scheduled on a CPU becomes too high, it's a clear indicator of CPU saturation.
-   **Action:** The system adjusts a "target CPU utilization" limit specifically for elastic work.
    -   If scheduler latency increases, the CPU utilization limit for elastic work is decreased.
    -   If scheduler latency is low, the limit is increased, allowing background tasks to consume more of the available, unused CPU resources.
-   **Control:** This feedback loop is governed by a set of `admission.elastic_cpu.*` cluster settings, which control the minimum and maximum utilization percentages and the rate at which they are adjusted. This allows elastic work to "fill in the gaps" without disrupting user-facing operations.

## Interaction Between Throttling Mechanisms

The two mechanisms described above—slot adjustment for regular work and latency-based throttling for elastic work—are triggered by different signals and largely operate independently. However, there is a crucial **one-way feedback loop** from the regular work system to other parts of the query execution engine.

-   **KV Slots Act as an "Emergency Brake":** If the KV layer becomes completely saturated (i.e., all KV slots are in use), the `kvSlotAdjuster`'s `isOverloaded()` method provides an instantaneous signal. The `GrantCoordinator` uses this signal to **immediately stop admitting new SQL work** (`SQLKVResponseWork` and `SQLSQLResponseWork`). This is a critical safety valve that prevents the system from accepting new user queries when the underlying storage engine is already at capacity.
-   **No Crossover in Reverse:** The scheduler latency mechanism that throttles elastic work does **not** influence the number of slots available for regular KV work. Its only purpose is to gate low-priority tasks.
-   **Single Threshold for `kvSlotAdjuster`:** The `kvSlotAdjuster` uses a single primary threshold to determine if the CPU is overloaded. It does not have separate, lower thresholds for different work types; its focus is solely on managing the concurrency of the high-priority KV layer.

## How Signals Are Computed

The admission control system relies on two key signals from the Go runtime to make decisions.

-   **Runnable Goroutines:** This signal is collected by a high-frequency polling mechanism (typically every millisecond) in `pkg/util/grunning/scheduler.go`. This poller checks the number of goroutines that are in a `runnable` state, waiting for a CPU core to become available. This provides a near real-time measure of CPU scheduling pressure and is the primary input for the `kvSlotAdjuster`.
-   **Scheduler Latency:** This signal is collected via an observer pattern in `pkg/util/goschedstats`. The observer registers with the Go runtime and, every few seconds, receives a histogram of scheduler latencies—how long goroutines had to wait before being executed. The p99 value from this histogram is used by the `scheduler_latency_listener` to determine if long-tail latencies are increasing, which indicates sustained CPU saturation. This signal is the primary input for throttling elastic work. 