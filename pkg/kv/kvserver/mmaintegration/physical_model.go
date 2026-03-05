// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"context"
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
)

// physicalStore holds the physical load, capacity, and amplification
// factors for a store across all load dimensions. Each vector is indexed by
// mmaprototype.LoadDimension.
type physicalStore struct {
	// load is the per-store physical load in each dimension's native unit
	// (ns/s for CPURate, bytes/s for WriteBandwidth, bytes for ByteSize).
	// Store load does not have to originate from MMA-tracked range work. For
	// example, the CPU dimension includes background load (SQL gateway, GC,
	// etc.), so the sum of per-range CPU loads will generally be less than the
	// store load.
	load mmaprototype.LoadVector
	// capacity is the per-store physical capacity in each dimension's native
	// unit.
	capacity mmaprototype.LoadVector
	// amplificationFactors converts logical per-range loads to physical units.
	// Both CPU and disk amplification factors are capped (see
	// maxCPUAmplification, maxDiskSpaceAmplification). When the cap
	// binds, per-range physical loads undercount reality — ranges look smaller
	// than they are. This affects range selection (which range to move) but not
	// store selection (which store to shed from), because store-level load comes
	// from direct physical measurements, not from summing amplified range loads.
	amplificationFactors mmaprototype.AmpVector
}

// physicalDimension holds the outputs of the physical model for a single
// load dimension.
type physicalDimension struct {
	load                mmaprototype.LoadValue
	capacity            mmaprototype.LoadValue
	amplificationFactor mmaprototype.Amp
}

// computePhysicalCPU computes per-store physical CPU load, capacity, and
// amplification factor from the store descriptor.
//
// The computation has two steps:
//
//  1. Estimate the amplification factor and immovable CPU. When SQL CPU
//     metrics are available, node CPU is decomposed into moveable work
//     (KV + distSQL, scaled by k1) and immovable work (gateway SQL, scaled
//     by k2). When SQL metrics are unavailable, falls back to a single-ratio
//     model. See "SQL-aware decomposition" below.
//
//  2. Compute per-store load and capacity.
//     Load uses this store's direct replica CPU (CPUPerSecond) amplified by
//     the factor, plus an even share of immovable CPU. This lets stores with
//     more KV work report higher load while still accounting for node-level
//     overhead. Falls back to storesCPU/numStores when CPUPerSecond is
//     unavailable. Capacity is nodeCap/numStores — a real-world quantity (the
//     store's share of the node's CPU cores) that remains directly
//     interpretable.
//
// Design decisions for the CPU physical model. For simplicity, the examples
// below assume single-store nodes unless stated otherwise.
//
// # Immovable CPU in load, not capacity
//
// Immovable CPU (max(0, nodeUsage - storesCPU*ampFactor)) is added to each
// store's load rather than subtracted from capacity. Capacity stays a
// real-world number (nodeCap/numStores) and nodes running hot from any cause
// become shed candidates.
//
// The alternative — subtracting immovable CPU from capacity — hides pressure
// in the denominator:
//
//	Two nodes, 10-core each:
//	  X: 80% total CPU (60% immovable + 20% KV).  immov = 6, KV = 2.
//	  Y: 60% total CPU (all KV).                  immov = 0, KV = 6.
//
//	  Subtract from capacity:  X = 2/(10-6)  = 50%,  Y = 6/10 = 60%.
//	  Add to load (chosen):    X = 8/10      = 80%,  Y = 6/10 = 60%.
//
// With subtraction MMA sees Y as more loaded and sheds from Y to X. But X is
// the node running at 80% real CPU — sending it more work makes things worse.
// With addition, X is correctly the hotter node.
//
// # Per-store load via CPUPerSecond
//
// Each store's load uses its own CPUPerSecond (amplified) instead of an even
// split of node usage. On a 4-store node (storesCPU=8, nodeUsage=12,
// nodeCap=16) with S1 at 5 cores and S2 at 1 core: S1 shows 188% util vs S2
// at 38%. Even splitting hides this — both would show 75%. Falls back to even
// split when CPUPerSecond is unavailable.
//
// # Capacity split evenly, not weighted
//
// Each store gets nodeCap/numStores. Weighting by store fraction causes
// storeCPU to cancel in the utilization ratio (load/capacity =
// storeCPU*amp / (nodeCap*storeCPU/storesCPU) = amp*storesCPU/nodeCap),
// making all stores on a node show identical utilization regardless of
// imbalance. Even split preserves differentiation; the node-level overload
// check (canShedAndAddLoad) prevents over-accepting.
//
// Trade-off: a node with heavy immovable CPU and little KV work still appears
// loaded and MMA may try to shed from it. We accept this because:
//
//  1. Operators see balanced total CPU across nodes — they don't need to
//     distinguish KV from non-KV usage to understand the cluster state.
//  2. If the node has no ranges at all, the shed attempt is a harmless no-op.
//  3. The inflated store load raises the topK threshold (minLoadFraction *
//     adjustedLoad), filtering out tiny ranges that wouldn't meaningfully
//     reduce load — naturally limiting churn on immovable-heavy nodes.
//  4. The alternative (subtracting immovable CPU from capacity) makes the numbers
//     harder to reason about and, as shown above, can cause MMA to send work
//     toward already-hot nodes.
//
// # SQL-aware decomposition
//
// In general, node CPU splits into moveable work (anything that scales with
// KV ranges) and immovable work (everything else):
//
//	nodeUsage = moveable + immovable
//	moveable  = k1 * (storesCPU + sqlDistCPU)
//	immovable = k2 * sqlGatewayCPU
//
// k1 and k2 are overhead multipliers that scale the tracked CPU sources to
// account for untracked work (backups, elastic work, GC, CDC, OS overhead,
// etc.) that could belong to either bucket. With one equation
// (nodeUsage = k1*trackedMoveable + k2*sqlGatewayCPU) and two unknowns, we
// cannot solve for k1 and k2 independently — we'd need a second measurement
// that separates moveable from immovable overhead, which we don't have.
// Instead, we assume k1 = k2 = k and solve:
//
//	k = nodeUsage / totalTracked
//
// where totalTracked = storesCPU + sqlDistCPU + sqlGatewayCPU. This is more
// accurate than the fallback's nodeUsage/storesCPU, which lumps all non-KV
// CPU into the overhead.
//
// Distributed SQL scales proportionally with KV, so it is folded into the
// amplification factor rather than treated as immovable:

// trackedMoveable = storesCPU + sqlDistCPU
//
//	= storesCPU * (1 + sqlDistCPU/storesCPU)
//
// physical by trackedMoveable = trackedMoveable * k
//
//	= storesCPU * (1 + sqlDistCPU/storesCPU) * k
//
// By definition, physical = storesCPU * ampFactor, so ampsFactor = (1 +
// sqlDistCPU/storesCPU) * k.
//
//	ampFactor = (1 + sqlDistCPU/storesCPU) * k
//	immovable = max(0, nodeUsage - trackedMoveable * k)
//
// where trackedMoveable = storesCPU + sqlDistCPU. The cap
// (maxCPUAmplification) is applied to k — the per-source overhead
// multiplier — so that the model doesn't attribute more than 3x overhead to
// any tracked source. ampFactor = (1 + distRatio) * k may exceed the cap
// when distSQL is significant; that's expected since it reflects real
// measured distSQL work, not estimation error. When k is clamped, the
// excess overhead spills into the immovable term. When k is unclamped,
// immovable = sqlGatewayCPU * k.
//
// Fallback: when SQL metrics are zero, the model uses a single ratio
// (ampFactor = clamp(nodeUsage/storesCPU, 1, cap)) and attributes the
// residual as immovable, matching the pre-SQL behavior.
func computePhysicalCPU(desc roachpb.StoreDescriptor) (res physicalDimension) {
	assertCPUInputs(desc)
	defer func() { res.capacity = max(minCapacity, res.capacity) }()
	nc := desc.NodeCapacity
	numStores := max(1, float64(nc.NumStores))
	nodeUsage := max(0, float64(nc.NodeCPURateUsage))
	nodeCap := max(0, float64(nc.NodeCPURateCapacity))
	storesCPU := max(0, float64(nc.StoresCPURate))
	sqlGatewayCPU := max(0, float64(nc.SQLGatewayCPUNanoPerSec))
	sqlDistCPU := max(0, float64(nc.SQLDistCPUNanoPerSec))

	// Step 1: Estimate the amplification factor and immovable CPU.
	//
	// See "SQL-aware decomposition" above. When SQL metrics are available, k is
	// derived from the full tracked denominator and capped at
	// maxCPUAmplification. Gateway SQL and any excess become immovable.
	var ampFactor, immovable float64
	hasSQLMetrics := (sqlGatewayCPU + sqlDistCPU) > 0
	if storesCPU <= 0 {
		ampFactor = maxCPUAmplification
		immovable = max(0, nodeUsage)
	} else if hasSQLMetrics {
		trackedMoveable := storesCPU + sqlDistCPU
		totalTracked := trackedMoveable + sqlGatewayCPU
		k := max(1, min(nodeUsage/totalTracked, maxCPUAmplification))
		ampFactor = (1 + sqlDistCPU/storesCPU) * k
		immovable = max(0, nodeUsage-trackedMoveable*k)
	} else {
		ampFactor = max(1, min(nodeUsage/storesCPU, maxCPUAmplification))
		immovable = max(0, nodeUsage-storesCPU*ampFactor)
	}

	// Invariant: sum of per-store loads equals nodeUsage, unless measurement lag
	// causes a transient overshoot (see assertCPULoadInvariant).
	assertCPULoadInvariant(storesCPU, ampFactor, immovable, nodeUsage, sqlDistCPU, sqlGatewayCPU)

	// Step 2: Compute per-store load and capacity.
	storeCPU := desc.Capacity.CPUPerSecond
	if storeCPU <= 0 {
		// Either because grunning is unsupported on the node's architecture, in
		// which case CPUPerSecond is -1, or during early startup before replicas
		// have reported CPU usage, in which case CPUPerSecond is 0.
		storeCPU = storesCPU / numStores
	}
	load := mmaprototype.LoadValue(storeCPU*ampFactor + immovable/numStores)
	capacity := mmaprototype.LoadValue(nodeCap / numStores)
	if nodeCap <= 0 {
		capacity = load * 2
	}

	return physicalDimension{
		load:                load,
		capacity:            capacity,
		amplificationFactor: mmaprototype.Amp(ampFactor),
	}
}

// minCapacity is the floor for per-store physical capacity in any dimension.
// This prevents zero-capacity values that could arise during early node startup
// or on empty stores. It is a last-resort guard against a truly zero value, not
// a load-dependent saturation clamp like the old model's cpuCapacityFloorPerStore.
//
// Concretely, 1.0 means 1 ns/s for CPURate and 1 byte for ByteSize.
//
// Note: The old capacity model(computeCPUCapacityWithCap) needed a more
// meaningful floor (cpuCapacityFloorPerStore = 0.1 cores = 1e8 ns/s) because
// its capacity formula subtracted background load from the node's CPU capacity:
// capacity = (nodeCap - background) / mult. As background grew, capacity shrank
// toward zero while store load stayed constant, sending utilization
// (load/capacity) to infinity. The physical model avoids this by keeping
// capacity fixed at nodeCap/numStores and folding background into the load side
// instead, so capacity never shrinks under load pressure and a negligible floor
// suffices.
const minCapacity mmaprototype.LoadValue = 1.0

// maxDiskSpaceAmplification caps the ratio of physical disk bytes used to
// logical (MVCC) bytes. Values above this are treated as if the extra physical
// usage is independent of range data (e.g. WAL, auxiliary files, tombstone
// bloat). Without this cap, transient LSM bloat could produce extreme
// amplification factors that massively overcount per-range physical footprint.
//
// TODO(wenyihu): revisit this value. 5.0 is a rough guess based on typical LSM
// amplification (2-4x) with headroom for compaction delays. Validate against
// production data once the physical model is deployed.
const maxDiskSpaceAmplification = 5.0

// computePhysicalDisk computes physical disk load, capacity, and space
// amplification factor from the store capacity in the store descriptor.
//
// Both load and capacity are in physical bytes so that load/capacity =
// Used/(Used+Available) = actual disk utilization. Note that capacity is
// Used+Available, not the total disk Capacity from the descriptor — the gap
// (Capacity - Available - Used) represents space consumed by the OS,
// filesystem metadata, and other applications. We exclude it from both sides
// so that disk utilization reflects only the portion of the disk the store
// can influence, consistent with SMA's FractionUsed.
//
// The amplification factor is Used/LogicalBytes, capped at
// maxDiskSpaceAmplification. Values below 1.0 (from compression) are
// preserved. Defaults to 1.0 for empty/new stores.
func computePhysicalDisk(desc roachpb.StoreDescriptor) (res physicalDimension) {
	defer func() { res.capacity = max(minCapacity, res.capacity) }()

	sc := desc.Capacity
	var ampFactor float64
	if sc.LogicalBytes > 0 && sc.Used > 0 {
		ampFactor = min(float64(sc.Used)/float64(sc.LogicalBytes), maxDiskSpaceAmplification)
	} else {
		ampFactor = 1.0
	}
	return physicalDimension{
		load:                mmaprototype.LoadValue(sc.Used),
		capacity:            mmaprototype.LoadValue(sc.Used + sc.Available),
		amplificationFactor: mmaprototype.Amp(ampFactor),
	}
}

// computePhysicalWriteBandwidth computes physical write-bandwidth load from the
// store descriptor.
//
// WriteBytesPerSecond is the sum of per-replica logical write bytes/s, not the
// actual physical disk write throughput. The amplification factor is set to 1.0
// because we don't have a measure of physical disk writes to compare against,
// so there is no meaningful ratio to compute. Capacity is unknown for the same
// reason: disk write throughput limits depend on hardware and workload
// characteristics that aren't available in the store descriptor.
func computePhysicalWriteBandwidth(desc roachpb.StoreDescriptor) physicalDimension {
	return physicalDimension{
		load:                mmaprototype.LoadValue(desc.Capacity.WriteBytesPerSecond),
		capacity:            mmaprototype.UnknownCapacity,
		amplificationFactor: mmaprototype.Amp(1.0),
	}
}

// computePhysicalStore computes the physical load, capacity, and amplification
// factors for a store across all load dimensions given its descriptor.
//
// Design note: MakeStoreLoadMsg and ComputeAmplificationFactors both consume a
// StoreDescriptor but obtain it through different paths. MakeStoreLoadMsg
// receives the descriptor from a gossip callback (the snapshot the store most
// recently published), while ComputeAmplificationFactors is called from
// mmaStore.amplificationFactors which constructs a descriptor from separately
// cached local values (CachedCapacity and GetNodeCapacity). Ideally both would
// use the exact same descriptor snapshot, but in practice the two may be from
// slightly different points in time. This is acceptable because:
//  1. The underlying inputs (node CPU EWMA, space amplification) are
//     slow-moving; the drift between two successive reads is negligible.
//  2. Store-level load != sum of per-range loads for several reasons:
//     (a) CPU store load includes background load (SQL gateway, GC, etc.)
//     that doesn't originate from ranges, (b) range load reports arrive
//     asynchronously, so in any given cycle some ranges have stale or
//     missing reports, (c) follower replicas contribute CPU to the store
//     total but don't report range-level load, and (d) ranges may join or
//     leave between snapshots. A small drift in amplification factors is
//     negligible compared to these structural gaps.
//
// If tighter consistency is ever needed, the result can be returned as a
// byproduct of MakeStoreLoadMsg and cached alongside the StoreLoadMsg.
func computePhysicalStore(desc roachpb.StoreDescriptor) (res physicalStore) {
	res.setDimension(mmaprototype.CPURate, computePhysicalCPU(desc))
	res.setDimension(mmaprototype.ByteSize, computePhysicalDisk(desc))
	res.setDimension(mmaprototype.WriteBandwidth, computePhysicalWriteBandwidth(desc))
	return res
}

func (r *physicalStore) setDimension(dim mmaprototype.LoadDimension, d physicalDimension) {
	r.load[dim] = d.load
	r.capacity[dim] = d.capacity
	r.amplificationFactors[dim] = d.amplificationFactor
}

// ComputeAmplificationFactors returns the per-dimension amplification factors
// for a store, given its descriptor. These factors convert logical per-range
// loads (direct replica CPU, MVCC bytes) into physical units for use at the
// MMA integration boundary.
func ComputeAmplificationFactors(desc roachpb.StoreDescriptor) mmaprototype.AmpVector {
	return computePhysicalStore(desc).amplificationFactors
}

// MakePhysicalRangeLoad converts logical per-range load measurements into a
// physical RangeLoad by applying the amplification factors. This is the single
// entry point for all logical-to-physical range load conversion and should be
// called at the integration boundary before passing range loads to MMA.
func MakePhysicalRangeLoad(
	requestCPUNanos, raftCPUNanos, writeBytesPerSec float64,
	logicalBytes int64,
	amp mmaprototype.AmpVector,
) mmaprototype.RangeLoad {
	var rl mmaprototype.RangeLoad
	cpuNanos := requestCPUNanos + raftCPUNanos
	rl.Load[mmaprototype.CPURate] = mmaprototype.LoadValue(cpuNanos * float64(amp[mmaprototype.CPURate]))
	rl.RaftCPU = mmaprototype.LoadValue(raftCPUNanos * float64(amp[mmaprototype.CPURate]))
	rl.Load[mmaprototype.WriteBandwidth] = mmaprototype.LoadValue(writeBytesPerSec * float64(amp[mmaprototype.WriteBandwidth]))
	rl.Load[mmaprototype.ByteSize] = mmaprototype.LoadValue(float64(logicalBytes) * float64(amp[mmaprototype.ByteSize]))
	return rl
}

// assertCPULoadInvariant verifies (in test builds only) that the sum of
// per-store CPU loads is consistent with nodeUsage.
//
// Each store's load is storeCPU_i * ampFactor + immovable/numStores. Summing
// across all stores (sum(storeCPU_i) = storesCPU):
//
//	sumLoad = storesCPU * ampFactor + immovable
//
// Substituting ampFactor = (1 + sqlDistCPU/storesCPU) * k:
//
//	storesCPU * ampFactor = (storesCPU + sqlDistCPU) * k
//	                      = trackedMoveable * k
//
// So sumLoad = trackedMoveable * k + immovable. Since
// immovable = max(0, nodeUsage - trackedMoveable*k):
//
//   - k unclamped (1 ≤ nodeUsage/totalTracked ≤ cap):
//     k = nodeUsage / totalTracked.
//     trackedMoveable * k ≤ nodeUsage (since trackedMoveable ≤ totalTracked).
//     immovable = nodeUsage - trackedMoveable*k ≥ 0.
//     sum = trackedMoveable*k + (nodeUsage - trackedMoveable*k) = nodeUsage.
//
//   - k clamped at cap (nodeUsage/totalTracked > cap):
//     k = cap. trackedMoveable * cap ≤ totalTracked * cap < nodeUsage.
//     immovable = nodeUsage - trackedMoveable*cap > 0.
//     sum = trackedMoveable*cap + (nodeUsage - trackedMoveable*cap) = nodeUsage.
//
//   - k floored at 1 (nodeUsage/totalTracked < 1, measurement lag):
//     k = 1.
//     If trackedMoveable ≤ nodeUsage (gateway CPU accounts for the gap):
//     immovable = nodeUsage - trackedMoveable ≥ 0.
//     sum = trackedMoveable + (nodeUsage - trackedMoveable) = nodeUsage.
//     If trackedMoveable > nodeUsage:
//     immovable = max(0, nodeUsage - trackedMoveable) = 0.
//     sum = trackedMoveable > nodeUsage.
//     Transient overshoot — stores appear slightly more loaded than
//     reality until measurements converge.
func assertCPULoadInvariant(
	storesCPU, ampFactor, immovable, nodeUsage, sqlDistCPU, sqlGatewayCPU float64,
) {
	if !buildutil.CrdbTestBuild {
		return
	}

	const floatEps = 1e-6
	approxEq := func(a, b float64) bool { return math.Abs(a-b) <= floatEps }
	moveable := storesCPU * ampFactor
	sumLoad := moveable + immovable
	ctx := context.Background()
	state := redact.Safe(fmt.Sprintf(
		"moveable=%.4f immovable=%.4f sumLoad=%.4f nodeUsage=%.4f "+
			"storesCPU=%.4f ampFactor=%.4f sqlDistCPU=%.4f sqlGatewayCPU=%.4f",
		moveable, immovable, sumLoad, nodeUsage,
		storesCPU, ampFactor, sqlDistCPU, sqlGatewayCPU))

	// immovable must be non-negative (it's clamped with max(0,...) at the call
	// site, but double-check here).
	if immovable < 0 {
		log.Dev.Fatalf(ctx, "CPU load invariant: immovable < 0 (%v)", state)
	}

	// Measurement lag: tracked moveable CPU (storesCPU + sqlDistCPU) exceeds
	// measured nodeUsage, so k is floored at 1 and moveable > nodeUsage. In
	// this case immovable = 0 and sumLoad = moveable > nodeUsage — a transient,
	// conservative overshoot that resolves as measurements converge.
	if (storesCPU + sqlDistCPU) > nodeUsage {
		if !approxEq(immovable, 0) {
			log.Dev.Fatalf(ctx, "CPU load invariant: measurement lag but immovable != 0 (%v)", state)
		}
		return
	}

	// Normal case: moveable + immovable must equal nodeUsage.
	if !approxEq(sumLoad, nodeUsage) {
		log.Dev.Fatalf(ctx, "CPU load invariant: sumLoad != nodeUsage (%v)", state)
	}
}

// assertCPUInputs verifies (in test builds only) that the raw NodeCapacity
// fields used by computePhysicalCPU are non-negative. All fields are int64 in
// the protobuf, so they can technically be negative but shouldn't be
// semantically. The caller clamps them with max(0, ...) after this check.
func assertCPUInputs(desc roachpb.StoreDescriptor) {
	if !buildutil.CrdbTestBuild {
		return
	}
	nc := desc.NodeCapacity
	// When !grunning.Supported (only in non-Bazel builds, i.e. plain
	// `go test`; all production binaries use Bazel), each store sets
	// CPUPerSecond to -1 and StoresCPURate (the sum across stores) is
	// -numStores. Skip those checks since the physical model handles
	// storesCPU <= 0 gracefully.
	storesCPURateNegative := nc.StoresCPURate < 0 && grunning.Supported
	storeCPURateNegative := desc.Capacity.CPUPerSecond < 0 && grunning.Supported
	if nc.NumStores < 0 || storesCPURateNegative || storeCPURateNegative ||
		nc.NodeCPURateUsage < 0 || nc.NodeCPURateCapacity < 0 ||
		nc.SQLGatewayCPUNanoPerSec < 0 || nc.SQLDistCPUNanoPerSec < 0 {
		log.Dev.Fatalf(context.Background(),
			"computePhysicalCPU: negative NodeCapacity fields "+
				"(NumStores=%d StoresCPURate=%d NodeCPURateUsage=%d "+
				"NodeCPURateCapacity=%d SQLGatewayCPU=%d SQLDistCPU=%d "+
				"grunning.Supported=%t)",
			nc.NumStores, nc.StoresCPURate, nc.NodeCPURateUsage,
			nc.NodeCPURateCapacity, nc.SQLGatewayCPUNanoPerSec, nc.SQLDistCPUNanoPerSec,
			grunning.Supported,
		)
	}
}
