// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

// changeState represents the state of a registered change.
type changeState int

const rangeID = 1

var rangeState = map[roachpb.RangeID]struct {
	leaseholder roachpb.StoreID
	replicas    []roachpb.ReplicaDescriptor
	usage       allocator.RangeUsageInfo
}{
	rangeID: {
		leaseholder: 1,
		replicas: []roachpb.ReplicaDescriptor{
			{StoreID: 1, NodeID: 1, ReplicaID: 1, Type: roachpb.VOTER_FULL},
			{StoreID: 2, NodeID: 2, ReplicaID: 2, Type: roachpb.NON_VOTER},
		},
		usage: allocator.RangeUsageInfo{
			RequestCPUNanosPerSecond: 100,
			RaftCPUNanosPerSecond:    50,
			WriteBytesPerSecond:      1024,
			LogicalBytes:             2048,
		},
	},
}

const (
	// changeRegistered indicates the change has been registered but not enacted.
	changeRegistered changeState = iota
	// changeSucceeded indicates the change was successfully enacted.
	changeSucceeded
	// changeFailed indicates the change failed to enact.
	changeFailed
)

// mockMMAAllocator implements the mmaAllocator interface for testing.
type mockMMAAllocator struct {
	changeSeqGen mmaprototype.ChangeID
	// tracks registered changes and their state
	changes map[mmaprototype.ChangeID]changeState
}

func printRangeState(rangeID roachpb.RangeID) string {
	if _, ok := rangeState[rangeID]; !ok {
		return fmt.Sprintf("range %d not found", rangeID)
	}
	var sb strings.Builder
	_, _ = fmt.Fprintf(&sb, "range_id=%d", rangeID)
	_, _ = fmt.Fprintf(&sb, " [replicas:{")
	for i, replica := range rangeState[rangeID].replicas {
		if i > 0 {
			_, _ = fmt.Fprintf(&sb, ",")
		}
		var isLeaseholder string
		if replica.StoreID == rangeState[rangeID].leaseholder {
			isLeaseholder = "*"
		}
		var replicaType string
		if replica.Type == roachpb.VOTER_FULL {
			replicaType = "voter"
		} else if replica.Type == roachpb.NON_VOTER {
			replicaType = "non-voter"
		} else {
			panic(fmt.Sprintf("unknown replica type: %s", replica.Type))
		}
		_, _ = fmt.Fprintf(&sb, "(r%d%s:n%d,s%d,%s)",
			replica.ReplicaID, isLeaseholder, replica.NodeID, replica.StoreID, replicaType)
	}
	_, _ = fmt.Fprintf(&sb, "} usage=%v]", mmaRangeLoad(rangeState[rangeID].usage))
	return sb.String()
}

func (m *mockMMAAllocator) String() string {
	var sb strings.Builder

	var ids []mmaprototype.ChangeID
	for id := range m.changes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) == 0 {
		_, _ = fmt.Fprintf(&sb, "\tempty")
	} else {
		_, _ = fmt.Fprintf(&sb, "\t")
	}
	for _, id := range ids {
		state := m.changes[id]
		if id > 0 {
			sb.WriteString(", ")
		}
		_, _ = fmt.Fprintf(&sb, "cid(%d)=", id)
		switch state {
		case changeRegistered:
			_, _ = fmt.Fprintf(&sb, "pending")
		case changeSucceeded:
			_, _ = fmt.Fprintf(&sb, "success")
		case changeFailed:
			_, _ = fmt.Fprintf(&sb, "failed")
		}
	}
	_, _ = fmt.Fprintf(&sb, "\n")
	return sb.String()
}

type storeLoad struct {
	cpu            float64
	writeBandwidth float64
	byteSize       int64
	leaseCount     int
	replicaCount   int
}

// mockStorePool implements the storePool interface for testing.
type mockStorePool struct {
	// Track store loads.
	load map[roachpb.StoreID]storeLoad
}

// UpdateLocalStoresAfterLeaseTransfer updates the mock store pool after a lease
// transfer.
func (m *mockStorePool) UpdateLocalStoresAfterLeaseTransfer(
	removeFrom, addTo roachpb.StoreID, usage allocator.RangeUsageInfo,
) {
	// Update CPU loads after lease transfer.
	deltaLoad := storeLoad{
		cpu:        usage.RequestCPUNanosPerSecond, //non-raft cpu
		leaseCount: 1,
	}
	// Lease transfer.
	addToLoad := m.load[addTo]
	addToLoad.cpu += deltaLoad.cpu
	addToLoad.leaseCount += deltaLoad.leaseCount
	m.load[addTo] = addToLoad

	removeFromLoad := m.load[removeFrom]
	removeFromLoad.cpu -= deltaLoad.cpu
	removeFromLoad.leaseCount -= deltaLoad.leaseCount
	m.load[removeFrom] = removeFromLoad
}

// UpdateLocalStoreAfterRebalance updates the mock store pool after a rebalance like storepool.UpdateLocalStoreAfterRebalance.
// TODO(wenyihu6): should this function update RequestCPUNanosPerSecond if there is a lease transfer?
func (m *mockStorePool) UpdateLocalStoreAfterRebalance(
	storeID roachpb.StoreID, usage allocator.RangeUsageInfo, changeType roachpb.ReplicaChangeType,
) {
	deltaLoad := storeLoad{
		cpu:            usage.RaftCPUNanosPerSecond,
		writeBandwidth: usage.WriteBytesPerSecond,
		byteSize:       usage.LogicalBytes,
		replicaCount:   1,
	}
	switch changeType {
	case roachpb.ADD_VOTER, roachpb.ADD_NON_VOTER:
		addToLoad := m.load[storeID]
		addToLoad.cpu += deltaLoad.cpu
		addToLoad.writeBandwidth += deltaLoad.writeBandwidth
		addToLoad.byteSize += deltaLoad.byteSize
		addToLoad.replicaCount += deltaLoad.replicaCount
		m.load[storeID] = addToLoad

	case roachpb.REMOVE_VOTER, roachpb.REMOVE_NON_VOTER:
		removeFromLoad := m.load[storeID]
		removeFromLoad.cpu -= deltaLoad.cpu
		removeFromLoad.writeBandwidth -= deltaLoad.writeBandwidth
		removeFromLoad.byteSize -= deltaLoad.byteSize
		removeFromLoad.replicaCount -= deltaLoad.replicaCount
		m.load[storeID] = removeFromLoad
	}
}

// String implements the Stringer interface for mockStorePool.
func (m *mockStorePool) String() string {
	var sb strings.Builder
	storeIDs := make([]roachpb.StoreID, 0, len(m.load))
	for storeID := range m.load {
		storeIDs = append(storeIDs, storeID)
	}
	sort.Slice(storeIDs, func(i, j int) bool {
		return storeIDs[i] < storeIDs[j]
	})
	for _, storeID := range storeIDs {
		load := m.load[storeID]
		_, _ = fmt.Fprintf(&sb, "\ts%d: (cpu=%.2f, write_band=%.2f, byte_size=%d, lease_count=%d, replica_count=%d)\n",
			storeID, load.cpu, load.writeBandwidth, load.byteSize, load.leaseCount, load.replicaCount)
	}
	return sb.String()
}

// AdjustPendingChangesDisposition informs mockMMAAllocator that the pending
// changes have been applied.
func (m *mockMMAAllocator) AdjustPendingChangesDisposition(
	changes []mmaprototype.ChangeID, success bool,
) {
	for _, id := range changes {
		if _, ok := m.changes[id]; !ok {
			panic(fmt.Sprintf("change %d not found", id))
		}
		if success {
			m.changes[id] = changeSucceeded
		} else {
			m.changes[id] = changeFailed
		}
	}
}

// RegisterExternalChanges informs mockMMAAllocator that the external changes
// have been registered.
func (m *mockMMAAllocator) RegisterExternalChanges(
	changes []mmaprototype.ReplicaChange,
) []mmaprototype.ChangeID {
	changeIDs := make([]mmaprototype.ChangeID, len(changes))
	for i := range changeIDs {
		id := m.nextChangeID()
		changeIDs[i] = id
		m.changes[id] = changeRegistered // Register change as pending
	}
	return changeIDs
}

func (m *mockMMAAllocator) nextChangeID() mmaprototype.ChangeID {
	id := m.changeSeqGen
	m.changeSeqGen++
	return id
}

func (m *mockMMAAllocator) makeMMAPendingRangeChange(
	rangeID roachpb.RangeID, replicaChanges []mmaprototype.ReplicaChange,
) mmaprototype.PendingRangeChange {
	changeIDs := make([]mmaprototype.ChangeID, len(replicaChanges))
	for i := range changeIDs {
		changeIDs[i] = m.nextChangeID()
		m.changes[changeIDs[i]] = changeRegistered
	}
	return mmaprototype.MakePendingRangeChangeForTesting(rangeID, replicaChanges, changeIDs)
}

// createTestAllocatorSync creates a test allocator sync with mock dependencies
// and some predefined ranges.
func createTestAllocatorSync(mmaEnabled bool) (*AllocatorSync, *mockMMAAllocator, *mockStorePool) {
	st := cluster.MakeTestingClusterSettings()
	mma := &mockMMAAllocator{
		changes: make(map[mmaprototype.ChangeID]changeState),
	}
	sp := &mockStorePool{
		load: make(map[roachpb.StoreID]storeLoad),
	}
	for i := 1; i <= storeCount; i++ {
		sp.load[roachpb.StoreID(i)] = storeLoad{}
	}
	for _, replica := range rangeState[rangeID].replicas {
		load := sp.load[replica.StoreID]
		load.cpu += rangeState[rangeID].usage.RaftCPUNanosPerSecond
		load.writeBandwidth += rangeState[rangeID].usage.WriteBytesPerSecond
		load.byteSize += rangeState[rangeID].usage.LogicalBytes
		load.replicaCount++
		sp.load[replica.StoreID] = load
	}
	leaseholderLoad := sp.load[rangeState[rangeID].leaseholder]
	leaseholderLoad.cpu += rangeState[rangeID].usage.RequestCPUNanosPerSecond
	leaseholderLoad.leaseCount++
	sp.load[rangeState[rangeID].leaseholder] = leaseholderLoad

	if mmaEnabled {
		kvserverbase.LoadBasedRebalancingMode.Override(context.Background(), &st.SV, kvserverbase.LBRebalancingMultiMetric)
	}
	as := NewAllocatorSync(sp, mma, st)
	return as, mma, sp
}

// getTracked is a helper function to get a tracked change from the allocator sync
// without deleting the change.
func getTracked(as *AllocatorSync, id SyncChangeID) (trackedAllocatorChange, bool) {
	as.mu.Lock()
	defer as.mu.Unlock()
	change, ok := as.mu.trackedChanges[id]
	return change, ok
}

func makeAddNonVoterOp(target int) changeReplicasOp {
	return changeReplicasOp{
		chgs: kvpb.ReplicationChanges{
			kvpb.ReplicationChange{
				ChangeType: roachpb.ADD_NON_VOTER,
				Target:     roachpb.ReplicationTarget{StoreID: roachpb.StoreID(target), NodeID: roachpb.NodeID(target)},
			},
		},
	}
}

func makeReplaceVoterWithPromotionOp(from, to int) changeReplicasOp {
	return changeReplicasOp{
		chgs: kvpb.ReplicationChanges{
			kvpb.ReplicationChange{
				ChangeType: roachpb.ADD_VOTER,
				Target:     roachpb.ReplicationTarget{StoreID: roachpb.StoreID(to), NodeID: roachpb.NodeID(to)},
			},
			kvpb.ReplicationChange{
				ChangeType: roachpb.REMOVE_NON_VOTER,
				Target:     roachpb.ReplicationTarget{StoreID: roachpb.StoreID(to), NodeID: roachpb.NodeID(to)},
			},
			kvpb.ReplicationChange{
				ChangeType: roachpb.REMOVE_VOTER,
				Target:     roachpb.ReplicationTarget{StoreID: roachpb.StoreID(from), NodeID: roachpb.NodeID(from)},
			},
		},
	}
}

func makeRebalanceVoterOpWithPromotionAndDemotion(from, to int) changeReplicasOp {
	return changeReplicasOp{
		chgs: kvpb.ReplicationChanges{
			// Promotion.
			kvpb.ReplicationChange{
				ChangeType: roachpb.REMOVE_NON_VOTER,
				Target:     roachpb.ReplicationTarget{StoreID: roachpb.StoreID(to), NodeID: roachpb.NodeID(to)},
			},
			kvpb.ReplicationChange{
				ChangeType: roachpb.ADD_VOTER,
				Target:     roachpb.ReplicationTarget{StoreID: roachpb.StoreID(to), NodeID: roachpb.NodeID(to)},
			},
			// Demotion.
			kvpb.ReplicationChange{
				ChangeType: roachpb.ADD_NON_VOTER,
				Target:     roachpb.ReplicationTarget{StoreID: roachpb.StoreID(from), NodeID: roachpb.NodeID(from)},
			},
			kvpb.ReplicationChange{
				ChangeType: roachpb.REMOVE_VOTER,
				Target:     roachpb.ReplicationTarget{StoreID: roachpb.StoreID(from), NodeID: roachpb.NodeID(from)},
			},
		},
	}
}

func makeChangeReplicasOperation(opType string, from, to int) changeReplicasOp {
	switch opType {
	case "add-nonvoter":
		return makeAddNonVoterOp(to)
	case "replace-voter-with-promotion":
		return makeReplaceVoterWithPromotionOp(from, to)
	case "rebalance-voter-with-promotion-and-demotion":
		return makeRebalanceVoterOpWithPromotionAndDemotion(from, to)
	default:
		panic(fmt.Sprintf("unknown operation: %s", opType))
	}
}

func printReplicaChange(change []mmaprototype.ReplicaChange) string {
	var sb strings.Builder
	_, _ = fmt.Fprintf(&sb, "\t[")
	for i, c := range change {
		if i > 0 {
			_, _ = fmt.Fprintf(&sb, "\t")
		}
		_, _ = fmt.Fprintf(&sb, "%s", c.String())
		if i < len(change)-1 {
			_, _ = fmt.Fprintf(&sb, "\n")
		}
	}
	_, _ = fmt.Fprintf(&sb, "]")
	return sb.String()
}

func printTrackedChange(change trackedAllocatorChange) string {
	var sb strings.Builder
	if len(change.changeIDs) > 0 {
		_, _ = fmt.Fprintf(&sb, "cid:%v", change.changeIDs)
	} else {
		_, _ = fmt.Fprintf(&sb, "cid:empty")
	}
	switch {
	case change.leaseTransferOp != nil:
		_, _ = fmt.Fprintf(&sb, ", lease_transfer_from:s%d->s%d",
			change.leaseTransferOp.transferFrom, change.leaseTransferOp.transferTo)
	case change.changeReplicasOp != nil:
		_, _ = fmt.Fprintf(&sb, ", change_replicas:%v", change.changeReplicasOp.chgs)
	}
	_, _ = fmt.Fprintf(&sb, ", {request_cpu:%.1f, raft_cpu:%.1f, write_bytes:%.1f, logical_bytes:%d}",
		change.usage.RequestCPUNanosPerSecond, change.usage.RaftCPUNanosPerSecond,
		change.usage.WriteBytesPerSecond, change.usage.LogicalBytes)
	return sb.String()
}

func printAllocatorSync(as *AllocatorSync) string {
	var sb strings.Builder
	_, _ = fmt.Fprintf(&sb, "\ttracked:[")
	var ids []int
	for id := range as.mu.trackedChanges {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)

	for i, id := range ids {
		change := as.mu.trackedChanges[SyncChangeID(id)]
		if i > 0 {
			_, _ = fmt.Fprintf(&sb, ",")
		}
		_, _ = fmt.Fprintf(&sb, "sync_id=%d->(%s)", id, printTrackedChange(change))
	}
	_, _ = fmt.Fprintf(&sb, "]\n")
	return sb.String()
}

var fromSupported = map[string]bool{
	"add-nonvoter":                                false,
	"replace-voter-with-promotion":                true,
	"rebalance-voter-with-promotion-and-demotion": true,
}

const storeCount = 4

// TestDataDriven is a data-driven test for the allocator sync functionality.
// It provides the following commands:
//
//   - "init"
//     Initialize a new allocator sync with mock dependencies.
//     Args: mma_enabled=<bool>
//
//   - "pre-apply-lease-transfer"
//     Register a lease transfer operation with the allocator sync.
//     Args: range_id=<int> from=<int> to=<int> from_mma=<bool>
//
//   - "pre-apply-change-replicas"
//     Register a replica change operation with the allocator sync.
//     Args: range_id=<int> type=<string> from=<int> to=<int> from_mma=<bool>
//
//   - "mark-changes-failed"
//     Mark specific change IDs as failed.
//     Args: change_ids=<comma-separated list>
//
//   - "get-tracked-change"
//     Get details about a tracked change.
//     Args: sync_id=<int>
//
//   - "post-apply"
//     Apply a tracked change with success or failure.
//     Args: id=<int> success=<bool>
//
//   - "print"
//     Print the current state of the allocator sync, MMA state and store pool.
//
//   - "make-operation"
//     Create a replication operation based on the given type and store IDs.
//     Args: type=<string> to=<int> from=<int>
//
//   - "convert-lease-transfer"
//     Convert a lease transfer operation to MMA format.
//     Args: transfer_from=<int> transfer_to=<int>
//
//   - "convert-replica-change"
//     Convert a replica change operation to MMA format.
//     Args: change=<type>,<store_id> (multiple)
func TestDataDrivenMMAIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, datapathutils.TestDataPath(t, "mmaintegration"), func(t *testing.T, path string) {
		var as *AllocatorSync
		var mma *mockMMAAllocator
		var sp *mockStorePool

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				var mmaEnabled bool
				d.ScanArgs(t, "mma_enabled", &mmaEnabled)
				as, mma, sp = createTestAllocatorSync(mmaEnabled)
				return printRangeState(rangeID)
			case "mark-changes-failed":
				var changeIDsStr string
				d.ScanArgs(t, "change_ids", &changeIDsStr)

				changeIDStrs := strings.Split(changeIDsStr, ",")
				var changeIDs []mmaprototype.ChangeID
				for _, idStr := range changeIDStrs {
					id, err := strconv.Atoi(strings.TrimSpace(idStr))
					if err != nil {
						d.Fatalf(t, "invalid change ID: %s", idStr)
					}
					changeIDs = append(changeIDs, mmaprototype.ChangeID(id))
				}
				as.MarkChangesAsFailed(changeIDs)
				return fmt.Sprintf("marked %v as failed", changeIDs)
			case "get-tracked-change":
				var id int
				d.ScanArgs(t, "sync_id", &id)
				change, ok := getTracked(as, SyncChangeID(id))
				if !ok {
					return fmt.Sprintf("change not found: %d", id)
				}
				return printTrackedChange(change)

			case "post-apply":
				var id int
				var success bool
				d.ScanArgs(t, "id", &id)
				d.ScanArgs(t, "success", &success)
				as.PostApply(SyncChangeID(id), success)
				return fmt.Sprintf("applied change %d with success=%v", id, success)

			case "print":
				var stringBuilder strings.Builder
				_, _ = fmt.Fprintf(&stringBuilder, "allocator_sync:\n%s", printAllocatorSync(as))
				_, _ = fmt.Fprintf(&stringBuilder, "mma_state:\n%s", mma.String())
				_, _ = fmt.Fprintf(&stringBuilder, "store_pool:\n%s", sp.String())
				return stringBuilder.String()

			case "pre-apply-lease-transfer":
				var to, from, rangeID int
				var fromMMA bool
				d.ScanArgs(t, "to", &to)
				d.ScanArgs(t, "from", &from)
				d.ScanArgs(t, "range_id", &rangeID)
				d.ScanArgs(t, "from_mma", &fromMMA)
				rangeState := rangeState[roachpb.RangeID(rangeID)]
				var stringBuilder strings.Builder
				desc := &roachpb.RangeDescriptor{
					RangeID:          roachpb.RangeID(rangeID),
					InternalReplicas: rangeState.replicas,
				}
				if to > storeCount || from > storeCount {
					return fmt.Sprintf("invalid target or from store id: %d, %d", to, from)
				}
				fromID := roachpb.ReplicationTarget{StoreID: roachpb.StoreID(from), NodeID: roachpb.NodeID(from)}
				toID := roachpb.ReplicationTarget{StoreID: roachpb.StoreID(to), NodeID: roachpb.NodeID(to)}
				mmaChange := convertLeaseTransferToMMA(
					desc,
					rangeState.usage,
					fromID,
					toID,
				)
				var syncID SyncChangeID
				if fromMMA {
					prc := mma.makeMMAPendingRangeChange(
						roachpb.RangeID(rangeID),
						mmaChange,
					)
					syncID = as.MMAPreApply(rangeState.usage, prc)
				} else {
					syncID = as.NonMMAPreTransferLease(
						desc,
						rangeState.usage,
						fromID,
						toID,
					)
				}
				_, _ = fmt.Fprintf(&stringBuilder, "sync_change_id:%d\n", syncID)
				_, _ = fmt.Fprintf(&stringBuilder, "mma_change:\n%v\n", printReplicaChange(mmaChange))
				return stringBuilder.String()
			case "pre-apply-change-replicas":
				var opType string
				var to, from, rangeID int
				var fromMMA bool
				d.ScanArgs(t, "type", &opType)
				d.ScanArgs(t, "to", &to)
				hasFrom := d.MaybeScanArgs(t, "from", &from)
				if b, ok := fromSupported[opType]; ok && b != hasFrom {
					panic(fmt.Sprintf("%v from(%t)!=from_supported(%t)", opType, hasFrom, b))
				}
				d.ScanArgs(t, "range_id", &rangeID)
				d.ScanArgs(t, "from_mma", &fromMMA)
				op := makeChangeReplicasOperation(opType, from, to)
				if to > storeCount || from > storeCount {
					return fmt.Sprintf("invalid target or from store id: %d, %d", to, from)
				}
				rangeState := rangeState[roachpb.RangeID(rangeID)]
				var stringBuilder strings.Builder
				desc := &roachpb.RangeDescriptor{
					RangeID:          roachpb.RangeID(rangeID),
					InternalReplicas: rangeState.replicas,
				}
				mmaChange := convertReplicaChangeToMMA(
					desc,
					rangeState.usage,
					op.chgs,
					rangeState.leaseholder,
				)
				var syncID SyncChangeID
				if fromMMA {
					prc := mma.makeMMAPendingRangeChange(
						roachpb.RangeID(rangeID),
						mmaChange,
					)
					syncID = as.MMAPreApply(rangeState.usage, prc)
				} else {
					syncID = as.NonMMAPreChangeReplicas(
						desc,
						rangeState.usage,
						op.chgs,
						rangeState.leaseholder,
					)
				}
				_, _ = fmt.Fprintf(&stringBuilder, "sync_change_id:%d\n", syncID)
				_, _ = fmt.Fprintf(&stringBuilder, "mma_change:\n%v\n", printReplicaChange(mmaChange))
				return stringBuilder.String()
			default:
				d.Fatalf(t, "unknown command: %s", d.Cmd)
				return ""
			}
		})
	})
}
