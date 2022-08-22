// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package admission

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

// KVAdmissionControllerImpl implements KVAdmissionController interface.
type KVAdmissionControllerImpl struct {
	// Admission control queues and coordinators. Both should be nil or non-nil.
	kvAdmissionQ     *WorkQueue
	storeGrantCoords *StoreGrantCoordinators
	settings         *cluster.Settings
	every            log.EveryN
}

var _ KVAdmissionController = &KVAdmissionControllerImpl{}

type admissionHandle struct {
	tenantID                           roachpb.TenantID
	callAdmittedWorkDoneOnKVAdmissionQ bool
	storeAdmissionQ                    *StoreWorkQueue
	storeWorkHandle                    StoreWorkHandle
}

// MakeKVAdmissionController returns a KVAdmissionController. Both parameters
// must together either be nil or non-nil.
func MakeKVAdmissionController(
	kvAdmissionQ *WorkQueue, storeGrantCoords *StoreGrantCoordinators, settings *cluster.Settings,
) KVAdmissionController {
	return &KVAdmissionControllerImpl{
		kvAdmissionQ:     kvAdmissionQ,
		storeGrantCoords: storeGrantCoords,
		settings:         settings,
		every:            log.Every(10 * time.Second),
	}
}

// AdmitKVWork implements the KVAdmissionController interface.
func (n *KVAdmissionControllerImpl) AdmitKVWork(
	ctx context.Context, tenantID roachpb.TenantID, ba *roachpb.BatchRequest,
) (handle interface{}, err error) {
	ah := admissionHandle{tenantID: tenantID}
	if n.kvAdmissionQ != nil {
		bypassAdmission := ba.IsAdmin()
		source := ba.AdmissionHeader.Source
		if !roachpb.IsSystemTenantID(tenantID.ToUint64()) {
			// Request is from a SQL node.
			bypassAdmission = false
			source = roachpb.AdmissionHeader_FROM_SQL
		}
		if source == roachpb.AdmissionHeader_OTHER {
			bypassAdmission = true
		}
		createTime := ba.AdmissionHeader.CreateTime
		if !bypassAdmission && createTime == 0 {
			// TODO(sumeer): revisit this for multi-tenant. Specifically, the SQL use
			// of zero CreateTime needs to be revisited. It should use high priority.
			createTime = timeutil.Now().UnixNano()
		}
		admissionInfo := WorkInfo{
			TenantID:        tenantID,
			Priority:        admissionpb.WorkPriority(ba.AdmissionHeader.Priority),
			CreateTime:      createTime,
			BypassAdmission: bypassAdmission,
		}
		var err error
		// Don't subject HeartbeatTxnRequest to the storeAdmissionQ. Even though
		// it would bypass admission, it would consume a slot. When writes are
		// throttled, we start generating more txn heartbeats, which then consume
		// all the slots, causing no useful work to happen. We do want useful work
		// to continue even when throttling since there are often significant
		// number of tokens available.
		if ba.IsWrite() && !ba.IsSingleHeartbeatTxnRequest() {
			ah.storeAdmissionQ = n.storeGrantCoords.TryGetQueueForStore(int32(ba.Replica.StoreID))
		}
		admissionEnabled := true
		if ah.storeAdmissionQ != nil {
			ah.storeWorkHandle, err = ah.storeAdmissionQ.Admit(
				ctx, StoreWriteWorkInfo{WorkInfo: admissionInfo})
			if err != nil {
				return admissionHandle{}, err
			}
			if !ah.storeWorkHandle.AdmissionEnabled() {
				// Set storeAdmissionQ to nil so that we don't call AdmittedWorkDone
				// on it. Additionally, the code below will not call
				// kvAdmissionQ.Admit, and so callAdmittedWorkDoneOnKVAdmissionQ will
				// stay false.
				ah.storeAdmissionQ = nil
				admissionEnabled = false
			}
		}
		if admissionEnabled {
			ah.callAdmittedWorkDoneOnKVAdmissionQ, err = n.kvAdmissionQ.Admit(ctx, admissionInfo)
			if err != nil {
				if ah.storeAdmissionQ != nil {
					// No bytes were written.
					_ = ah.storeAdmissionQ.AdmittedWorkDone(ah.storeWorkHandle, StoreWorkDoneInfo{})
				}
				return admissionHandle{}, err
			}
		}
	}
	return ah, nil
}

// AdmittedKVWorkDone implements the KVAdmissionController interface.
func (n *KVAdmissionControllerImpl) AdmittedKVWorkDone(
	handle interface{}, writeBytes *StoreWorkDoneInfo,
) {
	ah := handle.(admissionHandle)
	if ah.callAdmittedWorkDoneOnKVAdmissionQ {
		n.kvAdmissionQ.AdmittedWorkDone(ah.tenantID)
	}
	if ah.storeAdmissionQ != nil {
		var doneInfo StoreWorkDoneInfo
		if writeBytes != nil {
			doneInfo = *writeBytes
		}
		err := ah.storeAdmissionQ.AdmittedWorkDone(ah.storeWorkHandle, doneInfo)
		if err != nil {
			// This shouldn't be happening.
			if util.RaceEnabled {
				log.Fatalf(context.Background(), "%s", errors.WithAssertionFailure(err))
			}
			if n.every.ShouldLog() {
				log.Errorf(context.Background(), "%s", err)
			}
		}
	}
}

// SetTenantWeightProvider implements the KVAdmissionController interface.
func (n *KVAdmissionControllerImpl) SetTenantWeightProvider(
	provider TenantWeightProvider, stopper *stop.Stopper,
) {
	go func() {
		const weightCalculationPeriod = 10 * time.Minute
		ticker := time.NewTicker(weightCalculationPeriod)
		// Used for short-circuiting the weights calculation if all weights are
		// disabled.
		allWeightsDisabled := false
		for {
			select {
			case <-ticker.C:
				kvDisabled := !KVTenantWeightsEnabled.Get(&n.settings.SV)
				kvStoresDisabled := !KVStoresTenantWeightsEnabled.Get(&n.settings.SV)
				if allWeightsDisabled && kvDisabled && kvStoresDisabled {
					// Have already transitioned to disabled, so noop.
					continue
				}
				weights := provider.GetTenantWeights()
				if kvDisabled {
					weights.Node = nil
				}
				n.kvAdmissionQ.SetTenantWeights(weights.Node)
				for _, storeWeights := range weights.Stores {
					q := n.storeGrantCoords.TryGetQueueForStore(int32(storeWeights.StoreID))
					if q != nil {
						if kvStoresDisabled {
							storeWeights.Weights = nil
						}
						q.SetTenantWeights(storeWeights.Weights)
					}
				}
				allWeightsDisabled = kvDisabled && kvStoresDisabled
			case <-stopper.ShouldQuiesce():
				ticker.Stop()
				return
			}
		}
	}()
}

// SnapshotIngested implements the KVAdmissionController interface.
func (n *KVAdmissionControllerImpl) SnapshotIngested(
	storeID roachpb.StoreID, ingestStats pebble.IngestOperationStats,
) {
	storeAdmissionQ := n.storeGrantCoords.TryGetQueueForStore(int32(storeID))
	if storeAdmissionQ == nil {
		return
	}
	storeAdmissionQ.StatsToIgnore(ingestStats)
}

// FollowerStoreWriteBytes implements the KVAdmissionController interface.
func (n *KVAdmissionControllerImpl) FollowerStoreWriteBytes(
	storeID roachpb.StoreID, followerWriteBytes FollowerStoreWriteBytes,
) {
	if followerWriteBytes.WriteBytes == 0 && followerWriteBytes.IngestedBytes == 0 {
		return
	}
	storeAdmissionQ := n.storeGrantCoords.TryGetQueueForStore(int32(storeID))
	if storeAdmissionQ == nil {
		return
	}
	storeAdmissionQ.BypassedWorkDone(
		followerWriteBytes.NumEntries, followerWriteBytes.StoreWorkDoneInfo)
}
