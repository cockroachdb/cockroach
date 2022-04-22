// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvadmissioncontroller

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// KVAdmissionController provides admission control for the KV layer.
type KVAdmissionController interface {
	// AdmitKVWork must be called before performing KV work.
	// BatchRequest.AdmissionHeader and BatchRequest.Replica.StoreID must be
	// populated for admission to work correctly. If err is non-nil, the
	// returned handle can be ignored. If err is nil, AdmittedKVWorkDone must be
	// called after the KV work is done executing.
	AdmitKVWork(
		ctx context.Context, tenantID roachpb.TenantID, ba *roachpb.BatchRequest,
	) (handle interface{}, err error)
	// AdmittedKVWorkDone is called after the admitted KV work is done
	// executing.
	AdmittedKVWorkDone(handle interface{})
	// SetTenantWeightProvider is used to set the provider that will be
	// periodically polled for weights. The stopper should be used to terminate
	// the periodic polling.
	SetTenantWeightProvider(provider TenantWeightProvider, stopper *stop.Stopper)
}

// TenantWeightProvider can be periodically asked to provide the tenant
// weights.
type TenantWeightProvider interface {
	GetTenantWeights() TenantWeights
}

// TenantWeights contains the various tenant weights.
type TenantWeights struct {
	// Node is the node level tenant ID => weight.
	Node map[uint64]uint32
	// Stores contains the per-store tenant weights.
	Stores []TenantWeightsForStore
}

// TenantWeightsForStore contains the tenant weights for a store.
type TenantWeightsForStore struct {
	roachpb.StoreID
	// Weights is tenant ID => weight.
	Weights map[uint64]uint32
}

// KVAdmissionControllerImpl implements KVAdmissionController interface.
type KVAdmissionControllerImpl struct {
	// Admission control queues and coordinators. Both should be nil or non-nil.
	kvAdmissionQ     *admission.WorkQueue
	storeGrantCoords *admission.StoreGrantCoordinators
	settings         *cluster.Settings
}

var _ KVAdmissionController = KVAdmissionControllerImpl{}

type admissionHandle struct {
	tenantID                           roachpb.TenantID
	callAdmittedWorkDoneOnKVAdmissionQ bool
	storeAdmissionQ                    *admission.WorkQueue
}

// MakeKVAdmissionController returns a KVAdmissionController. Both parameters
// must together either be nil or non-nil.
func MakeKVAdmissionController(
	kvAdmissionQ *admission.WorkQueue,
	storeGrantCoords *admission.StoreGrantCoordinators,
	settings *cluster.Settings,
) KVAdmissionController {
	return KVAdmissionControllerImpl{
		kvAdmissionQ:     kvAdmissionQ,
		storeGrantCoords: storeGrantCoords,
		settings:         settings,
	}
}

// AdmitKVWork implements the KVAdmissionController interface.
func (n KVAdmissionControllerImpl) AdmitKVWork(
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
		admissionInfo := admission.WorkInfo{
			TenantID:        tenantID,
			Priority:        admission.WorkPriority(ba.AdmissionHeader.Priority),
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
			if admissionEnabled, err = ah.storeAdmissionQ.Admit(ctx, admissionInfo); err != nil {
				return admissionHandle{}, err
			}
			if !admissionEnabled {
				// Set storeAdmissionQ to nil so that we don't call AdmittedWorkDone
				// on it. Additionally, the code below will not call
				// kvAdmissionQ.Admit, and so callAdmittedWorkDoneOnKVAdmissionQ will
				// stay false.
				ah.storeAdmissionQ = nil
			}
		}
		if admissionEnabled {
			ah.callAdmittedWorkDoneOnKVAdmissionQ, err = n.kvAdmissionQ.Admit(ctx, admissionInfo)
			if err != nil {
				return admissionHandle{}, err
			}
		}
	}
	return ah, nil
}

// AdmittedKVWorkDone implements the KVAdmissionController interface.
func (n KVAdmissionControllerImpl) AdmittedKVWorkDone(handle interface{}) {
	ah := handle.(admissionHandle)
	if ah.callAdmittedWorkDoneOnKVAdmissionQ {
		n.kvAdmissionQ.AdmittedWorkDone(ah.tenantID)
	}
	if ah.storeAdmissionQ != nil {
		ah.storeAdmissionQ.AdmittedWorkDone(ah.tenantID)
	}
}

// SetTenantWeightProvider implements the KVAdmissionController interface.
func (n KVAdmissionControllerImpl) SetTenantWeightProvider(
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
				kvDisabled := !admission.KVTenantWeightsEnabled.Get(&n.settings.SV)
				kvStoresDisabled := !admission.KVStoresTenantWeightsEnabled.Get(&n.settings.SV)
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
