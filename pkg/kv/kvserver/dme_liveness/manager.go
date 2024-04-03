// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package dme_liveness

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/dme_liveness/dme_livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// TODO(sumeer):
// x- synchronization
//     x- managerImpl
//     x- localStoreState
//     x- heartbeatAndSupport
//     x- message sender
//     x- can persistent storage be getting updated concurrently due to a race because releasing mutex early?
//
// - integrate manager with kvserver.
//   x- know a local store and its identity when call Store.Start. May need to change
//     manager init interface to add local store, to match the looping we do there.
//   **- add remote stores.
//   x- for outgoing messages: nodedialer
//   x- for incoming messages: plumb it via node, by registering the server like we do
//     kvpb.RegisterInternalServer(grpcServer.Server, node)
// x- implement StorageProvider
// x- implement MessageSender
// x- basic unit test
// - basic caching
// *- manual integration testing
// - ensure highly concurrent

// Lock ordering:
//
//	  managerImpl.mu > localStoreState.mu
//		localStoreState.mu > lockedEpoch.mu
//		heartbeatAndSupport.mu > lockedEpoch.mu
//
// TODO: check if we actually need localStoreState.mu > heartbeatAndSupport.mu
// anymore. We need to be careful here since there are some places where
// heartbeatAndSupport, after dropping its own mu, calls
// PropagateLocalSupportStateSoon. If the 	implementation of
// PropagateLocalSupportSoon chose to immediately call into 	localStoreState
// and localStoreState.mu was already held, we would have a deadlock.

type localStoreState struct {
	storeID dme_livenesspb.StoreIdentifier
	StorageProvider
	MessageSender

	// Protected by mu.
	epoch lockedEpoch
	mu    syncutil.RWMutex
	// Protected by mu. Do not call any method on a heartbeatAndSupport while
	// holding mu.
	heartbeatAndSupportMap map[dme_livenesspb.StoreIdentifier]*heartbeatAndSupport

	parent *managerImpl
}

type lockedEpoch struct {
	mu    syncutil.Mutex
	epoch int64
}

func (le *lockedEpoch) getEpoch() int64 {
	le.mu.Lock()
	defer le.mu.Unlock()
	return le.epoch
}

func (le *lockedEpoch) incrementEpoch(s StorageProvider) error {
	le.mu.Lock()
	defer le.mu.Unlock()
	err := s.UpdateCurrentEpoch(le.epoch + 1)
	if err != nil {
		return err
	}
	le.epoch++
	return nil
}

func newLocalStoreState(ls LocalStore, parent *managerImpl) (*localStoreState, error) {
	epoch, err := ls.StorageProvider.ReadCurrentEpoch()
	if err != nil {
		return nil, err
	}
	epoch++
	if err := ls.StorageProvider.UpdateCurrentEpoch(epoch); err != nil {
		return nil, err
	}
	heartbeatAndSupportMap := map[dme_livenesspb.StoreIdentifier]*heartbeatAndSupport{}
	err = ls.StorageProvider.ReadSupportBySelfFor(
		func(store dme_livenesspb.StoreIdentifier, support dme_livenesspb.Support) {
			hbs := newHeartbeatAndSupportStruct(store, epoch, support)
			heartbeatAndSupportMap[store] = hbs
		})
	if err != nil {
		return nil, err
	}
	lss := &localStoreState{
		storeID: dme_livenesspb.StoreIdentifier{
			NodeID:  parent.o.NodeID,
			StoreID: ls.StoreID,
		},
		StorageProvider:        ls.StorageProvider,
		MessageSender:          ls.MessageSender,
		epoch:                  lockedEpoch{epoch: epoch},
		heartbeatAndSupportMap: heartbeatAndSupportMap,
		parent:                 parent,
	}
	lss.MessageSender.SetHandleHeartbeatResponseInterface(lss)
	for storeID, hbs := range heartbeatAndSupportMap {
		lss.MessageSender.AddRemoteStore(storeID)
		hbs.start(lss)
	}
	lss.MessageSender.SetSupportStateProvider(lss)

	return lss, nil
}

func (lss *localStoreState) addRemoteStore(storeID dme_livenesspb.StoreIdentifier) {
	hbs := func() *heartbeatAndSupport {
		lss.mu.Lock()
		defer lss.mu.Unlock()
		_, ok := lss.heartbeatAndSupportMap[storeID]
		if ok {
			return nil
		}
		hbs := newHeartbeatAndSupportStruct(storeID, lss.epoch.getEpoch(), dme_livenesspb.Support{})
		lss.heartbeatAndSupportMap[storeID] = hbs
		return hbs
	}()
	if hbs == nil {
		return
	}
	hbs.start(lss)
	lss.MessageSender.AddRemoteStore(storeID)
	lss.MessageSender.PropagateLocalSupportStateSoon()
}

func (lss *localStoreState) removeRemoteStore(storeID dme_livenesspb.StoreIdentifier) {
	hbs := func() *heartbeatAndSupport {
		lss.mu.Lock()
		defer lss.mu.Unlock()
		hbs, ok := lss.heartbeatAndSupportMap[storeID]
		if !ok {
			return nil
		}
		delete(lss.heartbeatAndSupportMap, storeID)
		return hbs
	}()
	hbs.close()
	lss.MessageSender.RemoveRemoteStore(storeID)
}

func (lss *localStoreState) close() {
	var hbss []*heartbeatAndSupport
	func() {
		lss.mu.RLock()
		defer lss.mu.RUnlock()
		for _, hbs := range lss.heartbeatAndSupportMap {
			hbss = append(hbss, hbs)
		}
	}()
	for _, hbs := range hbss {
		hbs.close()
	}
}

func (lss *localStoreState) handleHeartbeat(
	from dme_livenesspb.StoreIdentifier, msg dme_livenesspb.Heartbeat,
) (ack bool, err error) {
	lss.mu.RLock()
	hbs, ok := lss.heartbeatAndSupportMap[from]
	lss.mu.RUnlock()
	if !ok {
		return false, errors.Errorf("unknown store s%s", from.StoreID)
	}
	return hbs.handleHeartbeat(msg)
}

func (lss *localStoreState) HandleHeartbeatResponse(
	from dme_livenesspb.StoreIdentifier,
	msg dme_livenesspb.Heartbeat,
	responderTime hlc.ClockTimestamp,
	ack bool,
) error {
	lss.mu.RLock()
	hbs, ok := lss.heartbeatAndSupportMap[from]
	lss.mu.RUnlock()
	if !ok {
		return errors.Errorf("unknown store s%s", from.StoreID)
	}
	return hbs.handleHeartbeatResponse(msg, responderTime, ack)
}

func (lss *localStoreState) handleSupportState(
	from dme_livenesspb.StoreIdentifier, msg dme_livenesspb.SupportState,
) error {
	lss.mu.RLock()
	hbs, ok := lss.heartbeatAndSupportMap[from]
	lss.mu.RUnlock()
	if !ok {
		return errors.Errorf("unknown store s%s", from.StoreID)
	}
	return hbs.handleSupportState(msg)
}

// LocalSupportState implements SupportStateForPropagationProvider.
func (lss *localStoreState) LocalSupportState() dme_livenesspb.SupportState {
	lss.parent.metrics.SupportStateSent.Inc(1)
	var forSelfBy dme_livenesspb.SupportForOtherBy
	forSelfBy.For = lss.storeID
	var bySelfFor dme_livenesspb.SupportByOtherFor
	bySelfFor.By = lss.storeID
	var hbss []*heartbeatAndSupport
	func() {
		lss.mu.RLock()
		defer lss.mu.RUnlock()
		for _, hbs := range lss.heartbeatAndSupportMap {
			hbss = append(hbss, hbs)
		}
	}()
	getState := func(hbs *heartbeatAndSupport) {
		hbs.mu.RLock()
		defer hbs.mu.RUnlock()
		forSelfBy.Support = append(forSelfBy.Support, dme_livenesspb.SupportForOtherBySingle{
			By:      hbs.storeID,
			Support: hbs.forSelfByStoreID,
		})
		bySelfFor.Support = append(bySelfFor.Support, dme_livenesspb.SupportByOtherForSingle{
			For:     hbs.storeID,
			Support: hbs.bySelfForStoreID,
		})
	}
	for _, hbs := range hbss {
		getState(hbs)
	}
	return dme_livenesspb.SupportState{
		StateTime:  lss.parent.o.Clock.NowAsClockTimestamp(),
		ForOtherBy: forSelfBy,
		ByOtherFor: bySelfFor,
	}
}

// computeSupport ...
//
// ts1 is <= clock. The state in the data-structure can be from a time >= ts1,
// i.e., the future of ts1. We sample the clock again, after reading the
// state. The state in the data-structure is from a time <= ts2. We discuss
// below why ts2 is needed for correctness.
//
// Consider the individual SupportStatus values from each replica:
//
// - Supported: A necessary condition is EndTime > ts2.
//
//   - epoch param is provided: The supported_epoch must be <= epoch. This
//     case did not need to use ts2 for correctness, since support for ts1 is
//     there, even if the state examined is from the future. By using ts2, we
//     have only strengthened the support requirement (we could fall into the
//     CurrentlyUnsupported case, which is harmless).
//
//   - epoch param is not provided: we sample the epoch (sampled_epoch) and
//     use it as a lower-bound of suggestedEpoch. We cannot be certain that
//     sampled_epoch was supported at ts1, but we will ensure that it is
//     supported at ts2. EndTime > ts2
//
//   - SupportWithdrawn: A necessary condition is that epoch is populated.
//     Note that after computing the suggested_epoch we could go back and
//     compute this value, but we don't bother since the only use case for
//     suggesting an epoch is for a lease proposal, for which SupportWithdrawn
//     is no worse than CurrentlyUnsupported. Other necessary condition is
//     that the supported_epoch has advanced to a value > the epoch param.
//     However, the advancement could have happened after ts1. We are
//     guaranteed that ts2 has SupportWithdrawn. supported_epoch > epoch
//
//   - CurrentlyUnsupported: This necessarily requires an empty EndTime or an
//     EndTime in the past of ts2. supported_epoch <= epoch and (EndTime < ts1
//     or EndTime < ts2)
func (lss *localStoreState) computeSupport(
	ts1 hlc.ClockTimestamp,
	storeID dme_livenesspb.StoreIdentifier,
	epoch int64,
	suggestEpoch bool,
	replicaSets [][]dme_livenesspb.StoreIdentifier,
) (ts2 hlc.ClockTimestamp, ssi supportStatusInfo, err error) {
	if suggestEpoch {
		epoch = 0
	}
	computingSupportForSelf := storeID == lss.storeID
	var propagateSupportSoon bool
	for i := range replicaSets {
		replicas := replicaSets[i]
		var supports replicaSupportStatusInfos
		for _, r := range replicas {
			var support supportStatusInfo
			if storeID == r {
				suggestedEpoch := int64(0)
				// A store always supports itself. Even if the store restarts, there
				// is no self-support withdrawal. If support is being used for leases,
				// stepping down as leaseholder happens via a mechanism above
				// DME-liveness (e.g. the proscribed state for leases). Inside
				// DME-liveness, we cannot distinguish between an epoch increment at a
				// store due to a restart, versus a regular epoch increment.
				if computingSupportForSelf {
					if suggestEpoch {
						suggestedEpoch = lss.epoch.getEpoch()
					}
				} else {
					lss.mu.RLock()
					hbs, ok := lss.heartbeatAndSupportMap[r]
					lss.mu.RUnlock()
					if !ok {
						// Unknown store.
						return hlc.ClockTimestamp{}, supportStatusInfo{},
							errors.Errorf("cannot compute support for unknown store %+v", storeID)
					}
					if suggestEpoch {
						// We want to compute an epoch that is as close to the most recent
						// epoch that storeID is using. The best we can do is
						// hbs.maxSeenSupportEpoch.
						hbs.mu.RLock()
						suggestedEpoch = hbs.maxSeenSupportEpoch
						hbs.mu.RUnlock()
						if suggestedEpoch < 1 {
							suggestedEpoch = 1
						}
					}
				}
				support = supportStatusInfo{
					Supported, hlc.MaxTimestamp, suggestedEpoch}
			} else if r == lss.storeID {
				// The storeID seeking support is not self, but it has sought support
				// from a replica at self.
				lss.mu.RLock()
				hbs, ok := lss.heartbeatAndSupportMap[storeID]
				lss.mu.RUnlock()
				if !ok {
					// Unknown store.
					return hlc.ClockTimestamp{}, supportStatusInfo{},
						errors.Errorf("cannot compute support for unknown store %+v", storeID)
				}
				hbs.mu.RLock()
				supportProto := hbs.bySelfForStoreID
				hbs.mu.RUnlock()
				support = computeSupportStatusInfoFromSupport(
					ts1, epoch, suggestEpoch, supportProto)
				if !suggestEpoch && support.supportStatus == CurrentlyUnsupported &&
					supportProto.EndTime == (hlc.Timestamp{}) {
					// Bump up the epoch that one will support. That is, withdraw
					// support. This is one of the "information flowing down" cases.
					// Ensures that this support status transitions to SupportWithdrawn.
					func() {
						hbs.mu.Lock()
						defer hbs.mu.Unlock()
						if hbs.bySelfForStoreID.Epoch <= epoch {
							err = hbs.withdrawSupportLocked(epoch + 1)
						}
					}()
					if err != nil {
						return hlc.ClockTimestamp{}, supportStatusInfo{}, err
					}
					propagateSupportSoon = true
					// NB: This is happening in the future of ts1, but we will sample
					// ts2 later, so it is in the past of ts2. Therefore, we can claim
					// to have withdrawn support.
					support.supportStatus = SupportWithdrawn
				}
			} else {
				// A replica that is not the one seeking support, and the replica is
				// not self.
				if computingSupportForSelf {
					lss.mu.RLock()
					hbs, ok := lss.heartbeatAndSupportMap[r]
					lss.mu.RUnlock()
					if !ok {
						return hlc.ClockTimestamp{}, supportStatusInfo{},
							errors.Errorf("cannot compute support by unknown store %+v", r)
					}
					hbs.mu.RLock()
					supportProto := hbs.forSelfByStoreID
					hbs.mu.RUnlock()
					support = computeSupportStatusInfoFromSupport(
						ts1, epoch, suggestEpoch, supportProto)
				} else {
					// Both storeID and the replica are not self. The best we can do is
					// look at the information propagated to us.
					lss.mu.RLock()
					hbsBy, byOk := lss.heartbeatAndSupportMap[r]
					hbsFor, forOk := lss.heartbeatAndSupportMap[storeID]
					lss.mu.RUnlock()
					if !byOk {
						return hlc.ClockTimestamp{}, supportStatusInfo{},
							errors.Errorf("cannot compute support by unknown store %+v", r)
					}
					if !forOk {
						return hlc.ClockTimestamp{}, supportStatusInfo{},
							errors.Errorf("cannot compute support for unknown store %+v", storeID)
					}
					// If not in the map, we will get an empty proto, which will compute
					// as CurrentlyUnsupported.
					hbsBy.mu.RLock()
					supportProto := hbsBy.byStoreIDFor[storeID]
					hbsBy.mu.RUnlock()
					support = computeSupportStatusInfoFromSupport(ts1, epoch, suggestEpoch, supportProto)
					hbsFor.mu.RLock()
					supportProto = hbsFor.forStoreIDBy[r]
					hbsFor.mu.RUnlock()
					support.Or(computeSupportStatusInfoFromSupport(ts1, epoch, suggestEpoch, supportProto))
				}
			}
			supportWithSupporter := supportStatusInfoWithSupporter{by: r, supportStatusInfo: support}
			supports = append(supports, supportWithSupporter)
		}
		ssiForSet := supports.aggregateStatus()
		if ssiForSet.supportStatus == CurrentlyUnsupported && computingSupportForSelf && !suggestEpoch {
			n := len(supports)
			for k := n - 1; k >= 0; k-- {
				if supports[k].supportStatus == CurrentlyUnsupported {
					lss.mu.RLock()
					hbs, ok := lss.heartbeatAndSupportMap[supports[k].by]
					lss.mu.RUnlock()
					if !ok {
						panic(errors.AssertionFailedf("expected to find %+v in map", supports[k].by))
					}
					func() {
						hbs.mu.Lock()
						defer hbs.mu.Unlock()
						if hbs.forSelfByStoreID.Epoch <= epoch {
							// Bump up the epoch. This is one of the "information flowing
							// down" cases. By bumping up, we ensure that the partial network
							// partition is addressed. We could wait until the final ssi value
							// is CurrentlyUnsupported, since this is unnecessary if some
							// replica set has ensured SupportWithdrawn.
							if err := lss.epoch.incrementEpoch(lss.StorageProvider); err != nil {
								panic(err)
							}
							hbs.forSelfByStoreID.Epoch = lss.epoch.getEpoch()
							hbs.forSelfByStoreID.EndTime = hlc.Timestamp{}
							propagateSupportSoon = true
						}
					}()
				} else {
					break
				}
			}
		}
		if propagateSupportSoon {
			lss.MessageSender.PropagateLocalSupportStateSoon()
		}
		if i == 0 {
			ssi = ssiForSet
		} else {
			ssi.And(ssiForSet)
		}
	}
	ts2 = lss.parent.o.Clock.NowAsClockTimestamp()
	if ssi.supportStatus == Supported && !(hlc.Timestamp(ts2).Less(ssi.endTime)) {
		ssi = supportStatusInfo{CurrentlyUnsupported, hlc.Timestamp{}, 0}
	}
	return ts2, ssi, nil
}

type supportStatusInfo struct {
	supportStatus  SupportStatus
	endTime        hlc.Timestamp
	suggestedEpoch int64
}

type supportStatusInfoWithSupporter struct {
	by dme_livenesspb.StoreIdentifier
	supportStatusInfo
}

func computeSupportStatusInfoFromSupport(
	ts1 hlc.ClockTimestamp, epoch int64, suggestEpoch bool, support dme_livenesspb.Support,
) supportStatusInfo {
	if !suggestEpoch && support.Epoch > epoch {
		return supportStatusInfo{SupportWithdrawn, hlc.Timestamp{}, 0}
	}
	timeIsSupported := hlc.Timestamp(ts1).Less(support.EndTime)
	// suggestEpoch || support.Epoch <= epoch.
	if timeIsSupported {
		if suggestEpoch {
			return supportStatusInfo{Supported, support.EndTime, support.Epoch}
		}
		return supportStatusInfo{Supported, support.EndTime, 0}
	}
	return supportStatusInfo{CurrentlyUnsupported, hlc.Timestamp{}, 0}
}

// Once we have the supportStatusInfo for all replicas in a set, we need to
// combine them. If have a quorum of Supported, can say Supported. Need to
// pick the min support time across them. So Supported first, sorted with
// decreasing end time. If don't have a quorum do we have a quorum of
// SupportWithdrawn. Sort that next.
func cmpForComputingSupportQuorum(
	a supportStatusInfoWithSupporter, b supportStatusInfoWithSupporter,
) int {
	if a.supportStatus != b.supportStatus {
		if b.supportStatus < a.supportStatus {
			return -1
		}
		return +1
	}
	if a.supportStatus == Supported {
		return b.endTime.Compare(a.endTime)
	}
	return 0
}

type replicaSupportStatusInfos []supportStatusInfoWithSupporter

// Need self-support to also have a suggestedEpoch, so can't do
func (rssi replicaSupportStatusInfos) aggregateStatus() supportStatusInfo {
	quorumCount := (len(rssi) + 2) / 2
	slices.SortFunc(rssi, cmpForComputingSupportQuorum)
	if rssi[quorumCount-1].supportStatus == Supported {
		// Have quorum of support.
		i := 0
		var suggestedEpoch int64
		for ; i < quorumCount; i++ {
			if rssi[i].suggestedEpoch > suggestedEpoch {
				suggestedEpoch = rssi[i].suggestedEpoch
			}
		}
		for ; i < len(rssi); i++ {
			if rssi[i].supportStatus != Supported {
				break
			}
			if rssi[i].suggestedEpoch > suggestedEpoch {
				suggestedEpoch = rssi[i].suggestedEpoch
			}
		}
		// NB: don't use suggestedEpoch using those that have SupportWithdrawn
		// since they may have incremented to an epoch one higher than what the
		// support requesting store is using.
		expiration := rssi[quorumCount-1].endTime
		return supportStatusInfo{Supported, expiration, suggestedEpoch}
	}
	countWithdrawn := 0
	for i := range rssi {
		if rssi[i].supportStatus == Supported {
			continue
		}
		if rssi[i].supportStatus == CurrentlyUnsupported {
			break
		}
		countWithdrawn++
	}
	if countWithdrawn > len(rssi)-quorumCount {
		return supportStatusInfo{SupportWithdrawn, hlc.Timestamp{}, 0}
	}
	return supportStatusInfo{CurrentlyUnsupported, hlc.Timestamp{}, 0}
}

// Or is called with infos computed for the same supporting store.
// SupportWithdrawn trumps everything. If there is no SupportWithdrawn, then
// Supported wins over CurrentlyUnsupported.
func (ssi *supportStatusInfo) Or(other supportStatusInfo) {
	if ssi.supportStatus == SupportWithdrawn {
		return
	}
	if other.supportStatus == SupportWithdrawn {
		*ssi = supportStatusInfo{SupportWithdrawn, hlc.Timestamp{}, 0}
		return
	}
	// Both are not SupportWithdrawn.
	if other.supportStatus == CurrentlyUnsupported {
		return
	}
	// other is Supported.
	if ssi.endTime.Less(other.endTime) {
		*ssi = supportStatusInfo{Supported, other.endTime, other.suggestedEpoch}
		return
	}
}

// And is called with infos computed for different replica sets.
// SupportWithdrawn trumps everything. If there is no SupportWithdrawn,
// CurrentlyUnsupported wins over Supported.
func (ssi *supportStatusInfo) And(other supportStatusInfo) {
	if ssi.supportStatus == SupportWithdrawn {
		return
	}
	if other.supportStatus == SupportWithdrawn {
		*ssi = supportStatusInfo{SupportWithdrawn, hlc.Timestamp{}, 0}
		return
	}
	// Both are not SupportWithdrawn.
	if ssi.supportStatus == CurrentlyUnsupported {
		return
	}
	// ssi is Supported.
	if other.supportStatus == CurrentlyUnsupported {
		*ssi = supportStatusInfo{CurrentlyUnsupported, hlc.Timestamp{}, 0}
	}
	// Both are Supported.
	if other.endTime.Less(ssi.endTime) {
		ssi.endTime = other.endTime
	}
	if ssi.suggestedEpoch < other.suggestedEpoch {
		ssi.suggestedEpoch = other.suggestedEpoch
	}
}

type heartbeatAndSupport struct {
	storeID dme_livenesspb.StoreIdentifier
	parent  *localStoreState

	mu                   syncutil.RWMutex
	forSelfByStoreID     dme_livenesspb.Support
	heartbeatTimerHandle callbackSchedulerHandle

	bySelfForStoreID             dme_livenesspb.Support
	supportWithdrawalTimerHandle callbackSchedulerHandle

	// maxSeenSupportEpoch is the highest epoch self has heard about regarding
	// storeID. This could have come from a forStoreIDBy, bySelfForStoreID, or
	// in some other heartbeatAndSupport's byStoreIDFor. This is at most one
	// higher than an epoch that storeID has reached. This is used to
	// fast-forward, when support is withdrawn.
	maxSeenSupportEpoch int64
	// stateTime is the timestamp in the SupportState message that contained the
	// following maps. Used to ignore stale messages.
	//
	// TODO(sumeer): when there are multiple local stores, this information is
	// being duplicated across the various localStoreStates. Since it is not
	// persistent, we could maintain it once in managerImpl,
	stateTime    hlc.ClockTimestamp
	forStoreIDBy map[dme_livenesspb.StoreIdentifier]dme_livenesspb.Support
	byStoreIDFor map[dme_livenesspb.StoreIdentifier]dme_livenesspb.Support

	closeCh chan struct{}
}

// Only creates the struct.
func newHeartbeatAndSupportStruct(
	storeID dme_livenesspb.StoreIdentifier, epoch int64, bySelfForStoreID dme_livenesspb.Support,
) *heartbeatAndSupport {
	return &heartbeatAndSupport{
		storeID: storeID,
		forSelfByStoreID: dme_livenesspb.Support{
			Epoch: epoch,
		},
		bySelfForStoreID:    bySelfForStoreID,
		maxSeenSupportEpoch: bySelfForStoreID.Epoch,
		forStoreIDBy:        map[dme_livenesspb.StoreIdentifier]dme_livenesspb.Support{},
		byStoreIDFor:        map[dme_livenesspb.StoreIdentifier]dme_livenesspb.Support{},
		closeCh:             make(chan struct{}),
	}
}

func (hbs *heartbeatAndSupport) start(parent *localStoreState) {
	hbs.parent = parent
	hbs.heartbeatTimerHandle = hbs.parent.parent.cbs.registerCallback(
		fmt.Sprintf("dme_liveness.heartbeat %+v", hbs.storeID), func() {
			hbs.mu.RLock()
			epoch := hbs.forSelfByStoreID.Epoch
			hbs.mu.RUnlock()
			log.Infof(context.Background(), "heartbeat: in dme_liveness.heartbeat for %+v", hbs.storeID)
			hbs.sendHeartbeat(epoch)
		})
	hbs.supportWithdrawalTimerHandle = hbs.parent.parent.cbs.registerCallback(
		fmt.Sprintf("dme_liveness.support %+v", hbs.storeID), hbs.supportTimerExpiry)
	{
		hbs.mu.RLock()
		epoch := hbs.forSelfByStoreID.Epoch
		hbs.mu.RUnlock()
		log.Infof(context.Background(), "heartbeat: start for %+v", hbs.storeID)
		hbs.sendHeartbeat(epoch)
	}
	if hbs.bySelfForStoreID.EndTime != (hlc.Timestamp{}) {
		// Give some grace period, by pretending that the heartbeat succeeded at
		// now.
		dur := hbs.parent.parent.o.LivenessExpiryInterval()
		hbs.supportWithdrawalTimerHandle.runCallbackAfterDuration(dur)
	}
}

func (hbs *heartbeatAndSupport) close() {
	hbs.heartbeatTimerHandle.unregister()
	hbs.supportWithdrawalTimerHandle.unregister()
	close(hbs.closeCh)
}

func (hbs *heartbeatAndSupport) supportTimerExpiry() {
	withdrewSupport, registerCallbackDuration := func() (bool, time.Duration) {
		hbs.mu.Lock()
		defer hbs.mu.Unlock()
		if hbs.bySelfForStoreID.EndTime == (hlc.Timestamp{}) {
			// Not supporting. Ignore.
			return false, 0
		}
		now := hbs.parent.parent.o.Clock.PhysicalNow()
		if now >= hbs.bySelfForStoreID.EndTime.WallTime {
			// Expired. Withdraw support.
			err := hbs.withdrawSupportLocked(hbs.bySelfForStoreID.Epoch + 1)
			if err != nil {
				// TODO(sumeer): retry N times?
				panic(err)
			}
			return true, 0
		} else {
			dur := hbs.bySelfForStoreID.EndTime.WallTime - now
			return false, time.Duration(dur) + time.Millisecond
		}
	}()
	if withdrewSupport {
		hbs.parent.MessageSender.PropagateLocalSupportStateSoon()
	}
	if registerCallbackDuration > 0 {
		hbs.supportWithdrawalTimerHandle.runCallbackAfterDuration(registerCallbackDuration)
	}
}

// Method does not read any mutex protected state. And mutex should not be
// held when calling this method.
func (hbs *heartbeatAndSupport) sendHeartbeat(epoch int64) {
	now := hbs.parent.parent.o.Clock.NowAsClockTimestamp()
	dur := hbs.parent.parent.o.LivenessExpiryInterval()
	endTime := hlc.Timestamp(now).AddDuration(dur)
	hbs.parent.MessageSender.SendHeartbeat(
		dme_livenesspb.Header{
			From: hbs.parent.storeID,
			To:   hbs.storeID,
		},
		dme_livenesspb.Heartbeat{
			Epoch:      epoch,
			SenderTime: now,
			EndTime:    endTime,
		})
	// Divide by 2 is a temporary hack.
	log.Infof(context.Background(), "heartbeat: sendHeartbeat for %+v, reset %s",
		hbs.storeID, (dur / 2).String())
	hbs.heartbeatTimerHandle.runCallbackAfterDuration(dur / 2)
	hbs.parent.parent.metrics.HeartbeatSent.Inc(1)
}

func (hbs *heartbeatAndSupport) withdrawSupportLocked(nextEpoch int64) error {
	if nextEpoch > hbs.maxSeenSupportEpoch {
		hbs.maxSeenSupportEpoch = nextEpoch
	} else {
		nextEpoch = hbs.maxSeenSupportEpoch
	}
	support := dme_livenesspb.Support{
		Epoch: nextEpoch,
	}
	err := hbs.parent.StorageProvider.UpdateSupportBySelfFor(hbs.storeID, support)
	if err != nil {
		return err
	}
	hbs.bySelfForStoreID = support
	return nil
}

func (hbs *heartbeatAndSupport) handleHeartbeat(
	msg dme_livenesspb.Heartbeat,
) (ack bool, err error) {
	hbs.parent.parent.metrics.HeartbeatRecv.Inc(1)
	err = hbs.parent.parent.o.Clock.UpdateAndCheckMaxOffset(context.Background(), msg.SenderTime)
	if err != nil {
		return false, err
	}
	var propagateSupportSoon bool
	ack, err = func() (ack bool, err error) {
		hbs.mu.Lock()
		defer hbs.mu.Unlock()
		if msg.Epoch < hbs.bySelfForStoreID.Epoch {
			// Nack.
			return false, nil
		}
		if msg.Epoch == hbs.bySelfForStoreID.Epoch && msg.EndTime.LessEq(hbs.bySelfForStoreID.EndTime) {
			// Stale message and already have supported beyond the end time.
			return true, nil
		}
		// msg.Epoch > hbs.bySelfForStoreID.Epoch or msg.EndTime >
		// hbs.bySelfForStoreID.EndTime NB: In the case of where the first predicate
		// is true, we may be regressing hbs.bySelfForStoreID.EndTime, in that we
		// have supported a lower epoch until later than msg.EndTime. This is
		// desirable: The storeID has unilaterally decided to stop using
		// hbs.bySelfForStoreID.Epoch at at time earlier than what we were willing
		// to support it.
		support := dme_livenesspb.Support{
			Epoch:   msg.Epoch,
			EndTime: msg.EndTime,
		}
		propagateSupportSoon = msg.Epoch > hbs.bySelfForStoreID.Epoch
		err = hbs.parent.StorageProvider.UpdateSupportBySelfFor(hbs.storeID, support)
		if err != nil {
			return false, err
		}
		hbs.bySelfForStoreID = support
		dur := msg.EndTime.WallTime -
			hbs.parent.parent.o.Clock.PhysicalNow() + 1
		if dur <= 0 {
			hbs.supportWithdrawalTimerHandle.runCallbackAfterDuration(time.Duration(math.MaxInt64))
			err := hbs.withdrawSupportLocked(hbs.bySelfForStoreID.Epoch + 1)
			if err != nil {
				panic(err)
			}
			propagateSupportSoon = true
			return false, nil
		}
		hbs.supportWithdrawalTimerHandle.runCallbackAfterDuration(time.Duration(dur))
		return true, nil
	}()
	if propagateSupportSoon {
		hbs.parent.MessageSender.PropagateLocalSupportStateSoon()
	}
	return ack, err
}

func (hbs *heartbeatAndSupport) handleHeartbeatResponse(
	msg dme_livenesspb.Heartbeat, responderTime hlc.ClockTimestamp, ack bool,
) error {
	if ack {
		hbs.parent.parent.metrics.HeartbeatResponseAck.Inc(1)
	} else {
		hbs.parent.parent.metrics.HeartbeatResponseNack.Inc(1)
	}
	err := hbs.parent.parent.o.Clock.UpdateAndCheckMaxOffset(context.Background(), responderTime)
	if err != nil {
		return err
	}
	var epoch int64
	var sendHeartbeat bool
	func() {
		hbs.mu.Lock()
		defer hbs.mu.Unlock()
		if hbs.forSelfByStoreID.Epoch < msg.Epoch {
			panic("remote support is supporting responding to a heartbeat this store did not send")
		}
		if ack {
			if hbs.forSelfByStoreID.Epoch == msg.Epoch && hbs.forSelfByStoreID.EndTime.LessEq(msg.EndTime) {
				hbs.forSelfByStoreID.EndTime = msg.EndTime
			}
			// Else late ack. Ignore.
			return
		}
		// Nack.
		if hbs.forSelfByStoreID.Epoch > msg.Epoch {
			// Late nack. Ignore
			return
		}
		// INVARIANT: hbs.forSelfByStoreID.Epoch == msg.Epoch.
		if err := hbs.parent.epoch.incrementEpoch(hbs.parent.StorageProvider); err != nil {
			panic(err)
		}
		hbs.forSelfByStoreID.Epoch = hbs.parent.epoch.getEpoch()
		hbs.forSelfByStoreID.EndTime = hlc.Timestamp{}
		epoch = hbs.forSelfByStoreID.Epoch
		sendHeartbeat = true
	}()
	if sendHeartbeat {
		log.Infof(context.Background(), "heartbeat: hhr for %+v", hbs.storeID)
		hbs.sendHeartbeat(epoch)
		hbs.parent.MessageSender.PropagateLocalSupportStateSoon()
	}
	return nil
}

func (hbs *heartbeatAndSupport) handleSupportState(msg dme_livenesspb.SupportState) error {
	hbs.parent.parent.metrics.SupportStateRecv.Inc(1)
	err := hbs.parent.parent.o.Clock.UpdateAndCheckMaxOffset(context.Background(), msg.StateTime)
	if err != nil {
		return err
	}
	var propagateSupportSoon bool
	err = func() error {
		hbs.mu.Lock()
		defer hbs.mu.Unlock()
		if msg.StateTime.LessEq(hbs.stateTime) {
			return nil
		}
		hbs.stateTime = msg.StateTime
		for k := range hbs.forStoreIDBy {
			delete(hbs.forStoreIDBy, k)
		}
		for k := range hbs.byStoreIDFor {
			delete(hbs.byStoreIDFor, k)
		}
		if msg.ForOtherBy.For != hbs.storeID {
			panic("routed incorrectly")
		}
		if msg.ByOtherFor.By != hbs.storeID {
			panic("routed incorrectly")
		}
		for _, s := range msg.ForOtherBy.Support {
			hbs.forStoreIDBy[s.By] = s.Support
			if hbs.maxSeenSupportEpoch < s.Support.Epoch {
				hbs.maxSeenSupportEpoch = s.Support.Epoch
			}
		}
		if hbs.bySelfForStoreID.EndTime == (hlc.Timestamp{}) &&
			hbs.maxSeenSupportEpoch > hbs.bySelfForStoreID.Epoch {
			err := hbs.withdrawSupportLocked(hbs.maxSeenSupportEpoch)
			if err != nil {
				panic(err)
			}
			propagateSupportSoon = true
		}
		for _, s := range msg.ByOtherFor.Support {
			hbs.byStoreIDFor[s.For] = s.Support
			hbs2, ok := hbs.parent.heartbeatAndSupportMap[s.For]
			if ok {
				if hbs2.maxSeenSupportEpoch < s.Support.Epoch {
					hbs2.maxSeenSupportEpoch = s.Support.Epoch
				}
				if hbs2.bySelfForStoreID.EndTime == (hlc.Timestamp{}) &&
					hbs2.maxSeenSupportEpoch > hbs2.bySelfForStoreID.Epoch {
					err := hbs2.withdrawSupportLocked(hbs2.maxSeenSupportEpoch)
					if err != nil {
						panic(err)
					}
					propagateSupportSoon = true
				}
			}
		}
		return nil
	}()
	if propagateSupportSoon {
		hbs.parent.MessageSender.PropagateLocalSupportStateSoon()
	}
	return err
}

type managerImpl struct {
	o       Options
	stopper *stop.Stopper
	cbs     callbackScheduler
	mu      syncutil.RWMutex
	// Protected by mu.
	localStores map[dme_livenesspb.StoreIdentifier]*localStoreState

	metrics *dmeMetrics
}

func NewManager(o Options) Manager {
	localStores := map[dme_livenesspb.StoreIdentifier]*localStoreState{}
	m := &managerImpl{
		o:           o,
		stopper:     stop.NewStopper(),
		cbs:         o.callbackSchedulerForTesting,
		localStores: localStores,
		metrics:     newDMEMetrics(o.MetricRegistry),
	}
	if m.cbs == nil {
		m.cbs = &timerCallbackScheduler{stopper: m.stopper}
	}
	return m
}

func (m *managerImpl) AddLocalStore(store LocalStore) error {
	lss, err := newLocalStoreState(store, m)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.localStores[lss.storeID] = lss
	return nil
}

func (m *managerImpl) AddRemoteStore(storeID dme_livenesspb.StoreIdentifier) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, lss := range m.localStores {
		lss.addRemoteStore(storeID)
	}
}

func (m *managerImpl) RemoveRemoteStore(storeID dme_livenesspb.StoreIdentifier) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, lss := range m.localStores {
		lss.removeRemoteStore(storeID)
	}
}

func (m *managerImpl) HandleHeartbeat(
	header dme_livenesspb.Header, msg dme_livenesspb.Heartbeat,
) (ack bool, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	lss, ok := m.localStores[header.To]
	if !ok {
		// Testing hack
		panic(errors.Errorf("store s%s not found", header.To.StoreID.String()))
		// return false, errors.Errorf("store s%s not found", header.To.StoreID.String())
	}
	return lss.handleHeartbeat(header.From, msg)
}

func (m *managerImpl) HandleSupportState(
	header dme_livenesspb.Header, msg dme_livenesspb.SupportState,
) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	lss, ok := m.localStores[header.To]
	if !ok {
		// Testing hack
		panic(errors.Errorf("store s%s not found", header.To.StoreID.String()))
		// return errors.Errorf("store s%s not found", header.To.StoreID.String())
	}
	return lss.handleSupportState(header.From, msg)
}

func (m *managerImpl) EpochAndSupportForLeaseProposal(
	start hlc.ClockTimestamp,
	storeID dme_livenesspb.StoreIdentifier,
	descriptor roachpb.RangeDescriptor,
) (adjustedStart hlc.ClockTimestamp, epoch int64, err error) {
	replicaSets := descriptorToReplicaSets(descriptor)
	lss := m.localStoreInReplicas(replicaSets)
	if lss == nil {
		panic(errors.AssertionFailedf(
			"calling EpochAndSupportForLeaseProposal but none of the local stores are a replica"))
	}
	ts2, ssi, err := lss.computeSupport(start, storeID, 0, true, replicaSets)
	if err != nil {
		return hlc.ClockTimestamp{}, 0, err
	}
	m.updateComputeSupportMetric(ssi.supportStatus)
	if ssi.supportStatus != Supported {
		return hlc.ClockTimestamp{}, 0, errors.Errorf(
			"cannot construct lease proposal since not Supported %+v", ssi)
	}
	return ts2, ssi.suggestedEpoch, nil
}

func (m *managerImpl) updateComputeSupportMetric(ss SupportStatus) {
	switch ss {
	case CurrentlyUnsupported:
		m.metrics.ComputeSupportCurrentlyUnsupported.Inc(1)
	case SupportWithdrawn:
		m.metrics.ComputeSupportWithdrawn.Inc(1)
	case Supported:
		m.metrics.ComputeSupportSupported.Inc(1)
	}
}
func (m *managerImpl) HasSupport(
	now hlc.ClockTimestamp,
	storeID dme_livenesspb.StoreIdentifier,
	epoch int64,
	descriptors []roachpb.RangeDescriptor,
) (support SupportStatus, expiration hlc.Timestamp, err error) {
	replicaSets := descriptorToReplicaSets(descriptors[0])
	for i := 1; i < len(descriptors); i++ {
		replicaSets = append(replicaSets, descriptorToReplicaSets(descriptors[i])...)
	}
	lss := m.localStoreInReplicas(replicaSets)
	if lss == nil {
		var b strings.Builder
		for i := range descriptors {
			fmt.Fprintf(&b, " desc %d: %s", i, descriptors[i].String())
		}
		panic(errors.AssertionFailedf(
			"calling HasSupport but none of the local stores are a replica%s", b.String()))
	}
	_, ssi, err := lss.computeSupport(now, storeID, epoch, false, replicaSets)
	m.updateComputeSupportMetric(ssi.supportStatus)
	return ssi.supportStatus, ssi.endTime, err
}

func (m *managerImpl) localStoreInReplicas(
	replicaSets [][]dme_livenesspb.StoreIdentifier,
) *localStoreState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var lss *localStoreState
	for i := range replicaSets {
		for j := range replicaSets[i] {
			var ok bool
			lss, ok = m.localStores[replicaSets[i][j]]
			if ok {
				break
			}
		}
		if lss != nil {
			break
		}
	}
	return lss
}

func (m *managerImpl) Close() {
	m.stopper.Stop(context.Background())
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, lss := range m.localStores {
		lss.close()
	}
}

func descriptorToReplicaSets(
	descriptor roachpb.RangeDescriptor,
) (replicaSets [][]dme_livenesspb.StoreIdentifier) {
	setCount := 1
	for _, r := range descriptor.InternalReplicas {
		isOld := r.IsVoterOldConfig()
		isNew := r.IsVoterNewConfig()
		if !isOld && !isNew {
			continue
		}
		if !isOld && isNew {
			setCount++
			break
		}
	}
	for len(replicaSets) < setCount {
		replicaSets = append(replicaSets, []dme_livenesspb.StoreIdentifier{})
	}
	for _, r := range descriptor.InternalReplicas {
		isOld := r.IsVoterOldConfig()
		isNew := r.IsVoterNewConfig()
		if !isOld && !isNew {
			continue
		}
		storeID := dme_livenesspb.StoreIdentifier{
			NodeID:  r.NodeID,
			StoreID: r.StoreID,
		}
		if isOld {
			replicaSets[0] = append(replicaSets[0], storeID)
		}
		if isNew && setCount == 2 {
			replicaSets[1] = append(replicaSets[1], storeID)
		}
	}
	return replicaSets
}

type StorageProviderForDMELiveness struct {
	AmbientCtx log.AmbientContext
	Engine     storage.Engine
}

var _ StorageProvider = &StorageProviderForDMELiveness{}

func (s *StorageProviderForDMELiveness) ReadCurrentEpoch() (int64, error) {
	ctx := s.AmbientCtx.AnnotateCtx(context.Background())
	var epochProto dme_livenesspb.Epoch
	ok, err := storage.MVCCGetProto(ctx, s.Engine, keys.StoreDMEEpochKey(), hlc.Timestamp{},
		&epochProto, storage.MVCCGetOptions{})
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	return epochProto.Epoch, nil
}

func (s *StorageProviderForDMELiveness) UpdateCurrentEpoch(epoch int64) error {
	ctx := s.AmbientCtx.AnnotateCtx(context.Background())
	epochProto := dme_livenesspb.Epoch{Epoch: epoch}
	return storage.MVCCPutProto(
		ctx,
		s.Engine,
		keys.StoreDMEEpochKey(),
		hlc.Timestamp{},
		&epochProto,
		storage.MVCCWriteOptions{},
	)
}

func (s *StorageProviderForDMELiveness) ReadSupportBySelfFor(
	f func(
		store dme_livenesspb.StoreIdentifier, support dme_livenesspb.Support),
) error {
	ctx := s.AmbientCtx.AnnotateCtx(context.Background())
	minKey := keys.StoreDMESupportBySelfKey(0, 0)
	maxKey := keys.StoreDMESupportBySelfKey(math.MaxInt32, math.MaxInt32)
	reader := s.Engine.NewReadOnly(storage.StandardDurability)
	defer reader.Close()
	_, err := storage.MVCCIterate(ctx, reader, minKey, maxKey, hlc.Timestamp{}, storage.MVCCScanOptions{},
		func(kv roachpb.KeyValue) error {
			var support dme_livenesspb.Support
			nodeID, storeID, err := keys.DecodeStoreDMESupportBySelfKey(kv.Key)
			if err != nil {
				return err
			}
			if err := kv.Value.GetProto(&support); err != nil {
				return err
			}
			f(dme_livenesspb.StoreIdentifier{NodeID: nodeID, StoreID: storeID}, support)
			return nil
		})
	return err
}

func (s *StorageProviderForDMELiveness) UpdateSupportBySelfFor(
	store dme_livenesspb.StoreIdentifier, support dme_livenesspb.Support,
) error {
	ctx := s.AmbientCtx.AnnotateCtx(context.Background())
	key := keys.StoreDMESupportBySelfKey(store.NodeID, store.StoreID)
	return storage.MVCCPutProto(
		ctx,
		s.Engine,
		key,
		hlc.Timestamp{},
		&support,
		storage.MVCCWriteOptions{},
	)
}

func (s *StorageProviderForDMELiveness) StateForTesting() string {
	var b strings.Builder
	epoch, err := s.ReadCurrentEpoch()
	if err != nil {
		return err.Error()
	}
	fmt.Fprintf(&b, "epoch: %d\n", epoch)
	fmt.Fprintf(&b, "supports:\n")
	err = s.ReadSupportBySelfFor(func(store dme_livenesspb.StoreIdentifier, support dme_livenesspb.Support) {
		fmt.Fprintf(&b, " %+v: %+v\n", store, support)
	})
	if err != nil {
		fmt.Fprintf(&b, " error: %s\n", err.Error())
	}
	return b.String()
}

// callbackScheduler abstracts timer based callbacks for unit testing. The
// caller of registerCallback does not control the goroutine on which the
// callback happens, which allows a test implementation to do these calls
// synchronously from the test goroutine (which avoids the need to sleep to
// get deterministic test output).
type callbackScheduler interface {
	registerCallback(debugName string, f func()) callbackSchedulerHandle
}

type callbackSchedulerHandle interface {
	runCallbackAfterDuration(d time.Duration)
	unregister()
}

type timerCallbackScheduler struct {
	stopper *stop.Stopper
}

func (tcs *timerCallbackScheduler) registerCallback(
	debugName string, f func(),
) callbackSchedulerHandle {
	h := &timerCallbackSchedulerHandle{
		unregisterCh: make(chan struct{}),
	}
	// Hack: if we don't reset the timer, the h.timer.C is a nil channel that
	// blocks forever.
	h.timer.Reset(365 * 24 * time.Hour)
	err := tcs.stopper.RunAsyncTask(context.Background(), debugName, func(_ context.Context) {
		for {
			select {
			case <-h.timer.C:
				h.timer.Read = true
				log.Infof(context.Background(), "%s pre f", debugName)
				f()
				log.Infof(context.Background(), "%s post f", debugName)
			case <-tcs.stopper.ShouldQuiesce():
				return
			case <-h.unregisterCh:
				return
			}
		}
	})
	if err != nil {
		panic(err)
	}
	return h
}

type timerCallbackSchedulerHandle struct {
	timer        timeutil.Timer
	unregisterCh chan struct{}
}

func (h *timerCallbackSchedulerHandle) runCallbackAfterDuration(d time.Duration) {
	h.timer.Reset(d)
}

func (h *timerCallbackSchedulerHandle) unregister() {
	h.timer.Stop()
	close(h.unregisterCh)
}

// TODO(sumeer): these should be per local store. We should hook these up via
// StoreMetrics, but this hack is ok for the prototype.

type dmeMetrics struct {
	HeartbeatSent                      *metric.Counter
	HeartbeatResponseAck               *metric.Counter
	HeartbeatResponseNack              *metric.Counter
	HeartbeatRecv                      *metric.Counter
	SupportStateSent                   *metric.Counter
	SupportStateRecv                   *metric.Counter
	ComputeSupportSupported            *metric.Counter
	ComputeSupportWithdrawn            *metric.Counter
	ComputeSupportCurrentlyUnsupported *metric.Counter
}

var (
	heartbeatSentMeta = metric.Metadata{
		Name:        "dme.heartbeat.sent",
		Help:        "Number of heartbeats sent",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	heartbeatResponseAckMeta = metric.Metadata{
		Name:        "dme.heartbeat.response.ack",
		Help:        "Number of heartbeats responses received that were acks",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	heartbeatResponseNackMeta = metric.Metadata{
		Name:        "dme.heartbeat.response.nack",
		Help:        "Number of heartbeats responses received that were nacks",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	heartbeatRecvtMeta = metric.Metadata{
		Name:        "dme.heartbeat.recv",
		Help:        "Number of heartbeats received",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	supportStateSentMeta = metric.Metadata{
		Name:        "dme.support-state.sent",
		Help:        "Number of support-states sent",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	supportStateRecvMeta = metric.Metadata{
		Name:        "dme.support-state.recv",
		Help:        "Number of support-states received",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	computeSupportSupportedMeta = metric.Metadata{
		Name:        "dme.compute-support.supported",
		Help:        "Number of compute-support calls that supported",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	computeSupportWithdrawnMeta = metric.Metadata{
		Name:        "dme.compute-support.withdrawn",
		Help:        "Number of compute-support calls that computed withdrawn",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	computeSupportCurrentlyUnsupportedMeta = metric.Metadata{
		Name:        "dme.compute-support.currently-unsupported",
		Help:        "Number of compute-support calls that currently-unsupported",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
)

func newDMEMetrics(reg *metric.Registry) *dmeMetrics {
	m := &dmeMetrics{
		HeartbeatSent:                      metric.NewCounter(heartbeatSentMeta),
		HeartbeatResponseAck:               metric.NewCounter(heartbeatResponseAckMeta),
		HeartbeatResponseNack:              metric.NewCounter(heartbeatResponseNackMeta),
		HeartbeatRecv:                      metric.NewCounter(heartbeatRecvtMeta),
		SupportStateSent:                   metric.NewCounter(supportStateSentMeta),
		SupportStateRecv:                   metric.NewCounter(supportStateRecvMeta),
		ComputeSupportSupported:            metric.NewCounter(computeSupportSupportedMeta),
		ComputeSupportWithdrawn:            metric.NewCounter(computeSupportWithdrawnMeta),
		ComputeSupportCurrentlyUnsupported: metric.NewCounter(computeSupportCurrentlyUnsupportedMeta),
	}
	log.Infof(context.Background(), "registering DME metrics")
	reg.AddMetricStruct(m)
	return m
}

func (*dmeMetrics) MetricStruct() {}
