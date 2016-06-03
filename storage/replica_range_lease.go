// Copyright 2016 The Cockroach Authors.
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
// Author: Andrei Matei (andreimatei1@gmail.com)
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

// This file contains replica methods related to LeaderLeases.

package storage

import (
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/protoutil"
	"golang.org/x/net/context"
)

// requestLeaderLease sends a request to obtain or extend a leader
// lease for this replica. Unless an error is returned, the obtained
// lease will be valid for a time interval containing the requested
// timestamp. Only a single lease request may be pending at a time.
func (r *Replica) requestLeaderLease(timestamp hlc.Timestamp) <-chan *roachpb.Error {
	r.mu.Lock()
	defer r.mu.Unlock()

	llChan := make(chan *roachpb.Error, 1)
	if len(r.mu.llChans) > 0 {
		r.mu.llChans = append(r.mu.llChans, llChan)
		return llChan
	}
	if r.store.IsDrainingLeadership() || !r.store.Stopper().RunAsyncTask(func() {
		pErr := func() *roachpb.Error {
			// TODO(tschottdorf): get duration from configuration, either as a
			// config flag or, later, dynamically adjusted.
			startStasis := timestamp.Add(int64(r.store.ctx.leaderLeaseActiveDuration), 0)
			expiration := startStasis.Add(int64(r.store.Clock().MaxOffset()), 0)

			// Prepare a Raft command to get a leader lease for this replica.
			desc := r.Desc()
			_, replica := desc.FindReplica(r.store.StoreID())
			if replica == nil {
				return roachpb.NewError(roachpb.NewRangeNotFoundError(r.RangeID))
			}
			args := &roachpb.LeaderLeaseRequest{
				Span: roachpb.Span{
					Key: desc.StartKey.AsRawKey(),
				},
				Lease: roachpb.Lease{
					Start:       timestamp,
					StartStasis: startStasis,
					Expiration:  expiration,
					Replica:     *replica,
				},
			}
			ba := roachpb.BatchRequest{}
			ba.Timestamp = r.store.Clock().Now()
			ba.RangeID = r.RangeID
			ba.Add(args)

			// Send lease request directly to raft in order to skip unnecessary
			// checks from normal request machinery, (e.g. the command queue).
			// Note that the command itself isn't traced, but usually the caller
			// waiting for the result has an active Trace.
			ch, _, err := r.proposeRaftCommand(r.context(context.Background()), ba)
			if err != nil {
				return roachpb.NewError(err)
			}

			// If the command was committed, wait for the range to apply it.
			select {
			case c := <-ch:
				if c.Err != nil {
					if log.V(1) {
						log.Infof("failed to acquire leader lease for replica %s: %s", r.store, c.Err)
					}
				}
				return c.Err
			case <-r.store.Stopper().ShouldDrain():
				return roachpb.NewError(r.newNotLeaderError(nil, r.store.StoreID()))
			}
		}()

		// Send result of leader lease to all waiter channels.
		r.mu.Lock()
		defer r.mu.Unlock()
		for i, llChan := range r.mu.llChans {
			// Don't send the same pErr object twice; this can lead to
			// races. We could clone every time but it's more efficient to
			// send pErr itself to one of the channels (the last one; if we
			// send it earlier the race can still happen).
			if i == len(r.mu.llChans)-1 {
				llChan <- pErr
			} else {
				llChan <- protoutil.Clone(pErr).(*roachpb.Error) // works with `nil`
			}
		}
		r.mu.llChans = r.mu.llChans[:0]
	}) {
		// We failed to start the asynchronous task. Send a blank NotLeaderError
		// back to indicate that we have no idea who the leader might be; we've
		// withdrawn from active duty.
		llChan <- roachpb.NewError(r.newNotLeaderErrorLocked(nil, r.store.StoreID()))
		return llChan
	}

	r.mu.llChans = append(r.mu.llChans, llChan)
	return llChan
}
