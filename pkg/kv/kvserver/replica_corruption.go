// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// maybeSetCorrupt is a stand-in for proper handling of failing replicas. Such a
// failure is indicated by a call to maybeSetCorrupt with a ReplicaCorruptionError.
// Currently any error is passed through, but prospectively it should stop the
// range from participating in progress, trigger a rebalance operation and
// decide on an error-by-error basis whether the corruption is limited to the
// range, store, node or cluster with corresponding actions taken.
//
// Despite the fatal log call below this message we still return for the
// sake of testing.
//
// TODO(d4l3k): when marking a Replica corrupt, must subtract its stats from
// r.store.metrics. Errors which happen between committing a batch and sending
// a stats delta from the store are going to be particularly tricky and the
// best bet is to not have any of those.
// @bdarnell remarks: Corruption errors should be rare so we may want the store
// to just recompute its stats in the background when one occurs.
func (r *Replica) maybeSetCorrupt(ctx context.Context, pErr *roachpb.Error) *roachpb.Error {
	if cErr, ok := pErr.GetDetail().(*roachpb.ReplicaCorruptionError); ok {
		r.raftMu.Lock()
		defer r.raftMu.Unlock()
		return r.setCorruptRaftMuLocked(ctx, cErr)
	}
	return pErr
}

func (r *Replica) setCorruptRaftMuLocked(
	ctx context.Context, cErr *roachpb.ReplicaCorruptionError,
) *roachpb.Error {
	r.mu.Lock()
	defer r.mu.Unlock()

	log.ErrorfDepth(ctx, 1, "stalling replica due to: %s", cErr.ErrorMsg)
	cErr.Processed = true
	r.mu.destroyStatus.Set(cErr, destroyReasonRemoved)

	auxDir := r.store.engine.GetAuxiliaryDir()
	_ = os.MkdirAll(auxDir, 0755)
	path := base.PreventedStartupFile(auxDir)

	preventStartupMsg := fmt.Sprintf(`ATTENTION:

this node is terminating because replica %s detected an inconsistent state.
Please contact the CockroachDB support team. It is not necessarily safe
to replace this node; cluster data may still be at risk of corruption.

A file preventing this node from restarting was placed at:
%s
`, r, path)

	if err := ioutil.WriteFile(path, []byte(preventStartupMsg), 0644); err != nil {
		log.Warningf(ctx, "%v", err)
	}

	log.FatalfDepth(ctx, 1, "replica is corrupted: %s", cErr)
	return roachpb.NewError(cErr)
}
