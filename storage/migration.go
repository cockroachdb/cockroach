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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package storage

import (
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// MIGRATION(tschottdorf): As of #7310, we make sure that a Replica always has
// a complete Raft state on disk. Prior versions may not have that, which
// causes issues due to the fact that we used to synthesize a TruncatedState
// and do so no more. To make up for that, write a missing TruncatedState here.
// That key is in the replicated state, but since during a cluster upgrade, all
// nodes do it, it's fine (and we never CPut on that key, so anything in the
// Raft pipeline will simply overwrite it).
//
// Migration(tschottdorf): See #6991. It's possible that the HardState is
// missing after a snapshot was applied (so there is a TruncatedState). In this
// case, synthesize a HardState (simply setting everything that was in the
// snapshot to committed). Having lost the original HardState can theoretically
// mean that the replica was further ahead or had voted, and so there's no
// guarantee that this will be correct. But it will be correct in the majority
// of cases, and some state *has* to be recovered.
func migrate7310And6991(ctx context.Context, batch engine.ReadWriter, desc roachpb.RangeDescriptor) error {
	state, err := loadState(ctx, batch, &desc)
	if err != nil {
		return errors.Wrap(err, "could not migrate TruncatedState: %s")
	}
	if (*state.TruncatedState == roachpb.RaftTruncatedState{}) {
		state.TruncatedState.Term = raftInitialLogTerm
		state.TruncatedState.Index = raftInitialLogIndex
		state.RaftAppliedIndex = raftInitialLogIndex
		if _, err := saveState(ctx, batch, state); err != nil {
			return errors.Wrapf(err, "could not migrate TruncatedState to %+v", &state.TruncatedState)
		}
		log.Warningf(ctx, "migration: synthesized TruncatedState for %+v", desc)
	}

	hs, err := loadHardState(ctx, batch, desc.RangeID)
	if err != nil {
		return errors.Wrap(err, "unable to load HardState")
	}
	// Only update the HardState when there is a nontrivial Commit field. We
	// don't have a snapshot here, so we could wind up lowering the commit
	// index (which would error out and fatal us).
	if hs.Commit == 0 {
		if err := synthesizeHardState(ctx, batch, state, hs); err != nil {
			return errors.Wrap(err, "could not migrate HardState")
		}
	}
	return nil
}
