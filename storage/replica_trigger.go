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
	"github.com/cockroachdb/cockroach/storage/engine/enginepb"
)

// postCommitSplit is emitted when a Replica commits a split trigger and
// signals that the Replica has prepared the on-disk state for both the left
// and right hand sides of the split, and that the left hand side Replica
// should be updated as well as the right hand side created.
type postCommitSplit struct {
	roachpb.SplitTrigger
	// RHSDelta holds the statistics for what was written to what is now the
	// right-hand side of the split during the batch which executed it.
	// The on-disk state of the right-hand side is already correct, but the
	// Store must learn about this delta to update its counters appropriately.
	RightDeltaMS enginepb.MVCCStats
}

type proposalResult struct {
	*PostCommitTrigger

	// The stats delta that the application of the Raft command would cause.
	// On a split, contains only the contributions to the left-hand side.
	//
	// TODO(tschottdorf): we could also not send this along and compute it
	// from the new stats (which are contained in the write batch). See about
	// a potential performance penalty (reads forcing an index to be built for
	// what is initially a slim Go batch) in doing so.
	//
	// We are interested in this delta only to report it to the Store, which
	// keeps a running total of all of its Replicas' stats.
	delta enginepb.MVCCStats
}

// PostCommitTrigger is returned from Raft processing as a side effect which
// signals that further action should be taken as part of the processing of the
// Raft command.
// Depending on the content, actions may be executed on all Replicas, the lease
// holder, or a Replica determined by other conditions present in the specific
// trigger.
type PostCommitTrigger struct {
	// intents stores any intents encountered but not conflicted with. They
	// should be handed off to asynchronous intent processing so that an
	// attempt to resolve them is made.
	intents []intentsWithArg
	// split contains a postCommitSplit trigger emitted on a split.
	split *postCommitSplit
}

// updateTrigger takes a previous and new commit trigger and combines their
// contents into an updated trigger, consuming both inputs. It will panic on
// illegal combinations (such as being asked to combine two split triggers).
func updateTrigger(old, new *PostCommitTrigger) *PostCommitTrigger {
	if old == nil {
		old = new
	} else if new != nil {
		if new.intents != nil {
			old.intents = append(old.intents, new.intents...)
		}
		if old.split == nil {
			old.split = new.split
		} else if new.split != nil {
			panic("more than one split trigger")
		}
	}
	return old
}
