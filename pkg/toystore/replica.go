// Copyright 2019 The Cockroach Authors.
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

package toystore

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/toystore/destroyguard"
)

// GuardedReplica is the external interface through which callers interact with
// a Replica. By acquiring references to the underlying Replica object while it
// is being used, it allows safe removal of Replica objects and/or their under-
// lying data. This is an important fundamental for five reasons:
//
// # Confidence
// Today, we use no such mechanism, though this is largely due to failing in the
// past to establish one[1]. The status quo "works" by maintaining a
// destroyStatus inside of Replica.mu, which is checked in a few places such as
// before proposing a command or serving a read. Since these checks are only
// "point in time", and nothing prevents the write to the status from occurring
// just a moment after the check, there is a high cognitive overhead to
// reasoning about the possible outcomes. In fact, in the case in which things
// could go bad most spectacularly, namely removing a replica including data, we
// hold essentially all of the locks available to us and rely on the
// readOnlyCmdMu (which we would rather get rid off). This then is the first
// reason for proposing this change: make the replica lifetime easier to reason
// about and establish confidence that the Replica can't just disappear out from
// under us.
//
// # Simplified lifetime state transitions
// The second motivator are ReplicaID transitions, which can be extremely
// complicated. For example, a Replica may
//
// 1. start off as an uninitialized Replica with ReplicaID 12 (i.e. no data)
// 2. receive a preemptive snapshot, which confusingly results in an initialized
//    Replica with ReplicaID 12 (though preemptive snapshots nominally should
//    result in a preemptive replica -- one with ReplicaID zero).
// 3. update its ReplicaID to 18 (i.e. being added back to the Raft group).
// 4. get ReplicaGC'ed because
// 5. it blocks a preemptive snapshot, which now recreates it.
//
// In my point of view, changing the ReplicaID for a live Replica is a bad idea
// and incurs too much complexity. An architecture in which Replica objects have
// a single ReplicaID throughout their lifetime is conceptually much simpler,
// but it is also much more straightforward to maintain since it does away with
// a whole class of concurrency that needs to be tamed in today's code, and which
// may have performance repercussions. On the other hand, replicaID changes are
// not frequent, and only need to be moderately fast.
//
// The alternative is to instantiate a new incarnation of the Replica whenever
// the ReplicaID changes. The difficult part about this is destroying the old
// Replica; since Replica provides proper serialization, we mustn't have commands
// in-flight in two instances for the same data (and generally we want to avoid
// even having to think about concurrent use of old incarnations). This is
// explored here. The above history would read something like this:
//
// 1. start off as an uninitialized Replica R with Replica ID 12 (no data)
// 2. preemptive snapshot is received: tear down R, instantiate R'
// 3. ReplicaID is updated: tear down R', instantiate R''
// 4. R'' is marked for ReplicaGC: replace with placeholder R''' in Store, tear
//    down R'', wait for references to drain, remove the data, remove R'''.
// 5. instantiate R''' (no change from before).
//
// # Enable separate Replica types
// A third upshot is almost visible in the above description. Once we can re-
// instantiate cleanly on ReplicaID-based state changes, we might as well go
// ahead and pull apart various types of Replica:
//
// - preemptive snapshots (though we may replace those by learner replicas in
//   the future[2])
// - uninitialized Replicas (has replicaID, but no data)
// - initialized Replicas (has replicaID and data)
// - placeholders (have no replicaID and no data)
//
// To simplify replicaGC and to remove the preemptive snapshot state even before
// we stop using preemptive snapshots, we may allow placeholders to hold data
// (but with no means to run client requests against it).
//
// # Reduce footprint for idle Replicas
//
// Once there, reducing the memory footprint for "idle" replicas by only having
// a "shim" in memory (replacing the "full" version) until the next request
// comes in becomes a natural proposition by introducing a new replica state
// that upon access gets promoted to a full replica (which loads the full
// in-memory state from disk), pickup up past attempts at this which failed due
// to the technical debt present at the moment[3].
//
// [1]: https://github.com/cockroachdb/cockroach/issues/8630
// [2]: https://github.com/cockroachdb/cockroach/issues/34058
// [3]: https://github.com/cockroachdb/cockroach/pull/31663
type GuardedReplica struct {
	guard destroyguard.Guard

	// The remainder of Replica goes here.
}

// Replica is struct-identical to GuardedReplica, but exposes internal methods
// only (for which the assumption is that the caller transitively already holds
// a reference).
type Replica GuardedReplica

func (r *GuardedReplica) Send(ctx context.Context, req Request) (Response, error) {
	if err := r.guard.Acquire(); err != nil {
		return Response{}, err
	}
	defer r.guard.Release()
	return (*Replica)(r).Execute(ctx, req)
}

func (r *Replica) Execute(ctx context.Context, req Request) (Response, error) {
	// Placeholder for command execution.
	// Run checks and preprocess requests.
	// Check lease, perhaps trigger new lease and wait.
	// Enter spanlatch manager and wait.
	// Query timestamp cache.
	// Evaluate command (basically last step on read).
	// Propose.

	// Wait for result of proposal. Here, simulate a proposal that's stuck
	// (for example because the replica is no longer part of the range).
	ch := make(chan struct{})
	select {
	case <-ctx.Done():
		return Response{}, ctx.Err()
	case <-r.guard.Done():
		// The Replica is being torn down, so return up the stack to relinquish
		// any references held to it.
		return Response{}, r.guard.Err()
	case <-ch:
		return Response{}, nil
	}

	// Handle side effects.
	// Update timestamp cache.
	// Leave spanlatch manager.
}
