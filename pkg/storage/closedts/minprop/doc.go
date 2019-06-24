// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package minprop exports a main data structure, Tracker, which checkpoints
// closed timestamps and associated Raft Lease Applied indexes positions for
// which (under additional conditions) it is legal to serve follower reads. It
// does so by maintaining a 'next' timestamp above which new command evaluations
// are forced, and by tracking when all in-flight evaluations below this
// timestamp have completed (at which point a call to the Close method succeeds:
// 'next' becomes closed, and a new 'next' is initialized with a future
// timestamp).
//
// In-flight command evaluations are tracked via the Track method which acquires
// a reference with the tracker, returns a minimum timestamp to be used for the
// proposal evaluation, and provides a closure that releases the reference with
// a lease applied index used for the proposal.
package minprop
