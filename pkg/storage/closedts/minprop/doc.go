// Copyright 2018 The Cockroach Authors.
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
