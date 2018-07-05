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

// Package minprop exports a main data structure, Tracker, which tracks in-flight
// timestamped commands and enforces successively higher thresholds below which
// no new commands can be created. This is used by follower reads to create
// "closed timestamps", i.e. timestamps below which no new Raft proposals are
// going to be proposed. In that context, the Close method of a Tracker
// establishes a new lower bound for future proposals, and returns a "closed
// timestamp" with the guarantee that all proposals with timestamps less than or
// equal to it have already been observed. The Track method in turn is called
// before each proposal to acquire a reference with the Tracker, and returns a
// minimum timestamp to be used for the proposal. Once the proposal is no longer
// in-flight (somewhat unintuitively, in this context this means that it has
// been proposed to Raft), the reference is released.
//
// A Tracker consists of two timestamps next and closed with associated ref
// counts, as well as locking which we'll ignore in the description. The goal
// of the Tracker is to regularly move closed and next forward in time, and to
// close out the next timestamp (with some optimizations to close out closed when
// it isn't referenced).
//
// next is the safe value below which no new proposals can exist, and closed is
// the timestamp below which new proposals are not possible. The gap between
// them contains the in-flight proposals that must finish before a higher
// timestamp can be closed out.
package minprop
