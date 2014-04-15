// Copyright 2014 The Cockroach Authors.
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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Ben Darnell

/*
Package multiraft implements the Raft distributed consensus algorithm.

In contrast to other Raft implementations, this version is optimized for the case
where one server is a part of many Raft consensus groups (likely with overlapping membership).
This entails the use of a shared log and coalesced timers for heartbeats.

A cluster consists of a collection of nodes; the local node is represented by a MultiRaft
object.  Each node may participate in any number of groups.  Nodes must have a globally
unique ID (a string), and groups have a globally unique name.  The application is responsible
for providing a Transport interface that knows how to communicate with other nodes
based on their IDs, and a Storage interface to manage persistent data.

The Raft protocol is documented in "In Search of an Understandable Consensus Algorithm"
by Diego Ongaro and John Ousterhout.
https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf
*/
package multiraft
