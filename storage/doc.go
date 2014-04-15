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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

/*
Package storage implements the Cockroach storage node. A storage node
exports the "Node" Go RPC service. Each node handles one or more
stores, identified by a device name. Each store corresponds to a single
physical device. A store multiplexes to one or more ranges, identified
by start key. Ranges are contiguous regions of the keyspace. Each range
implements an instance of the raft consensus algorithm to synchronize
range replicas.

The Engine interface provides an API for key-value stores. InMem
implements an in-memory engine using a sorted map. RocksDB implements
an engine for data stored to local disk using RocksDB, a variant of
LevelDB.
*/
package storage
