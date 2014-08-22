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
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

/*
Package storage provides access to the Store and Range
abstractions. Each Cockroach node handles one or more stores, each of
which multiplexes to one or more ranges, identified by [start, end)
keys. Ranges are contiguous regions of the keyspace. Each range
implements an instance of the Raft consensus algorithm to synchronize
participating range replicas.

Each store is represented by a single engine.Engine instance. The
ranges hosted by a store all have access to the same engine, but write
to only a range-limited keyspace within it. Ranges access the
underlying engine via the MVCC interface, which provides historical
versioned values.
*/
package storage
