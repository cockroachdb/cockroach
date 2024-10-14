// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/*
Package kvserver provides access to the Store and Range
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
package kvserver
