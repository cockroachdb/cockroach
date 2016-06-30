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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

/*
Package client and its KV API has been deprecated for external usage. Please use
a postgres-compatible SQL driver (e.g. github.com/lib/pq). For more details, see
http://www.cockroachlabs.com/blog/sql-in-cockroachdb-mapping-table-data-to-key-value-storage/.

Package client provides clients for accessing the various externally-facing
Cockroach database endpoints.

DB Client

The DB client is a fully-featured client of Cockroach's key-value database. It
provides a simple, synchronous interface well-suited to parallel updates and
queries.

The simplest way to use the client is through the Run method. Run synchronously
invokes the call, fills in the reply and returns an error. The example
below shows a get and a put.

	db, err := client.Open("rpcs://root@localhost:26257")
	if err != nil {
		log.Fatal(err)
	}
	if err := db.Put("a", "hello"); err != nil {
		log.Fatal(err)
	}
	if gr, err := db.Get("a"); err != nil {
		log.Fatal(err)
	} else {
		log.Printf("%s", gr.ValueBytes())  // "hello"
	}

The API is synchronous, but accommodates efficient parallel updates and queries
using Batch objects. An arbitrary number of calls may be added to a Batch which
is executed using DB.Run. Note however that the individual calls within a batch
are not guaranteed to have atomic semantics. A transaction must be used to
guarantee atomicity. A simple example of using a Batch which does two scans in
parallel and then sends a sequence of puts in parallel:

	db, err := client.Open("rpcs://root@localhost:26257")
	if err != nil {
		log.Fatal(err)
	}

	b1 := &client.Batch{}
	b1.Scan("a", "c\x00", 1000)
	b1.Scan("x", "z\x00", 1000)

	// Run sends both scans in parallel and returns the first error or nil.
	if err := db.Run(b1); err != nil {
		log.Fatal(err)
	}

	acResult := b1.Results[0]
	xzResult := b1.Results[1]

	// Append maximum value from "a"-"c" to all values from "x"-"z".
	max := []byte(nil)
	for _, row := range acResult.Rows {
		if bytes.Compare(max, row.ValueBytes()) < 0 {
			max = row.ValueBytes()
		}
	}

	b2 := &client.Batch{}
	for _, row := range xzResult.Rows {
		b2.Put(row.Key, bytes.Join([][]byte{row.ValueBytes(), max}, []byte(nil)))
	}

	// Run all puts for parallel execution.
	if err := db.Run(b2); err != nil {
		log.Fatal(err)
	}

Transactions are supported through the DB.Txn() method, which takes a retryable
function, itself composed of the same simple mix of API calls typical of a
non-transactional operation. Within the context of the Txn() call, all method
invocations are transparently given necessary transactional details, and
conflicts are handled with backoff/retry loops and transaction restarts as
necessary. An example of using transactions with parallel writes:

	db, err := client.Open("rpcs://root@localhost:26257")
	if err != nil {
		log.Fatal(err)
	}

	err := db.Txn(func(txn *client.Txn) error {
		b := db.NewBatch()
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("testkey-%02d", i)
			b.Put(key, "test value")
		}

		// Note that the Txn client is flushed automatically when this function
		// returns success (i.e. nil). Calling CommitInBatch explicitly can
		// sometimes reduce the number of RPCs.
		return txn.CommitInBatch(b)
	})
	if err != nil {
		log.Fatal(err)
	}

Note that with Cockroach's lock-free transactions, clients should expect
retries as a matter of course. This is why the transaction functionality is
exposed through a retryable function. The retryable function should have no
side effects which are not idempotent.

Transactions should endeavor to use batches to perform multiple operations in a
single RPC. In addition to the reduced number of RPCs to the server, this
allows writes to the same range to be batched together. In cases where the
entire transaction affects only a single range, transactions can commit in a
single round trip.
*/
package client
