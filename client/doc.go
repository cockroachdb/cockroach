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
Package client provides clients for accessing the various
externally-facing Cockroach database endpoints.

KV Client

The KV client is a fully-featured client of Cockroach's key-value
database. It provides a simple, synchronous interface well-suited to
parallel updates and queries.

The simplest way to use the client is through the Run method. Run
synchronously invokes the call and fills in the the reply and returns
an error. The example below shows a get and a put.

  kv := client.NewKV(nil, client.NewHTTPSender("localhost:8080", httpClient))

  getCall := client.GetCall(proto.Key("a"))
  getResp := getCall.Reply.(*proto.GetResponse)
  if err := kv.Run(getCall); err != nil {
    log.Fatal(err)
  }
  if err := kv.Run(client.PutCall(proto.Key("b"), getResp.Value.Bytes)) err != nil {
    log.Fatal(err)
  }

The API is synchronous, but accommodates efficient parallel updates
and queries using the variadic Run method. An arbitrary number of
calls may be passed to Run which are sent to Cockroach as part of a
batch. Note however that such the individual API calls within a batch
are not guaranteed to have atomic semantics. A transaction must be
used to guarantee atomicity. A simple example of using the API which
does two scans in parallel and then sends a sequence of puts in
parallel:

  kv := client.NewKV(nil, client.NewHTTPSender("localhost:8080", httpClient))

  acScanCall := client.ScanCall(proto.Key("a"), proto.Key("c\x00"), 1000)
  xzScanCall := client.ScanCall(proto.Key("x"), proto.Key("z\x00"), 1000)

  // Run sends both scans in parallel and returns first error or nil.
  if err := kv.Run(acScanCall, xzScanCall); err != nil {
    log.Fatal(err)
  }

  acResp := acScanCall.Reply.(*proto.ScanResponse)
  xzResp := xzScanCall.Reply.(*proto.ScanResponse)

  // Append maximum value from "a"-"c" to all values from "x"-"z".
  max := []byte(nil)
  for _, keyVal := range acResp.Rows {
    if bytes.Compare(max, keyVal.Value.Bytes) < 0 {
      max = keyVal.Value.Bytes
    }
  }
  var calls []*client.Call
  for keyVal := range xzResp.Rows {
    putCall := client.PutCall(keyVal.Key, bytes.Join([][]byte{keyVal.Value.Bytes, max}, []byte(nil)))
    calls = append(calls, putCall)
  }

  // Run all puts for parallel execution.
  if err := kv.Run(calls...); err != nil {
    log.Fatal(err)
  }

Transactions are supported through the RunTransaction() method, which
takes a retryable function, itself composed of the same simple mix of
API calls typical of a non-transactional operation. Within the context
of the RunTransaction call, all method invocations are transparently
given necessary transactional details, and conflicts are handled with
backoff/retry loops and transaction restarts as necessary. An example
of using transactions with parallel writes:

  kv := client.NewKV(nil, client.NewHTTPSender("localhost:8080", httpClient))

  opts := &client.TransactionOptions{Name: "test", Isolation: proto.SERIALIZABLE}
  err := kv.RunTransaction(opts, func(txn *client.Txn) error {
    for i := 0; i < 100; i++ {
      key := proto.Key(fmt.Sprintf("testkey-%02d", i))
      txn.Prepare(client.PutCall(key, []byte("test value")))
    }

    // Note that the Txn client is flushed automatically on transaction
    // commit. Invoking Flush after individual API methods is only
    // required if the result needs to be received to take conditional
    // action.
    return nil
  })
  if err != nil {
    log.Fatal(err)
  }

Note that with Cockroach's lock-free transactions, clients should
expect retries as a matter of course. This is why the transaction
functionality is exposed through a retryable function. The retryable
function should have no side effects which are not idempotent.

Transactions should endeavor to write using KV.Prepare calls. This
allows writes to the same range to be batched together. In cases where
the entire transaction affects only a single range, transactions can
commit in a single round trip.
*/
package client
