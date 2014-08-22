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
Package kv provides a key-value API to an underlying cockroach
datastore. Cockroach itself provides a single, monolithic, sorted key
value map, distributed over multiple nodes. Each node holds a set of
key ranges. Package kv translates between the monolithic, logical map
which Cockroach clients experience to the physically distributed key
ranges which comprise the whole.

Package kv implements the logic necessary to locate appropriate nodes
based on keys being read or written. In some cases, requests may span
a range of keys, in which case multiple RPCs may be sent out.

The API is asynchronous and meant to be exploited as such. If an
operation requires fetching multiple keys to satisfy a computation,
they should be fetched in parallel and only when all are in flight
should the caller block. Here's an example of usage:

  foo := kvDB.Get(&kv.GetRequest{[]byte("foo")})
  bar := kvDB.Get(&kv.GetRequest{[]byte("bar")})

  fooVal <-foo
  barVal <-bar

  // Should check errors, but leaving out for brevity.
  if string(fooVal.Value) == string(barVal.Value) {
    // Values are equal; take action.
  }

*/
package kv
