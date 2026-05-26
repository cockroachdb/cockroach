// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/*
Package kvcoord provides a key-value API to an underlying cockroach
datastore. Cockroach itself provides a single, monolithic, sorted key
value map, distributed over multiple nodes. Each node holds a set of
key ranges. Package kv translates between the monolithic, logical map
which Cockroach clients experience to the physically distributed key
ranges which comprise the whole.

Package kv implements the logic necessary to locate appropriate nodes
based on keys being read or written. In some cases, requests may span
a range of keys, in which case multiple RPCs may be sent out.
*/
package kvcoord
