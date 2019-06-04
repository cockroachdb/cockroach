// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

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
*/
package kv
