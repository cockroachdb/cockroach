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
