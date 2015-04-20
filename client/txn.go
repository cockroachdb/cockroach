// Copyright 2015 The Cockroach Authors.
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
// Author: Peter Mattis (peter.mattis@gmail.com)

package client

// Txn provides serial access to a KV store via Run and parallel
// access via Prepare and Flush. A Txn instance is not thread
// safe.
type Txn struct {
	kv       KV
	prepared []Call
}

// Run runs the specified calls synchronously in a single batch and
// returns any errors.
func (t *Txn) Run(calls ...Call) error {
	if len(calls) == 0 {
		return nil
	}
	if len(t.prepared) > 0 || len(calls) > 1 {
		t.Prepare(calls...)
		return t.Flush()
	}
	return t.kv.Run(calls...)
}

// Prepare accepts a KV API call, specified by arguments and a reply
// struct. The call will be buffered locally until the first call to
// Flush(), at which time it will be sent for execution as part of a
// batch call. Using Prepare/Flush parallelizes queries and updates
// and should be used where possible for efficiency.
//
// For clients using an HTTP sender, Prepare/Flush allows multiple
// commands to be sent over the same connection. Prepare/Flush can
// dramatically improve efficiency by compressing multiple writes into
// a single atomic update in the event that the writes are to keys
// within a single range.
//
// TODO(pmattis): Can Prepare/Flush be replaced with a Batch struct?
// Doing so could potentially make the Txn interface more symmetric
// with the KV interface, but potentially removes the optimization to
// send the EndTransaction in the same batch as the final set of
// prepared calls.
func (t *Txn) Prepare(calls ...Call) {
	for _, c := range calls {
		c.resetClientCmdID(t.kv.clock)
	}
	t.prepared = append(t.prepared, calls...)
}

// Flush sends all previously prepared calls, buffered by invocations
// of Prepare(). The calls are organized into a single batch command
// and sent together. Flush returns nil if all prepared calls are
// executed successfully. Otherwise, Flush returns the first error,
// where calls are executed in the order in which they were prepared.
// After Flush returns, all prepared reply structs will be valid.
func (t *Txn) Flush() error {
	calls := t.prepared
	t.prepared = nil
	return t.kv.Run(calls...)
}
