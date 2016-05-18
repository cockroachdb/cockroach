// Copyright 2016 The Cockroach Authors.
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
// Author: Radu Berinde (radu@cockroachlabs.com)

package distsql

import "github.com/cockroachdb/cockroach/sql/sqlbase"

type row []sqlbase.EncDatum

// rowReceiver is any component of a flow that receives rows from another
// component. It can be an input synchronizer, a router, or a mailbox.
type rowReceiver interface {
	// PushRow sends a row to this receiver. May block.
	// Returns true if the row was sent, or false if the receiver does not need
	// any more rows. In all cases, Close() still needs to be called.
	PushRow(row row) bool
	// Close is called when we have no more rows; it causes the rowReceiver to
	// process all rows and clean up. If err is not null, the error is sent to
	// the receiver (and the function may block).
	Close(err error)
}

// streamMsg is the message used in the channels that implement
// local physical streams.
type streamMsg struct {
	// Only one of these fields will be set.
	row row
	err error
}
