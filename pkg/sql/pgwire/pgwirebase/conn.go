// Copyright 2018 The Cockroach Authors.
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

package pgwirebase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// Conn exposes some functionality of a pgwire network connection to be
// used by the Copy-in subprotocol implemented in the sql package.
type Conn interface {
	// Rd returns a reader to be used to consume bytes from the connection.
	// This reader can be used with a pgwirebase.ReadBuffer for reading messages.
	//
	// Note that in the pgwire implementation, this reader encapsulates logic for
	// updating connection metrics.
	Rd() BufferedReader

	// BeginCopyIn sends the message server message initiating the Copy-in
	// subprotocol (COPY ... FROM STDIN). This message informs the client about
	// the columns that are expected for the rows to be inserted.
	//
	// Currently, we only support the "text" format for COPY IN.
	// See: https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-COPY
	BeginCopyIn(ctx context.Context, columns []sqlbase.ResultColumn) error

	// SendCommandComplete sends a serverMsgCommandComplete with the given
	// payload.
	SendCommandComplete(tag []byte) error
}
