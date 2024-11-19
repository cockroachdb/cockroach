// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgwirebase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
)

// Conn exposes some functionality of a pgwire network connection to be
// used by the Copy subprotocol implemented in the sql package.
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
	BeginCopyIn(ctx context.Context, columns []colinfo.ResultColumn, format FormatCode) error
}
