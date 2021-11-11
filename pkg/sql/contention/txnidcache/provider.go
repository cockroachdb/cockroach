// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package txnidcache

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// Provider is the main interface for txnidcache.
type Provider interface {
	// Start starts the txnidcache.Provider.
	Start(ctx context.Context, stopper *stop.Stopper)

	reader
	messageSink
	writerPool
}

// Writer is the interface that can be used to write to txnidcache.
type Writer interface {
	writer

	// Close closes the Writer and flushes any pending data. The Writer should
	// not be used after its closed.
	Close()
}

type disconnector interface {
	// disconnect allows a Writer to be disconnected from its attached target.
	disconnect(Writer)
}

type writerPool interface {
	// GetWriter returns a Writer to the caller. After returned, the Writer
	// is attached to the writerPool, and can be disconnected using the
	// disconnector.
	GetWriter() Writer

	disconnector
}

// messageSink is implemented by the top-level cache so that the Writers
// can push message blocks into.
type messageSink interface {
	// push allows the writer to push a message block into the sink.
	push(messageBlock)

	disconnector
}

type writer interface {
	// Record writes a pair of transactionID and transaction fingerprint ID
	// into a temporary buffer. This buffer will eventually be flushed into
	// the transaction ID cache asynchronously.
	Record(resolvedTxnID ResolvedTxnID)

	// Flush starts the flushing process of writer's temporary buffer.
	Flush()
}

type reader interface {
	// Lookup returns the corresponding transaction fingerprint ID for a given txnID,
	// if the given txnID has no entry in the Cache, the returned "found" boolean
	// will be false.
	Lookup(txnID uuid.UUID) (result roachpb.TransactionFingerprintID, found bool)
}

type storage interface {
	reader
	writer
}
