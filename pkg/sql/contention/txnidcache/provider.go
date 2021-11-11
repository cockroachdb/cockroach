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

	"github.com/cockroachdb/cockroach/pkg/util/stop"
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
