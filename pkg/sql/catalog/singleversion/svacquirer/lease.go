// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package svacquirer

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// lease corresponds to a written lease in the singleversion table's lease
// section.
type lease struct {

	// These fields are immutable for this lease instance.
	id        descpb.ID
	startTime hlc.Timestamp
	session   sqlliveness.Session

	// These fields are only ever modified under the Acquirer's mutex or when
	// the struct is being destructed. Group the Acquirer here to indicate that
	// it is responsible for managing these fields.
	a struct {
		*Acquirer

		// refCount tracks the number of outstanding in-memory references to this
		// lease.
		refCount int

		// highestUsedTimestamp is noted when a reference to lease is released. It
		// is important that before the lease is dropped that the local clock is
		// pushed above the highestUsedTimestamp by any lessee.
		highestUsedTimestamp hlc.Timestamp
	}

	acquisition waiter
	release     waiter
}

type waiter struct {
	done chan struct{}
	err  error
}

// Release releases the reference count to this lease.
func (l *lease) Release(ctx context.Context, maxUsedTimestamp hlc.Timestamp) {
	l.a.release(ctx, maxUsedTimestamp, l)
}

// ID is the descriptor ID to which this lease corresponds.
func (l lease) ID() descpb.ID {
	return l.id
}

// Start is the hlc timestamp at which this lease's validity interval begins.
// Note that this value is static for the life of the lease.
func (l lease) Start() hlc.Timestamp {
	return l.startTime
}

// Expiration is the current expiration of the lease. It represents the end
// of the lease's validity interval at the current moment. This value will
// continue to increase monotonically over time unless the underlying
// sqlliveness session expires.
func (l lease) Expiration() hlc.Timestamp {
	return l.session.Expiration()
}

func (l *lease) incRefCount() {
	l.a.refCount++
}

func (l *lease) waitForValid(ctx context.Context) (retry bool, err error) {
	if l.beingReleased() {
		err := l.release.wait(ctx, l.a.stopper)
		return err == nil, l.release.err
	}
	return false, l.waitForAcquisition(ctx)
}

func (l *lease) waitForAcquisition(ctx context.Context) (err error) {
	if err := l.acquisition.wait(ctx, l.a.stopper); err != nil {
		l.Release(ctx, hlc.Timestamp{})
		return err
	}
	return nil
}

func (l lease) beingReleased() bool {
	return l.release.done != nil
}
