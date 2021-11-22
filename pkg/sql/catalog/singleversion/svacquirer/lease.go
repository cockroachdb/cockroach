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

type lease struct {
	a         *Acquirer
	id        descpb.ID
	startTime hlc.Timestamp
	session   sqlliveness.Session

	acquirerMu struct {
		highestUsedTimestamp hlc.Timestamp
		refCount             int
	}

	acquisition struct {
		done chan struct{}
		err  error
	}

	release struct {
		done chan struct{} // only populated when releasing
		err  error
	}
}

func (l lease) ID() descpb.ID {
	return l.id
}

func (l lease) Start() hlc.Timestamp {
	return l.startTime
}

func (l lease) Expiration() hlc.Timestamp {
	return l.session.Expiration()
}

func (l *lease) incRefCount() {
	l.acquirerMu.refCount++
}

func (l *lease) waitForValid(ctx context.Context) (retry bool, err error) {
	if l.beingReleased() {
		err := waitForDone(ctx, l.a.stopper, l.release.done)
		return err == nil, err
	}
	return false, l.waitForAcquisition(ctx)
}

func (l *lease) waitForAcquisition(ctx context.Context) (err error) {
	if err := waitForDone(ctx, l.a.stopper, l.acquisition.done); err != nil {
		l.Release(ctx, hlc.Timestamp{})
		return err
	}
	return l.acquisition.err
}

func (l *lease) Release(ctx context.Context, maxUsedTimestamp hlc.Timestamp) {
	l.a.release(ctx, maxUsedTimestamp, l)
}

func (l lease) beingReleased() bool {
	return l.release.done != nil
}
