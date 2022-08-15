// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package lease

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// A lease stored in system.lease.
type storedLease struct {
	id        descpb.ID
	version   int
	sessionID sqlliveness.SessionID
}

func (s *storedLease) String() string {
	return fmt.Sprintf("ID = %d ver=%d session=%s", s.id, s.version, s.sessionID)
}

// descriptorVersionState holds the state for a descriptor version. This
// includes the lease information for a descriptor version.
// TODO(vivek): A node only needs to manage lease information on what it
// thinks is the latest version for a descriptor.
type descriptorVersionState struct {
	t *descriptorState
	// This descriptor is immutable and can be shared by many goroutines.
	// Care must be taken to not modify it.
	catalog.Descriptor

	// TOOD(jchan):
	session sqlliveness.Session

	mu struct {
		syncutil.Mutex

		refcount int
		// Set if the node has a lease on this descriptor version.
		// Leases can only be held for the two latest versions of
		// a descriptor. The latest version known to a node
		// (can be different than the current latest version in the store)
		// is always associated with a lease. The previous version known to
		// a node might not necessarily be associated with a lease.
		lease *storedLease
	}
}

func (s *descriptorVersionState) Release(ctx context.Context) {
	s.t.release(ctx, s)
}

func (s *descriptorVersionState) Underlying() catalog.Descriptor {
	return s.Descriptor
}

func (s *descriptorVersionState) Expiration() hlc.Timestamp {
	return s.getExpiration()
}

func (s *descriptorVersionState) SessionID() sqlliveness.SessionID {
	return s.session.ID()
}

func (s *descriptorVersionState) SafeMessage() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fmt.Sprintf("%d ver=%d:%s, refcount=%d", s.GetID(), s.GetVersion(), s.getExpiration(), s.mu.refcount)
}

func (s *descriptorVersionState) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stringLocked()
}

// stringLocked reads mu.refcount and thus needs to have mu held.
func (s *descriptorVersionState) stringLocked() string {
	return fmt.Sprintf("%d(%q) ver=%d:%s, refcount=%d", s.GetID(), s.GetName(), s.GetVersion(), s.getExpiration(), s.mu.refcount)
}

// hasExpired checks if the descriptor is too old to be used (by a txn
// operating) at the given timestamp.
func (s *descriptorVersionState) hasExpired(timestamp hlc.Timestamp) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hasExpiredLocked(timestamp)
}

// hasExpired checks if the descriptor is too old to be used (by a txn
// operating) at the given timestamp.
func (s *descriptorVersionState) hasExpiredLocked(timestamp hlc.Timestamp) bool {
	return s.getExpiration().LessEq(timestamp)
}

func (s *descriptorVersionState) incRefCount(ctx context.Context, expensiveLogEnabled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.incRefCountLocked(ctx, expensiveLogEnabled)
}

func (s *descriptorVersionState) incRefCountLocked(ctx context.Context, expensiveLogEnabled bool) {
	s.mu.refcount++
	if expensiveLogEnabled {
		log.VEventf(ctx, 2, "descriptorVersionState.incRefCount: %s", s.stringLocked())
	}
}

func (s *descriptorVersionState) getExpiration() hlc.Timestamp {
	return s.session.Expiration()
}

func (s *descriptorVersionState) GetName() string {
	return s.Descriptor.GetName()
}
