// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package lease

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/redact"
)

// A lease stored in system.lease.
type storedLease struct {
	id         descpb.ID
	prefix     []byte
	version    int
	expiration tree.DTimestamp
	sessionID  []byte
}

func (s *storedLease) String() string {
	return redact.StringWithoutMarkers(s)
}

var _ redact.SafeFormatter = (*storedLease)(nil)

// SafeFormat implements redact.SafeFormatter.
func (s *storedLease) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("ID=%d ver=%d expiration=%s", s.id, s.version, s.expiration)
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

	refcount atomic.Int32

	// The session that was used to acquire this descriptor version.
	session atomic.Pointer[sqlliveness.Session]

	// The expiration time for the descriptor version, which will be non-nil
	// if one exists (i.e. only for the previous descriptor versions). A transaction with
	// timestamp T can use this descriptor version iff
	// Descriptor.GetDescriptorModificationTime() <= T < expiration.
	//
	// The expiration time is either the expiration time of the lease when a lease
	// is associated with the version, or the ModificationTime of the next version
	// when the version isn't associated with a lease.
	expiration atomic.Pointer[hlc.Timestamp]

	mu struct {
		syncutil.Mutex

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

func (s *descriptorVersionState) Expiration(ctx context.Context) hlc.Timestamp {
	return s.getExpiration(ctx)
}

// SafeFormat implements redact.SafeFormatter.
func (s *descriptorVersionState) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Print(s.redactedString())
}

func (s *descriptorVersionState) String() string {
	return redact.StringWithoutMarkers(s)
}

// redactedString reads refcount and thus no longer needs mu held.
func (s *descriptorVersionState) redactedString() redact.RedactableString {
	var sessionID string
	if session := s.session.Load(); session != nil {
		sessionID = (*session).ID().String()
	}
	var expiration hlc.Timestamp
	if exp := s.expiration.Load(); exp != nil && !exp.IsEmpty() {
		expiration = *exp
	}
	return redact.Sprintf("%d(%q,%s) ver=%d:%s, refcount=%d", s.GetID(), s.GetName(), redact.SafeString(sessionID), s.GetVersion(), expiration, s.refcount.Load())
}

// getSessionID returns the current session ID from the lease.
func (s *descriptorVersionState) getSessionID() sqlliveness.SessionID {
	return (*s.session.Load()).ID()
}

// hasExpired checks if the descriptor is too old to be used (by a txn
// operating) at the given timestamp.
func (s *descriptorVersionState) hasExpired(ctx context.Context, timestamp hlc.Timestamp) bool {
	return s.getExpiration(ctx).LessEq(timestamp)
}

func (s *descriptorVersionState) incRefCount(ctx context.Context, expensiveLogEnabled bool) {
	s.refcount.Add(1)
	if expensiveLogEnabled {
		log.VEventf(ctx, 2, "descriptorVersionState.incRefCount: %s", s.redactedString())
	}
}

func (s *descriptorVersionState) getExpiration(ctx context.Context) hlc.Timestamp {
	// If an expiration is set then this descriptor is a stale version,
	// and will be eventually removed.
	if exp := s.expiration.Load(); exp != nil && !exp.IsEmpty() {
		return *exp
	}
	// Otherwise, the expiration is tied to sqlliveness.
	return (*s.session.Load()).Expiration()
}
