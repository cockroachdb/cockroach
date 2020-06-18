// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package leasemanager provides functionality for acquiring and managing leases
// via the kv api for use during sqlmigrations.
package leasemanager

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// DefaultLeaseDuration is the duration a lease will be acquired for if no
// duration was specified in a LeaseManager's options.
// Exported for testing purposes.
const DefaultLeaseDuration = 1 * time.Minute

// LeaseNotAvailableError indicates that the lease the caller attempted to
// acquire is currently held by a different client.
type LeaseNotAvailableError struct {
	key        roachpb.Key
	expiration hlc.Timestamp
}

func (e *LeaseNotAvailableError) Error() string {
	return fmt.Sprintf("lease %q is not available until at least %s", e.key, e.expiration)
}

// LeaseManager provides functionality for acquiring and managing leases
// via the kv api.
type LeaseManager struct {
	db            *kv.DB
	clock         *hlc.Clock
	clientID      string
	leaseDuration time.Duration
}

// Lease contains the state of a lease on a particular key.
type Lease struct {
	key roachpb.Key
	val struct {
		sem      chan struct{}
		lease    *LeaseVal
		leaseRaw []byte
	}
}

// Options are used to configure a new LeaseManager.
type Options struct {
	// ClientID must be unique to this LeaseManager instance.
	ClientID      string
	LeaseDuration time.Duration
}

// New allocates a new LeaseManager.
func New(db *kv.DB, clock *hlc.Clock, options Options) *LeaseManager {
	if options.ClientID == "" {
		options.ClientID = uuid.MakeV4().String()
	}
	if options.LeaseDuration <= 0 {
		options.LeaseDuration = DefaultLeaseDuration
	}
	return &LeaseManager{
		db:            db,
		clock:         clock,
		clientID:      options.ClientID,
		leaseDuration: options.LeaseDuration,
	}
}

// AcquireLease attempts to grab a lease on the provided key. Returns a non-nil
// lease object if it was successful, or an error if it failed to acquire the
// lease for any reason.
//
// NB: Acquiring a non-expired lease is allowed if this LeaseManager's clientID
// matches the lease owner's ID. This behavior allows a process to re-grab
// leases without having to wait if it restarts and uses the same ID.
func (m *LeaseManager) AcquireLease(ctx context.Context, key roachpb.Key) (*Lease, error) {
	lease := &Lease{
		key: key,
	}
	lease.val.sem = make(chan struct{}, 1)
	if err := m.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var val LeaseVal
		err := txn.GetProto(ctx, key, &val)
		if err != nil {
			return err
		}
		if !m.leaseAvailable(&val) {
			return &LeaseNotAvailableError{key: key, expiration: val.Expiration}
		}
		lease.val.lease = &LeaseVal{
			Owner:      m.clientID,
			Expiration: m.clock.Now().Add(m.leaseDuration.Nanoseconds(), 0),
		}
		var leaseRaw roachpb.Value
		if err := leaseRaw.SetProto(lease.val.lease); err != nil {
			return err
		}
		if err := txn.Put(ctx, key, &leaseRaw); err != nil {
			return err
		}
		lease.val.leaseRaw = leaseRaw.TagAndDataBytes()
		return nil
	}); err != nil {
		return nil, err
	}
	return lease, nil
}

func (m *LeaseManager) leaseAvailable(val *LeaseVal) bool {
	return val.Owner == m.clientID || m.timeRemaining(val) <= 0
}

// TimeRemaining returns the amount of time left on the given lease.
func (m *LeaseManager) TimeRemaining(l *Lease) time.Duration {
	l.val.sem <- struct{}{}
	defer func() { <-l.val.sem }()
	return m.timeRemaining(l.val.lease)
}

func (m *LeaseManager) timeRemaining(val *LeaseVal) time.Duration {
	maxOffset := m.clock.MaxOffset()
	return val.Expiration.GoTime().Sub(m.clock.Now().GoTime()) - maxOffset
}

// ExtendLease attempts to push the expiration time of the lease farther out
// into the future.
func (m *LeaseManager) ExtendLease(ctx context.Context, l *Lease) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case l.val.sem <- struct{}{}:
	}
	defer func() { <-l.val.sem }()

	if m.timeRemaining(l.val.lease) < 0 {
		return errors.Errorf("can't extend lease that expired at time %s", l.val.lease.Expiration)
	}

	newVal := &LeaseVal{
		Owner:      m.clientID,
		Expiration: m.clock.Now().Add(m.leaseDuration.Nanoseconds(), 0),
	}
	var newRaw roachpb.Value
	if err := newRaw.SetProto(newVal); err != nil {
		return err
	}
	if err := m.db.CPut(ctx, l.key, &newRaw, l.val.leaseRaw); err != nil {
		if errors.HasType(err, (*roachpb.ConditionFailedError)(nil)) {
			// Something is wrong - immediately expire the local lease state.
			l.val.lease.Expiration = hlc.Timestamp{}
			return errors.Wrapf(err, "local lease state %v out of sync with DB state", l.val.lease)
		}
		return err
	}
	l.val.lease = newVal
	l.val.leaseRaw = newRaw.TagAndDataBytes()
	return nil
}

// ReleaseLease attempts to release the given lease so that another process can
// grab it.
func (m *LeaseManager) ReleaseLease(ctx context.Context, l *Lease) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case l.val.sem <- struct{}{}:
	}
	defer func() { <-l.val.sem }()

	return m.db.CPut(ctx, l.key, nil /* value - delete the lease */, l.val.leaseRaw)
}
