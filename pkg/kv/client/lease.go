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

package client

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// DefaultLeaseDuration is the duration a lease will be acquired for if no
// duration was specified in a LeaseManager's options.
// Exported for testing purposes.
const DefaultLeaseDuration = time.Minute

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
	db            *DB
	clock         *hlc.Clock
	clientID      string
	leaseDuration time.Duration
}

// Lease contains the state of a lease on a particular key.
type Lease struct {
	key roachpb.Key
	val *LeaseVal
}

// LeaseManagerOptions are used to configure a new LeaseManager.
type LeaseManagerOptions struct {
	// ClientID must be unique to this LeaseManager instance.
	ClientID      string
	LeaseDuration time.Duration
}

// NewLeaseManager allocates a new LeaseManager.
func NewLeaseManager(db *DB, clock *hlc.Clock, options LeaseManagerOptions) *LeaseManager {
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
	if err := m.db.Txn(ctx, func(ctx context.Context, txn *Txn) error {
		var val LeaseVal
		err := txn.GetProto(ctx, key, &val)
		if err != nil {
			return err
		}
		if !m.leaseAvailable(&val) {
			return &LeaseNotAvailableError{key: key, expiration: val.Expiration}
		}
		lease.val = &LeaseVal{
			Owner:      m.clientID,
			Expiration: m.clock.Now().Add(m.leaseDuration.Nanoseconds(), 0),
		}
		return txn.Put(ctx, key, lease.val)
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
	return m.timeRemaining(l.val)
}

func (m *LeaseManager) timeRemaining(val *LeaseVal) time.Duration {
	return val.Expiration.GoTime().Sub(m.clock.Now().GoTime()) - m.clock.MaxOffset()
}

// ExtendLease attempts to push the expiration time of the lease farther out
// into the future.
func (m *LeaseManager) ExtendLease(ctx context.Context, l *Lease) error {
	if m.TimeRemaining(l) < 0 {
		return errors.Errorf("can't extend lease that expired at time %s", l.val.Expiration)
	}

	newVal := &LeaseVal{
		Owner:      m.clientID,
		Expiration: m.clock.Now().Add(m.leaseDuration.Nanoseconds(), 0),
	}

	if err := m.db.CPut(ctx, l.key, newVal, l.val); err != nil {
		if _, ok := err.(*roachpb.ConditionFailedError); ok {
			// Something is wrong - immediately expire the local lease state.
			l.val.Expiration = hlc.Timestamp{}
			return errors.Wrapf(err, "local lease state %v out of sync with DB state", l.val)
		}
		return err
	}
	l.val = newVal
	return nil
}

// ReleaseLease attempts to release the given lease so that another process can
// grab it.
func (m *LeaseManager) ReleaseLease(ctx context.Context, l *Lease) error {
	return m.db.CPut(ctx, l.key, nil, l.val)
}
