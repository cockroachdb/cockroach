// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgadvisory

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/internal/client/leasemanager"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

type LockManager interface {
	AcquireShared(ctx context.Context, txn *client.Txn, key []byte) (leasemanager.Lease, error)
	AcquireExclusive(ctx context.Context, txn *client.Txn, key []byte) (leasemanager.Lease, error)
}

// leaseScope indicates the logical scope of a lease.
type leaseScope int

const (
	// sessionScoped indicates that a lease is session-scoped.
	sessionScoped = iota
	// txnScoped indicated that a lease is txn-scoped.
	txnScoped
)

type leaseWithRefCount struct {
	leasemanager.Lease
	scope    leaseScope
	refCount uint64
}

type Session struct {
	db      *client.DB
	manager LockManager
	// txn is the last user txn that this Session was prepared for the next stmt
	// with.
	txn     *client.Txn
	leases  map[int64]*leaseWithRefCount
	scratch []byte
}

func NewSession(db *client.DB, manager LockManager) *Session {
	return &Session{
		db:      db,
		manager: manager,
		leases:  make(map[int64]*leaseWithRefCount),
	}
}

// LockEx acquires an exclusive session-scoped lease for key 'id'.
func (s *Session) LockEx(ctx context.Context, id int64) (hlc.Timestamp, error) {
	if lease, found := s.leases[id]; found {
		if lease.scope != sessionScoped {
			return hlc.Timestamp{}, errors.Errorf("txn-scoped lock already acquired")
		}
		if !lease.Exclusive() {
			return hlc.Timestamp{}, errors.Errorf("session-scoped ShareLock already acquired")
		}
		lease.refCount++
		return lease.StartTime(), nil
	}
	s.scratch = encoding.EncodeVarintAscending(s.scratch[:0], id)
	txn := s.db.NewTxn(ctx, fmt.Sprintf("session-lease-exclusive-%d", id))
	lease, err := s.manager.AcquireExclusive(ctx, txn, s.scratch)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	s.leases[id] = &leaseWithRefCount{Lease: lease, scope: sessionScoped, refCount: 1}
	return lease.StartTime(), nil
}

// UnlockEx releases an exclusive session-scoped lease for key 'id'.
func (s *Session) UnlockEx(ctx context.Context, id int64, ts hlc.Timestamp) (bool, error) {
	if lease, found := s.leases[id]; found {
		if lease.scope != sessionScoped || !lease.Exclusive() {
			return false, errors.Errorf("you don't own a lock of type ExclusiveLock")
		}
		lease.refCount--
		var err error
		if lease.refCount == 0 {
			lease.Txn().PushTo(ts)
			err = lease.Txn().Commit(ctx)
			delete(s.leases, id)
		}
		return true, err
	}
	return false, nil
}

// LockSh acquires a shared session-scoped lease for key 'id'.
func (s *Session) LockSh(ctx context.Context, id int64) (hlc.Timestamp, error) {
	if lease, found := s.leases[id]; found {
		if lease.scope != sessionScoped {
			return hlc.Timestamp{}, errors.Errorf("txn-scoped lock already acquired")
		}
		if lease.Exclusive() {
			return hlc.Timestamp{}, errors.Errorf("session-scoped ExclusiveLock already acquired")
		}
		lease.refCount++
		return lease.StartTime(), nil
	}
	s.scratch = encoding.EncodeVarintAscending(s.scratch[:0], id)
	txn := s.db.NewTxn(ctx, fmt.Sprintf("session-lease-shared-%d", id))
	lease, err := s.manager.AcquireShared(ctx, txn, s.scratch)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	s.leases[id] = &leaseWithRefCount{Lease: lease, scope: sessionScoped, refCount: 1}
	return lease.StartTime(), nil
}

// UnlockSh releases a shared session-scoped lease for key 'id'.
func (s *Session) UnlockSh(ctx context.Context, id int64, ts hlc.Timestamp) (bool, error) {
	if lease, found := s.leases[id]; found {
		if lease.scope != sessionScoped || lease.Exclusive() {
			return false, errors.Errorf("you don't own a lock of type ShareLock")
		}
		lease.refCount--
		var err error
		if lease.refCount == 0 {
			lease.Txn().PushTo(ts)
			err = lease.Txn().Commit(ctx)
			delete(s.leases, id)
		}
		return true, err
	}
	return false, nil
}

// TxnLockEx acquires an exclusive txn-scoped lease for key 'id'. Note that
// there is no corresponding "unlock" method because such lease is released when
// txn commits or aborts.
func (s *Session) TxnLockEx(ctx context.Context, txn *client.Txn, id int64) error {
	if lease, found := s.leases[id]; found {
		if lease.scope != txnScoped {
			return errors.Errorf("session-scoped lock already acquired")
		}
		if !lease.Exclusive() {
			return errors.Errorf("txn-scoped ShareLock already acquired")
		}
		if lease.Txn().ID() == txn.ID() {
			lease.refCount++
			return nil
		}
	}
	s.scratch = encoding.EncodeVarintAscending(s.scratch[:0], id)
	lease, err := s.manager.AcquireExclusive(ctx, txn, s.scratch)
	if err != nil {
		return err
	}
	s.leases[id] = &leaseWithRefCount{Lease: lease, scope: txnScoped, refCount: 1}
	return nil
}

// TxnLockSh acquires a shared txn-scoped lease for key 'id'. Note that
// there is no corresponding "unlock" method because such lease is released when
// txn commits or aborts.
func (s *Session) TxnLockSh(ctx context.Context, txn *client.Txn, id int64) error {
	if lease, found := s.leases[id]; found {
		if lease.scope != txnScoped {
			return errors.Errorf("session-scoped lease already acquired")
		}
		if lease.Exclusive() {
			return errors.Errorf("txn-scoped ExclusiveLock already acquired")
		}
		lease.refCount++
		return nil
	}
	s.scratch = encoding.EncodeVarintAscending(s.scratch[:0], id)
	lease, err := s.manager.AcquireShared(ctx, txn, s.scratch)
	if err != nil {
		return err
	}
	s.leases[id] = &leaseWithRefCount{Lease: lease, scope: txnScoped, refCount: 1}
	return nil
}

// PrepareForNextStmt prepares the session for execution of the next SQL
// statement. It takes in a current user txn and pushes all session-scoped
// leases to the "write" timestamp of that txn. Also, all txn-scoped leases are
// removed if the passed-in txn is different from the txn on the previous stmt.
func (s *Session) PrepareForNextStmt(txn *client.Txn) {
	if s.txn == nil || s.txn.ID() != txn.ID() {
		for id, lease := range s.leases {
			if lease.scope == txnScoped {
				delete(s.leases, id)
			}
		}
	}
	s.txn = txn
	pcts := txn.ProvisionalCommitTimestamp()
	for _, lease := range s.leases {
		if lease.scope == sessionScoped {
			lease.Txn().PushTo(pcts)
		}
	}
}

// Close closes the session. It goes through all of the session-scoped leases
// and commits the corresponding to those leases txns (which were created by
// this session to create the leases in the first place).
func (s *Session) Close(ctx context.Context) error {
	var resErr error
	for _, lease := range s.leases {
		if lease.scope == sessionScoped {
			if err := lease.Txn().Commit(ctx); err != nil && resErr == nil {
				resErr = err
			}
		}
	}
	return resErr
}
