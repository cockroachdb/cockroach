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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/go-errors/errors"
)

type Lock interface {
	Txn() *client.Txn
	Key() []byte
	Exclusive() bool
}
type LockManager interface {
	AcquireSh(ctx context.Context, txn *client.Txn, key []byte) (Lock, error)
	AcquireEx(ctx context.Context, txn *client.Txn, key []byte) (Lock, error)
}

// lockScope indicates the logical scope of a lock.
type lockScope int

const (
	// sessionScoped indicates that a lock is session-scoped.
	sessionScoped = iota
	// txnScoped indicated that a lock is txn-scoped.
	txnScoped
)

type lockWithRefCount struct {
	Lock
	scope    lockScope
	refCount uint64
}

type Session struct {
	sync.Mutex

	db      *client.DB
	manager LockManager
	locks   map[int64]*lockWithRefCount
	scratch []byte
}

// LockEx acquires an exclusive session-scoped lock for key 'id'.
func (s *Session) LockEx(ctx context.Context, id int64) error {
	s.Lock()
	defer s.Unlock()
	if lock, found := s.locks[id]; found {
		if lock.scope != sessionScoped {
			return errors.Errorf("txn scoped lock already acquired for id %d", id)
		}
		lock.refCount++
		return nil
	}
	s.scratch = encoding.EncodeVarintAscending(s.scratch[:0], id)
	txn := s.db.NewTxn(ctx, fmt.Sprintf("session-lock-exclusive-%d", id))
	lock, err := s.manager.AcquireEx(ctx, txn, s.scratch)
	if err != nil {
		return err
	}
	s.locks[id] = &lockWithRefCount{Lock: lock, scope: sessionScoped, refCount: 1}
	return nil
}

// UnlockEx releases an exclusive session-scoped lock for key 'id'.
func (s *Session) UnlockEx(_ context.Context, id int64) bool {
	s.Lock()
	defer s.Unlock()
	if lock, found := s.locks[id]; found {
		lock.refCount--
		if lock.refCount == 0 {
			delete(s.locks, id)
		}
		return true
	}
	return false
}

// LockSh acquires a shared session-scoped lock for key 'id'.
func (s *Session) LockSh(ctx context.Context, id int64) error {
	s.Lock()
	defer s.Unlock()
	if lock, found := s.locks[id]; found {
		if lock.scope != sessionScoped {
			return errors.Errorf("txn scoped lock already acquired for id %d", id)
		}
		lock.refCount++
		return nil
	}
	s.scratch = encoding.EncodeVarintAscending(s.scratch[:0], id)
	txn := s.db.NewTxn(ctx, fmt.Sprintf("session-lock-shared-%d", id))
	lock, err := s.manager.AcquireSh(ctx, txn, s.scratch)
	if err != nil {
		return err
	}
	s.locks[id] = &lockWithRefCount{Lock: lock, scope: sessionScoped, refCount: 1}
	return nil
}

// UnlockSh releases a shared session-scoped lock for key 'id'.
func (s *Session) UnlockSh(_ context.Context, id int64) bool {
	s.Lock()
	defer s.Unlock()
	if lock, found := s.locks[id]; found {
		lock.refCount--
		if lock.refCount == 0 {
			delete(s.locks, id)
		}
		return true
	}
	return false
}

// TxnLockEx acquires an exclusive txn-scoped lock for key 'id'. Note that
// there is no corresponding "unlock" method because such lock is released when
// txn commits or aborts.
func (s *Session) TxnLockEx(ctx context.Context, txn *client.Txn, id int64) error {
	s.Lock()
	defer s.Unlock()
	if lock, found := s.locks[id]; found {
		if lock.scope != txnScoped {
			return errors.Errorf("session scoped lock already acquired for id %d", id)
		}
		if lock.Txn().ID() == txn.ID() {
			lock.refCount++
			return nil
		}
	}
	s.scratch = encoding.EncodeVarintAscending(s.scratch[:0], id)
	lock, err := s.manager.AcquireEx(ctx, txn, s.scratch)
	if err != nil {
		return err
	}
	s.locks[id] = &lockWithRefCount{Lock: lock, scope: txnScoped, refCount: 1}
	return nil
}

// TxnLockSh acquires a shared txn-scoped lock for key 'id'. Note that
// there is no corresponding "unlock" method because such lock is released when
// txn commits or aborts.
func (s *Session) TxnLockSh(ctx context.Context, txn *client.Txn, id int64) error {
	s.Lock()
	defer s.Unlock()
	if lock, found := s.locks[id]; found {
		if lock.scope != txnScoped {
			return errors.Errorf("session scoped lock already acquired for id %d", id)
		}
		if lock.Txn().ID() == txn.ID() || !lock.Exclusive() {
			lock.refCount++
			return nil
		}
	}
	s.scratch = encoding.EncodeVarintAscending(s.scratch[:0], id)
	lock, err := s.manager.AcquireSh(ctx, txn, s.scratch)
	if err != nil {
		return err
	}
	s.locks[id] = &lockWithRefCount{Lock: lock, scope: txnScoped, refCount: 1}
	return nil
}
