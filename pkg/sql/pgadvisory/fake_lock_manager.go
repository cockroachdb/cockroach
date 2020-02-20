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
	"errors"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/internal/client/leasemanager"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

type FakeLock struct {
	exclusive   map[uuid.UUID]*client.Txn
	shared      map[uuid.UUID]*client.Txn
	key         []byte
	txn         *client.Txn
	mu          sync.RWMutex
	isExclusive bool
}

var _ leasemanager.Lease = &FakeLock{}

type FakeLockManager struct {
	locks map[string]*FakeLock
}

func NewFakeLockManager() *FakeLockManager {
	return &FakeLockManager{
		locks: make(map[string]*FakeLock),
	}
}

func (fm *FakeLockManager) Start(ctx context.Context, stopper *stop.Stopper) error {
	stopper.RunWorker(ctx, func(ctx context.Context) {
		for {
			select {
			case <-stopper.ShouldStop():
				return
			case <-time.After(50 * time.Millisecond):
				fm.unlockFinalized()
			}
		}
	})
	return nil
}

func (l *FakeLock) Txn() *client.Txn {
	return l.txn
}

func (l *FakeLock) Key() []byte {
	return l.key
}

func (l *FakeLock) Exclusive() bool {
	return l.isExclusive
}

func (l *FakeLock) GetExpiration() hlc.Timestamp {
	return hlc.Timestamp{}
}

func (l *FakeLock) StartTime() hlc.Timestamp {
	return hlc.Timestamp{}
}

func (fm *FakeLockManager) unlockFinalized() {
	for _, lock := range fm.locks {
		for _, txn := range lock.exclusive {
			if txn.Sender().TxnStatus().IsFinalized() {
				lock.mu.Unlock()
			}
		}
		for _, txn := range lock.shared {
			if txn.Sender().TxnStatus().IsFinalized() {
				lock.mu.RUnlock()
			}
		}
	}

}

func (fm *FakeLockManager) upsertLock(txn *client.Txn, key []byte) *FakeLock {
	strKey := string(key)
	storedLock, found := fm.locks[strKey]
	if !found {
		storedLock = &FakeLock{
			exclusive: make(map[uuid.UUID]*client.Txn),
			shared:    make(map[uuid.UUID]*client.Txn),
			mu:        sync.RWMutex{},
		}
	}
	storedLock.txn = txn
	storedLock.key = key
	return storedLock
}

func (fm *FakeLockManager) AcquireExclusive(
	ctx context.Context, txn *client.Txn, key []byte,
) (leasemanager.Lease, error) {
	storedLock := fm.upsertLock(txn, key)
	storedLock.exclusive[txn.ID()] = txn
	storedLock.isExclusive = true
	storedLock.mu.Lock()
	return storedLock, nil
}

func (fm *FakeLockManager) AcquireShared(
	ctx context.Context, txn *client.Txn, key []byte,
) (leasemanager.Lease, error) {
	storedLock := fm.upsertLock(txn, key)
	storedLock.shared[txn.ID()] = txn
	storedLock.mu.RLock()
	return storedLock, nil
}

func (fm *FakeLockManager) TryAcquireEx(
	ctx context.Context, txn *client.Txn, key []byte,
) (lock leasemanager.Lease, err error) {
	group := ctxgroup.WithContext(context.Background())
	group.Go(func() error {
		lock, err = fm.AcquireExclusive(ctx, txn, key)
		return nil
	})
	group.Go(func() error {
		<-time.After(time.Millisecond)
		return errors.New("could not acquire lock")
	})
	if waitErr := group.Wait(); waitErr != nil {
		return nil, waitErr
	}
	return
}
