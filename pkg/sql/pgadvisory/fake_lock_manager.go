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
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

type FakeLock struct {
	exclusive map[uuid.UUID]*client.Txn
	shared    map[uuid.UUID]*client.Txn
	key       []byte
	mu        sync.RWMutex
}

type FakeLockManager struct {
	locks map[string]*FakeLock
}

func (fm *FakeLockManager) Start(ctx context.Context, stopper *stop.Stopper) error {
	fm.locks = make(map[string]*FakeLock)
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
	return nil
}

func (l *FakeLock) Key() []byte {
	return l.key
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
			key:       key,
		}
	}
	return storedLock
}

func (fm *FakeLockManager) AcquireEx(txn *client.Txn, key []byte) error {
	storedLock := fm.upsertLock(txn, key)
	storedLock.exclusive[txn.ID()] = txn
	storedLock.mu.Lock()
	return nil
}

func (fm *FakeLockManager) AcquireSh(txn *client.Txn, key []byte) error {
	storedLock := fm.upsertLock(txn, key)
	storedLock.shared[txn.ID()] = txn
	storedLock.mu.RLock()
	return nil
}
