// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"strings"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// StoresIterator is the concrete implementation of
// kvserverbase.StoresIterator.
type StoresIterator struct {
	stores *Stores
}

var _ kvserverbase.StoresIterator = &StoresIterator{}

// MakeStoresIterator returns a new StoresIterator instance.
func MakeStoresIterator(stores *Stores) StoresIterator {
	return StoresIterator{stores}
}

// ForEachStore is part of kvserverbase.StoresIterator.
func (s StoresIterator) ForEachStore(f func(kvserverbase.Store) error) error {
	var err error
	s.stores.storeMap.Range(func(k int64, v unsafe.Pointer) bool {
		store := (*Store)(v)

		err = f(baseStore{store})
		return err == nil
	})
	return err
}

// baseStore is the concrete implementation of kvserverbase.Store.
type baseStore struct {
	store *Store
}

var _ kvserverbase.Store = &baseStore{}

// StoreID is part of kvserverbase.Store.
func (s baseStore) StoreID() roachpb.StoreID {
	return s.store.StoreID()
}

// Enqueue is part of kvserverbase.Store.
func (s baseStore) Enqueue(
	ctx context.Context, queue string, rangeID roachpb.RangeID, skipShouldQueue bool,
) error {
	repl, err := s.store.GetReplica(rangeID)
	if err != nil {
		return err
	}

	_, processErr, enqueueErr := s.store.ManuallyEnqueue(ctx, queue, repl, skipShouldQueue)
	if processErr != nil {
		return processErr
	}
	if enqueueErr != nil {
		return enqueueErr
	}
	return nil
}

// SetQueueActive is part of kvserverbase.Store.
func (s baseStore) SetQueueActive(active bool, queue string) error {
	var kvQueue replicaQueue
	for _, rq := range s.store.scanner.queues {
		if strings.EqualFold(rq.Name(), queue) {
			kvQueue = rq
		}
	}

	if kvQueue == nil {
		return errors.Errorf("unknown queue %q", queue)
	}

	kvQueue.SetDisabled(!active)
	return nil
}
