// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// StoresIterator is the concrete implementation of
// kvserverbase.StoresIterator.
type StoresIterator Stores

var _ kvserverbase.StoresIterator = &StoresIterator{}

// MakeStoresIterator returns a new StoresIterator instance.
func MakeStoresIterator(stores *Stores) *StoresIterator {
	return (*StoresIterator)(stores)
}

// ForEachStore is part of kvserverbase.StoresIterator.
func (s *StoresIterator) ForEachStore(f func(kvserverbase.Store) error) error {
	var err error
	s.storeMap.Range(func(_ roachpb.StoreID, s *Store) bool {
		err = f((*baseStore)(s))
		return err == nil
	})
	return err
}

// baseStore is the concrete implementation of kvserverbase.Store.
type baseStore Store

var _ kvserverbase.Store = &baseStore{}

// StoreID is part of kvserverbase.Store.
func (s *baseStore) StoreID() roachpb.StoreID {
	store := (*Store)(s)
	return store.StoreID()
}

// Enqueue is part of kvserverbase.Store.
func (s *baseStore) Enqueue(
	ctx context.Context, queue string, rangeID roachpb.RangeID, skipShouldQueue bool,
) error {
	store := (*Store)(s)
	repl, err := store.GetReplica(rangeID)
	if err != nil {
		return err
	}

	processErr, enqueueErr := store.Enqueue(ctx, queue, repl, skipShouldQueue, false /* async */)
	if processErr != nil {
		return processErr
	}
	if enqueueErr != nil {
		return enqueueErr
	}
	return nil
}

// SetQueueActive is part of kvserverbase.Store.
func (s *baseStore) SetQueueActive(active bool, queue string) error {
	store := (*Store)(s)
	var kvQueue replicaQueue
	for _, rq := range store.scanner.queues {
		if strings.EqualFold(rq.Name(), queue) {
			kvQueue = rq
			break
		}
	}

	if kvQueue == nil {
		return errors.Errorf("unknown queue %q", queue)
	}

	kvQueue.SetDisabled(!active)
	return nil
}

// GetReplicaMutexForTesting is part of kvserverbase.Store.
func (s *baseStore) GetReplicaMutexForTesting(rangeID roachpb.RangeID) *syncutil.RWMutex {
	store := (*Store)(s)
	if repl := store.GetReplicaIfExists(rangeID); repl != nil {
		return (*syncutil.RWMutex)(repl.GetMutexForTesting())
	}
	return nil
}
