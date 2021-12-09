// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package loqrecovery

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
)

type storeIDSet map[roachpb.StoreID]struct{}

// storeListFromSet unwraps map to a sorted list of StoreIDs.
func storeListFromSet(set storeIDSet) []roachpb.StoreID {
	storeIDs := make([]roachpb.StoreID, 0, len(set))
	for k := range set {
		storeIDs = append(storeIDs, k)
	}
	sort.Slice(storeIDs, func(i, j int) bool {
		return storeIDs[i] < storeIDs[j]
	})
	return storeIDs
}

// Make a string of stores 'set' in ascending order.
func joinStoreIDs(storeIDs storeIDSet) string {
	storeNames := make([]string, 0, len(storeIDs))
	for _, id := range storeListFromSet(storeIDs) {
		storeNames = append(storeNames, fmt.Sprintf("s%d", id))
	}
	return strings.Join(storeNames, ", ")
}

// UpdatableStore is a wrapper around storage.Engine providing ability
// to lazily create batches and perform lifecycle management of the store
// and the batch.
type UpdatableStore struct {
	storage               storage.Engine
	batch                 storage.Batch
	nextUpdateRecordIndex uint64
}

// NewUpdatableStore creates new UpdatableStore from storage.Engine
func NewUpdatableStore(storage storage.Engine) (*UpdatableStore, error) {
	ind, err := findFirstAvailableRecoveryEventIndex(storage)
	if err != nil {
		return nil, errors.Wrap(err, "failed accessing update evidence records in store")
	}
	return &UpdatableStore{storage: storage, nextUpdateRecordIndex: ind}, nil
}

// GetStoreID reads roachpb.StoreIdent from underlying storage.
// Hides underlying operation details from loqrecovery users.
func (u *UpdatableStore) GetStoreID(ctx context.Context) (roachpb.StoreIdent, error) {
	return kvserver.ReadStoreIdent(ctx, u.storage)
}

// Batch creates or returns existing batch to update storage.
func (u *UpdatableStore) Batch() storage.Batch {
	if u.batch == nil {
		u.batch = u.storage.NewBatch()
	}
	return u.batch
}

// commit commits pending changes if available, otherwise does nothing.
// returns true, nil if any changes were written
// returns false, nil if store had no changes pending
// returns false, err if pending changes failed to commit due to underlying
// store problems.
func (u *UpdatableStore) commit() (bool, error) {
	if u.batch == nil || u.batch.Empty() {
		return false, nil
	}
	if err := u.batch.Commit(true); err != nil {
		return false, err
	}
	return true, nil
}

// Close closes storage and corresponding update batch if it was
// open.
func (u *UpdatableStore) Close() {
	if u.batch != nil {
		u.batch.Close()
	}
	u.storage.Close()
}
