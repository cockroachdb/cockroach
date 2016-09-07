// Copyright 2014 The Cockroach Authors.
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
//
// Author: Spencer Kimball (spencer@cockroachlabs.com)

package storage

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

type TxnCache struct {
	rangeID roachpb.RangeID
}

// NewTxnCache returns a new txn cache. Every range replica
// maintains an txn cache, not just the lease holder.
func NewTxnCache(rangeID roachpb.RangeID) *TxnCache {
	return &TxnCache{
		rangeID: rangeID,
	}
}

func (tc *TxnCache) min() roachpb.Key {
	return keys.TxnCacheKey(tc.rangeID, txnIDMin)
}

func (tc *TxnCache) max() roachpb.Key {
	return keys.TxnCacheKey(tc.rangeID, txnIDMax)
}

// ClearData removes all persisted items stored in the cache.
func (tc *TxnCache) ClearData(e engine.Engine) error {
	b := e.NewBatch()
	defer b.Close()
	_, err := engine.ClearRange(b, engine.MakeMVCCMetadataKey(tc.min()), engine.MakeMVCCMetadataKey(tc.max()))
	if err != nil {
		return err
	}
	return b.Commit()
}

// Get looks up an txn cache entry recorded for this transaction ID.
// Returns whether an txn record was found and any error.
func (tc *TxnCache) Get(ctx context.Context, e engine.Reader, txnID *uuid.UUID) (bool, error) {
	if txnID == nil {
		return false, errEmptyTxnID
	}
	// Pull response from disk and read into reply if available.
	key := keys.TxnCacheKey(tc.rangeID, txnID)
	val, _, err := engine.MVCCGet(ctx, e, key, hlc.ZeroTimestamp, true /* consistent */, nil /* txn */)
	return val != nil, err
}

// Put writes an entry for the specified transaction ID.
func (tc *TxnCache) Put(
	ctx context.Context,
	e engine.ReadWriter,
	ms *enginepb.MVCCStats,
	txnID *uuid.UUID,
) error {
	if txnID == nil {
		return errEmptyTxnID
	}
	key := keys.TxnCacheKey(tc.rangeID, txnID)
	value := roachpb.Value{}
	return engine.MVCCPut(ctx, e, ms, key, hlc.ZeroTimestamp, value, nil)
}
