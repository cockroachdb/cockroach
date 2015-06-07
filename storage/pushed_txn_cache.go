// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: yananzhi (zac.zhiyanan@gmail.com)

package storage

import (
	"sync"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/cache"

	gogoproto "github.com/gogo/protobuf/proto"
)

const (
	// The default size of the txn cache.
	defaultTxnCacheSize = 1 << 20
)

// If a txn modifies multiple keys in hot accessed data set,
// it may conflict with other txn simultaneously, there is no
// need to do a InternalPushTransaction RPC for each of the
// conflict keys modified by the same txn. This cache cached
// all the replies of InternalPushTransaction RPC, the subsequent
// InternalPushTransaction requests can check the cache, if
// the txn has already be pushed forward enough, it can resolve
// the intent directly.

// pushedTxnCache maintains txn which was pushed recently to avoid
// repushing further intents after a txn has already been aborted,
// comiitted or its timestamp moved forward.
type pushedTxnCache struct {

	//  protects txnCache for concurrent access
	sync.RWMutex

	// Use a LRU un-ordered cache, key is txn's ID, value is txn
	txnCache *cache.UnorderedCache
}

// newPushedTxnCache returns a new txn cache with the given maximum size.
func newPushedTxnCache(maxSize int) *pushedTxnCache {

	return &pushedTxnCache{
		txnCache: cache.NewUnorderedCache(cache.Config{
			Policy: cache.CacheLRU,
			ShouldEvict: func(n int, k, v interface{}) bool {
				return n > maxSize
			},
		}),
	}
}

// processPushedTxn return true if the cache txn can process a push txn request, and vice versa.
func (tc *pushedTxnCache) processPushedTxn(pushTxnReq *proto.InternalPushTxnRequest, pushTxnReply *proto.InternalPushTxnResponse) bool {

	tc.RLock()
	defer tc.RUnlock()

	value, ok := tc.txnCache.Get(string(pushTxnReq.PusheeTxn.ID))

	if !ok {
		return false
	}

	// Compare cached txn and pushee txn
	cachedTxn := value.(*proto.Transaction)
	if sufficientPushTxnRequest(cachedTxn, &pushTxnReq.PusheeTxn, pushTxnReq.PushType) {

		pushTxnReply.PusheeTxn = gogoproto.Clone(cachedTxn).(*proto.Transaction)
		pushTxnReply.Header().Txn = gogoproto.Clone(pushTxnReq.Header().Txn).(*proto.Transaction)
		pushTxnReply.Header().Timestamp = pushTxnReq.Header().Timestamp
		return true
	}

	return false
}

// update check and add a succeed pushed txn to the cache.
func (tc *pushedTxnCache) update(pushTxnReply *proto.InternalPushTxnResponse) {

	if pushTxnReply.GetError() != nil {
		return
	}

	pusheeTxn := pushTxnReply.PusheeTxn
	tc.Lock()
	defer tc.Unlock()

	value, ok := tc.txnCache.Get(string(pusheeTxn.ID))
	if !ok {
		tc.txnCache.Add(string(pusheeTxn.ID), gogoproto.Clone(pusheeTxn).(*proto.Transaction))
	} else {
		tc.updateCache(value.(*proto.Transaction), pusheeTxn)
	}

}

// updateCache return true is the cached txn was replace by the pushee txn
func (tc *pushedTxnCache) updateCache(cachedTxn *proto.Transaction, pusheeTxn *proto.Transaction) bool {

	replaced := shouldReplaceCache(cachedTxn, pusheeTxn)
	if replaced {
		newCachedTxn := gogoproto.Clone(pusheeTxn).(*proto.Transaction)
		tc.txnCache.Add(string(newCachedTxn.ID), newCachedTxn)
	}

	return replaced

}

// sufficientPushTxnRequest return true is the cached txn satisfy push txn request
func sufficientPushTxnRequest(cachedTxn *proto.Transaction, pusheeTxn *proto.Transaction, pushType proto.PushTxnType) bool {

	if cachedTxn.Status != proto.PENDING {
		return true
	}

	// Cached txn status is pending
	if pushType == proto.ABORT_TXN {
		return false
	}

	//  Push type is PUSH_TIMESTAMP, and cache txn's status is PENDING
	return pusheeTxn.Timestamp.Less(cachedTxn.Timestamp) &&
		pusheeTxn.Epoch == cachedTxn.Epoch &&
		pusheeTxn.Priority == cachedTxn.Priority
}

// shouldReplaceCache check pushed txn should replace
// the already cached txn
func shouldReplaceCache(cachedTxn *proto.Transaction, pusheeTxn *proto.Transaction) bool {
	if cachedTxn.Status != proto.PENDING {
		return false
	}

	// Cached txn status is pending
	if pusheeTxn.Status != proto.PENDING {
		return true
	}
	// Both status of cached txn and pushee txn are pending
	return cachedTxn.Timestamp.Less(pusheeTxn.Timestamp)

}
