// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

type intentInterleavingIterConstraint int8

const (
	notConstrained intentInterleavingIterConstraint = iota
	constrainedToLocal
	constrainedToGlobal
)

// commitMap maintains a logical map of TxnIDs of transactions that were
// "simple" in their behavior, and have committed, where simple is defined as
// all the following conditions:
// - No savepoint rollbacks: len(intent.IgnoredSeqNums)==0
// - Single epoch, i.e., TxnMeta.Epoch==0
// - Never pushed, i.e., TxnMeta.MinTimestamp==TxnMeta.WriteTimestamp
// For such transactions, their provisional values can be considered committed
// with the current version and local timestamp, i.e., we need no additional
// information about the txn other than the TxnID.
//
// Adding to commitMap:
// The earliest a txn can be added to the commitMap is when the transaction is
// in STAGING and has verified all the conditions for commit. That is, this
// can be done before the transition to COMMITTED is replicated. For the node
// that has the leaseholder of the range containing the txn record, this
// requires no external communication. For other nodes with intents for the
// txn, one could piggyback this information on the RPCs for async intent
// resolution, and add to the the commitMap before doing the intent resolution
// -- this piggybacking would incur 2 consensus rounds of contention. If we
// are willing to send the RPC earlier, it will be contention for 1 consensus
// round only. Note that these RPCs should also remove locks from the
// non-persistent lock table data-structure so should send information about
// the keys (like in a LockUpdate but won't remove the durable lock state).
//
// Removal from commitMap:
// The commitMap can be considered a cache of TxnIDs. It is helpful to have a
// txn in the cache until its intents have been resolved. Additionally,
// latchless intent resolution must pin a txn in the map before it releases
// latches and unpin when the intent resolution has been applied on the
// leaseholder. This pinning behavior is needed to ensure the correctness of
// the in-memory concurrency.lockTable, which must maintain the property that
// the replicated locks known to it are a subset of the persistent replicated
// locks. We are assuming here that there is no lockTable on followers.
//
// commitMap per range or node?:
// Scoping it to a range may be simpler, and result in smaller maps that are
// more efficient (see synchronization section below). But the same TxnID can
// be in multiple maps, so there is the overhead of duplication.
//
// Synchronization:
// An ideal data-structure would offer very efficient O(1) lookup without the
// need to acquire a mutex. So a copy-on-write approach would be ideal for
// reads. We also need to support a high rate of insertions. Removal can be
// batched, say by removing everything added > 50ms in the past.
//
// TODO: the current prototype focuses on changes to the read-path so just use
// a map.
type commitMap struct {
	simpleCommits map[uuid.UUID]struct{}
}

func (cm *commitMap) isPresent(txnID uuid.UUID) bool {
	if cm == nil || cm.simpleCommits == nil {
		return false
	}
	_, present := cm.simpleCommits[txnID]
	return present
}

// Multiple intents for a key:
//
// We allow a key (with multiple versions) to have multiple intents, under the
// condition that at most one of the intents is uncommitted.
// Additionally:
// - We don't want to have to coordinate intent resolution of these multiple
//   intents, by mandating that the resolution happen in any particular order
//
// -  We want to guarantee that even if the commitMap is cleared (since it is a
//   cache), we can maintain the invariant that a caller iterating over a key
//   sees at most one intent. As we will illustrate below, providing this
//   guarantee requires us to limit the commitMap to only contain
//   simple-committed txns.
//
// Consider a key with timestamps t5, t4, t3, t2, t1 in decreasing order and
// intents for t5, t4, t2, with corresponding txns txn5, ... txn1. We consider
// the disposition of an intent to be either unknown or simple-committed. In
// this example, the disposition of the intent for t4 and t2 is
// simple-committed solely based on the fact that there is at least one
// version (provisional or otherwise) more recent that the timestamp of the
// intent. That is, at most one intent, the one for t5, has a disposition that
// needs to rely on knowledge that is not self-contained in the history. For
// t5, we must rely on the commitMap to decide whether is unknown or
// simple-committed. It is possible that some user of the
// intentInterleavingIter saw t5 as simple-committed and a later user sees it
// as unknown disposition, if the txn5 got removed from the commitMap -- such
// regression is harmless since the latter user will simply have to do intent
// resolution. Note that the intent for t5 could get resolved before those for
// t4 and t2, and that is also fine since the disposition of t4, t2 stays
// simple-committed. If txn5 is aborted and the intent for t5 removed, and
// txn4 is no longer in the commitMap, the disposition of t4 could change to
// unknown. This is also acceptable, since t5 was only serving as a "local
// promise" that t4 was committed, which is simply an optimization. There is
// still a less efficient globally available "promise" that t4 is committed,
// and intent resolution of t4 is how we will enact that promise.
//
// Maintaining the above guarantees requires that historical versions must not
// be garbage collected without resolving intents. This is acceptable since GC
// is not latency sensitive.

// TODO: don't forget to fix the iteration for intent resolution in mvcc.go

// intentInterleavingIter makes physically separated intents appear as
// logically interleaved. It assumes that:
// - There are no physically interleaved intents.
// - An intent must have a corresponding value (possibly provisional).
// - The only single key locks in the lock table key space are intents.
//
//
// TODO: update following comment since we now have multiple intents and we are
// disallowing physically interleaved intents.
//
// Semantically, the functionality is equivalent to merging two MVCCIterators:
// - A MVCCIterator on the MVCC key space.
// - A MVCCIterator constructed by wrapping an EngineIterator on the lock table
//   key space where the EngineKey is transformed into the corresponding
//   intent key and appears as MVCCKey{Key: intentIterKey}.
// The implementation below is specialized to reduce unnecessary comparisons
// and iteration, by utilizing the aforementioned assumptions. The intentIter
// iterates over the lock table key space and iter over the MVCC key space.
// They are kept synchronized in the following way (for forward iteration):
// - At the same MVCCKey.Key: the intentIter is at the intent and iter at the
//   provisional value.
// - At different MVCCKey.Keys: the intentIter is ahead of iter, at the first
//   key after iter's MVCCKey.Key that has a separated intent.
// Note that in both cases the iterators are apart by the minimal possible
// distance. This minimal distance rule applies for reverse iteration too, and
// can be used to construct similar invariants.
// The one exception to the minimal distance rule is a sub-case of prefix
// iteration, when we know that no separated intents need to be seen, and so
// don't bother positioning intentIter.
//
// The implementation of intentInterleavingIter assumes callers iterating
// forward (reverse) are setting an upper (lower) bound. There is protection
// for misbehavior by the callers that don't set such bounds, by manufacturing
// bounds. These manufactured bounds prevent the lock table iterator from
// leaving the lock table key space. We also need to manufacture bounds for
// the MVCCIterator to prevent it from iterating into the lock table. Note
// that any manufactured bounds for both the lock table iterator and
// MVCCIterator must be consistent since the intentInterleavingIter does not
// like to see a lock with no corresponding provisional value (it will
// consider than an error). Manufacturing of bounds is complicated by the fact
// that the MVCC key space is split into two spans: local keys preceding the
// lock table key space, and global keys. To manufacture a bound, we need to
// know whether the caller plans to iterate over local or global keys. Setting
// aside prefix iteration, which doesn't need any of these manufactured
// bounds, the call to newIntentInterleavingIter must have specified at least
// one of the lower or upper bound. We use that to "constrain" the iterator as
// either a local key iterator or global key iterator and panic if a caller
// violates that in a subsequent SeekGE/SeekLT/SetUpperBound call.
type intentInterleavingIter struct {
	prefix     bool
	constraint intentInterleavingIterConstraint

	// iter is for iterating over MVCC keys.
	iter MVCCIterator
	// The valid value from iter.Valid() after the last positioning call.
	iterValid bool
	// When iterValid = true, this contains the result of iter.UnsafeKey(). We
	// store it here to avoid repeatedly calling UnsafeKey() since it repeats
	// key parsing.
	iterKey MVCCKey

	// intentIter is for iterating over separated intents, so that
	// intentInterleavingIter can make them look as if they were interleaved.
	//
	// If we observe an intent whose decoded key is equal to the current iter
	// key, we need to iterate over all such intents, and
	// - check whether the txnID is in the commitMap.
	// - decode the MVCCMetadata and compare the MVCCMetadata.Timestamps. The only
	//   potential unknown disposition MVCCMetadata is the one with the highest
	//   timestamp. Its disposition will be determined using the commitMap and
	//   based on whether there are more recent committed versions.
	intentIter EngineIterator
	// The validity state of intentIter.
	intentIterState pebble.IterValidityState
	// TODO(sumeer): change MVCCIterator interface so that caller can use what
	// the intentInterleavingIter has parsed and avoid parsing again.
	//
	// Parsing cost: consider the following cases
	// - No contention: reads and writes that are using an
	//   intentInterleavingIter will not see intents, and so there is no parsing
	//   of MVCCMetadata.
	// - Read-write contention: intentInterleavingIter will see a single intent
	//   and parse it. It either has simple-committed or unknown disposition.
	//   Prior to supporting simple-committed disposition, the caller would have
	//   parsed the intent, so there is no additional cost (assuming we avoid
	//   repeated parsing, as mentioned above). With simple-committed disposition,
	//   the intentInterleavingIter will parse and discard, which is fine -- we
	//   have avoided false contention.
	// - Write-write contention: intentInterleavingIter could see multiple
	//   intents and it has to parse them all. We claim that the saved false
	//   contention justifies this cost.
	//
	// mvccMetadata, when non-nil, represents an unknown disposition intent that
	// is the current position of the interleaving iter. That is, this is the
	// intent corresponding to the highest version and is not in the commitMap.
	//
	// mvccMetadata is populated by iterating over all the intents corresponding
	// to a key.
	// - For forward iteration this is done when the intent key and the version
	//   key, i.e., intentIterKey and iterKey become equal. As a result of this
	//   iteration, the intentIter is moved past all the intents for that key
	//   (see numIntents for how many intents were encountered).
	// - For reverse iteration this is done when iterating past the latest
	//   provisional value. The intentIter will also move past all the intents
	//   for that key (in the reverse direction).
	// Note that both cases share the similarity that mvccMetadata is being
	// populated when, if non-nil, this intent has to be exposed now to the
	// caller of intentInterleavingIter.
	mvccMetadata *enginepb.MVCCMetadata
	// The number of intents seen in the last call to tryInitIntentState. This
	// count is used to decide how many times we need to step the intentIter
	// when switching from forward to reverse iteration and vice versa, and the
	// interleavingIter was currently positioned at the intent.
	numIntents int
	// intentKey is the key for the corresponding mvccMetadata. It is not the
	// same as intentIterKey since the intentIter has been moved past all the
	// intents for this key.
	intentKey roachpb.Key
	// Parsing buffers. If mvccMetadata != nil, it points to one of the entries
	// in metadataBuf.
	metadataBuf [2]enginepb.MVCCMetadata
	// largestVersionSeen is used in reverse iteration so that we can determine
	// whether an intent is older than another version and therefore
	// simple-committed.
	largestVersionSeen hlc.Timestamp
	// The last limit key used in *WithLimit calls. We remember it so that it
	// can be used later for iteration over the lock table, when consuming all
	// the intents for a key.
	lastLimitKey roachpb.Key

	// The decoded key from the lock table. This is an unsafe key
	// in that it is only valid when intentIter has not been
	// repositioned. It is nil if the intentIter is considered to be
	// exhausted. Note that the intentIter may still be positioned
	// at a valid position in the case of prefix iteration, but the
	// intentIterKey may be set to nil to override that state.
	intentIterKey                 roachpb.Key
	intentKeyAsNoTimestampMVCCKey []byte

	// intentCmp is used to represent the relationship of an intent key with
	// the current iter key. When mvccMetadata is non-nil, the intent key is
	// intentKey:
	// - Forward direction: since there are versions corresponding to the
	//   mvccMetadata, this will always be 0.
	// - Reverse direction: since iter has been moved before the intent, this
	//   will always be +1.
	// By definition, valid=true in this case.
	//
	// The common case is mvccMetadata is nil, i.e., the interleaving iter is not
	// positioned at an intent. In that case, this is defined as:
	// - cmp output of (intentIterKey, current iter key) when both are valid.
	//   This does not take timestamps into consideration. So if intentIter
	//   is at an intent, and iter is at the corresponding provisional value,
	//   cmp will be 0. See the longer struct-level comment for more on the
	//   relative positioning of intentIter and iter.
	// - intentIterKey==nil, iterValid==true, cmp=dir
	//   (i.e., the nil key is akin to infinity in the forward direction
	//   and -infinity in the reverse direction, since that iterator is
	//   exhausted).
	// - intentIterKey!=nil, iterValid=false, cmp=-dir.
	// - If both are invalid. cmp is undefined and valid=false.
	intentCmp int
	// The current direction. +1 for forward, -1 for reverse.
	dir   int
	valid bool
	err   error

	// Buffers to reuse memory when constructing lock table keys for bounds and
	// seeks.
	intentSeekKeyBuf  []byte
	intentLimitKeyBuf []byte

	// Assumption: commitMap is akin to a snapshot, i.e., will not lose entries
	// while intentInterleavingIter is using it. Gaining new entries is
	// harmless.
	commitMap *commitMap
}

var _ MVCCIterator = &intentInterleavingIter{}

var intentInterleavingIterPool = sync.Pool{
	New: func() interface{} {
		return &intentInterleavingIter{}
	},
}

func isLocal(k roachpb.Key) bool {
	return len(k) == 0 || keys.IsLocal(k)
}

func newIntentInterleavingIterator(reader Reader, opts IterOptions) MVCCIterator {
	if !opts.MinTimestampHint.IsEmpty() || !opts.MaxTimestampHint.IsEmpty() {
		panic("intentInterleavingIter must not be used with timestamp hints")
	}
	var lowerIsLocal, upperIsLocal bool
	var constraint intentInterleavingIterConstraint
	if opts.LowerBound != nil {
		lowerIsLocal = isLocal(opts.LowerBound)
		if lowerIsLocal {
			constraint = constrainedToLocal
		} else {
			constraint = constrainedToGlobal
		}
	}
	if opts.UpperBound != nil {
		upperIsLocal = isLocal(opts.UpperBound) || bytes.Equal(opts.UpperBound, keys.LocalMax)
		if opts.LowerBound != nil && lowerIsLocal != upperIsLocal {
			panic(fmt.Sprintf(
				"intentInterleavingIter cannot span from lowerIsLocal %t, %s to upperIsLocal %t, %s",
				lowerIsLocal, opts.LowerBound.String(), upperIsLocal, opts.UpperBound.String()))
		}
		if upperIsLocal {
			constraint = constrainedToLocal
		} else {
			constraint = constrainedToGlobal
		}
	}
	if !opts.Prefix {
		if opts.LowerBound == nil && opts.UpperBound == nil {
			// This is the same requirement as pebbleIterator.
			panic("iterator must set prefix or upper bound or lower bound")
		}
		// At least one bound is specified, so constraint != notConstrained. But
		// may need to manufacture a bound for the currently unbounded side.
		if opts.LowerBound == nil && constraint == constrainedToGlobal {
			// Iterating over global keys, and need a lower-bound, to prevent the MVCCIterator
			// from iterating into the lock table.
			opts.LowerBound = keys.LocalMax
		}
		if opts.UpperBound == nil && constraint == constrainedToLocal {
			// Iterating over local keys, and need an upper-bound, to prevent the MVCCIterator
			// from iterating into the lock table.
			opts.UpperBound = keys.LocalRangeLockTablePrefix
		}
	}
	// Else prefix iteration, so do not need to manufacture bounds for both
	// iterators since the pebble.Iterator implementation will hide the keys
	// that do not match the prefix. Note that this is not equivalent to
	// constraint==notConstrained -- it is acceptable for a caller to specify a
	// bound for prefix iteration, though since they don't need to, most callers
	// don't.

	iiIter := intentInterleavingIterPool.Get().(*intentInterleavingIter)
	intentOpts := opts
	intentSeekKeyBuf := iiIter.intentSeekKeyBuf
	intentLimitKeyBuf := iiIter.intentLimitKeyBuf
	if opts.LowerBound != nil {
		intentOpts.LowerBound, intentSeekKeyBuf = keys.LockTableSingleKey(opts.LowerBound, intentSeekKeyBuf)
	} else if !opts.Prefix {
		// Make sure we don't step outside the lock table key space. Note that
		// this is the case where the lower bound was not set and
		// constrainedToLocal.
		intentOpts.LowerBound = keys.LockTableSingleKeyStart
	}
	if opts.UpperBound != nil {
		intentOpts.UpperBound, intentLimitKeyBuf =
			keys.LockTableSingleKey(opts.UpperBound, intentLimitKeyBuf)
	} else if !opts.Prefix {
		// Make sure we don't step outside the lock table key space. Note that
		// this is the case where the upper bound was not set and
		// constrainedToGlobal.
		intentOpts.UpperBound = keys.LockTableSingleKeyEnd
	}
	// Note that we can reuse intentKeyBuf, intentLimitKeyBuf after
	// NewEngineIterator returns.
	intentIter := reader.NewEngineIterator(intentOpts)

	// The creation of these iterators can race with concurrent mutations, which
	// may make them inconsistent with each other. So we clone here, to ensure
	// consistency (certain Reader implementations already ensure consistency,
	// and we use that when possible to save allocations).
	var iter MVCCIterator
	if reader.ConsistentIterators() {
		iter = reader.NewMVCCIterator(MVCCKeyIterKind, opts)
	} else {
		iter = newMVCCIteratorByCloningEngineIter(intentIter, opts)
	}

	*iiIter = intentInterleavingIter{
		prefix:            opts.Prefix,
		constraint:        constraint,
		iter:              iter,
		intentIter:        intentIter,
		intentKey:         iiIter.intentKey,
		intentSeekKeyBuf:  intentSeekKeyBuf,
		intentLimitKeyBuf: intentLimitKeyBuf,
	}
	return iiIter
}

// TODO(sumeer): the limits generated below are tight for the current value of
// i.iterKey.Key. And the semantics of the underlying *WithLimit methods in
// pebble.Iterator are best-effort, but the implementation is not. Consider
// strengthening the semantics and using the tightness of these limits to
// avoid comparisons between iterKey and intentKey.

// makeUpperLimitKey uses the current value of i.iterKey.Key (if
// i.iterValid=true and the iterator is not doing prefix iteration), to
// construct an exclusive upper limit roachpb.Key that will include the intent
// for i.iterKey.Key. If the above condition is not satisfied, the limit key
// is nil, i.e., no limit. The limit key is stored in i.lastLimitKey.
func (i *intentInterleavingIter) makeUpperLimitKey() {
	i.lastLimitKey = nil
	if !i.iterValid || i.prefix {
		return
	}
	key := i.iterKey.Key
	// The +2 is to account for the call to BytesNext and the need to append a
	// '\x00' in the implementation of the *WithLimit function. The rest is the
	// same as in the implementation of LockTableSingleKey. The BytesNext is to
	// construct the exclusive roachpb.Key as mentioned earlier. The
	// implementation of *WithLimit (in pebbleIterator), has to additionally
	// append '\x00' (the sentinel byte) to construct an encoded EngineKey with
	// an empty version.
	keyLen :=
		len(keys.LocalRangeLockTablePrefix) + len(keys.LockTableSingleKeyInfix) + len(key) + 3 + 2
	if cap(i.intentLimitKeyBuf) < keyLen {
		i.intentLimitKeyBuf = make([]byte, 0, keyLen)
	}
	_, i.intentLimitKeyBuf = keys.LockTableSingleKey(key, i.intentLimitKeyBuf)
	// To construct the exclusive limitKey, roachpb.BytesNext gives us a
	// tight limit. Since it appends \x00, this is not decodable, except at
	// the Pebble level, which is all we need here. We don't actually use
	// BytesNext since it tries not to overwrite the slice.
	i.intentLimitKeyBuf = append(i.intentLimitKeyBuf, '\x00')
	i.lastLimitKey = i.intentLimitKeyBuf
}

// makeLowerLimitKey uses the current value of i.iterKey.Key (if
// i.iterValid=true), to construct an inclusive lower limit roachpb.Key that
// will include the intent for i.iterKey.Key. If the above condition is not
// satisfied, the limit key is nil, i.e., no limit. The limit key is stored in
// i.lastLimitKey.
func (i *intentInterleavingIter) makeLowerLimitKey() {
	i.lastLimitKey = nil
	if !i.iterValid {
		return
	}
	key := i.iterKey.Key
	// The +1 is to account for the need to append a '\x00' in the
	// implementation of the *WithLimit function. The rest is the same as in the
	// implementation of LockTableSingleKey.  The implementation of *WithLimit
	// (in pebbleIterator), has to additionally append '\x00' (the sentinel
	// byte) to construct an encoded EngineKey with an empty version.
	keyLen :=
		len(keys.LocalRangeLockTablePrefix) + len(keys.LockTableSingleKeyInfix) + len(key) + 3 + 1
	if cap(i.intentLimitKeyBuf) < keyLen {
		i.intentLimitKeyBuf = make([]byte, 0, keyLen)
	}
	_, i.intentLimitKeyBuf = keys.LockTableSingleKey(key, i.intentLimitKeyBuf)
	i.lastLimitKey = i.intentLimitKeyBuf
}

func (i *intentInterleavingIter) SeekGE(key MVCCKey) {
	i.dir = +1
	i.valid = true
	i.err = nil
	i.mvccMetadata = nil

	if i.constraint != notConstrained {
		i.checkConstraint(key.Key, false)
	}
	i.iter.SeekGE(key)
	if err := i.tryDecodeKey(); err != nil {
		return
	}
	var intentSeekKey roachpb.Key
	if key.Timestamp.IsEmpty() {
		// Common case.
		intentSeekKey, i.intentSeekKeyBuf = keys.LockTableSingleKey(key.Key, i.intentSeekKeyBuf)
	} else if !i.prefix {
		// Seeking to a specific version, so go past the intent.
		// NB: the caller should only be doing this if it has observed the intent,
		// or knows there is no intent, since it may now be exposed to a
		// provisional value.
		intentSeekKey, i.intentSeekKeyBuf = keys.LockTableSingleKey(key.Key.Next(), i.intentSeekKeyBuf)
	} else {
		// Else seeking to a particular version and using prefix iteration,
		// so don't expect to ever see the intent. NB: intentSeekKey is nil.
		i.intentIterKey = nil
	}
	if intentSeekKey != nil {
		i.makeUpperLimitKey()
		iterState, err := i.intentIter.SeekEngineKeyGEWithLimit(
			EngineKey{Key: intentSeekKey}, i.lastLimitKey)
		if err = i.tryDecodeLockKey(iterState, err); err != nil {
			return
		}
	}
	i.computePos(false)
}

func (i *intentInterleavingIter) SeekIntentGE(key roachpb.Key, txnUUID uuid.UUID) {
	i.dir = +1
	i.valid = true
	i.err = nil
	i.mvccMetadata = nil

	if i.constraint != notConstrained {
		i.checkConstraint(key, false)
	}
	i.iter.SeekGE(MVCCKey{Key: key})
	if err := i.tryDecodeKey(); err != nil {
		return
	}
	var engineKey EngineKey
	engineKey, i.intentSeekKeyBuf = LockTableKey{
		Key:      key,
		Strength: lock.Exclusive,
		TxnUUID:  txnUUID[:],
	}.ToEngineKey(i.intentSeekKeyBuf)
	i.makeUpperLimitKey()
	iterState, err := i.intentIter.SeekEngineKeyGEWithLimit(engineKey, i.lastLimitKey)
	if err = i.tryDecodeLockKey(iterState, err); err != nil {
		return
	}
	i.computePos(false)
}

func (i *intentInterleavingIter) checkConstraint(k roachpb.Key, isExclusiveUpper bool) {
	kConstraint := constrainedToGlobal
	if isLocal(k) {
		if bytes.Compare(k, keys.LocalRangeLockTablePrefix) > 0 {
			panic(fmt.Sprintf("intentInterleavingIter cannot be used with invalid local keys %s",
				k.String()))
		}
		kConstraint = constrainedToLocal
	} else if isExclusiveUpper && bytes.Equal(k, keys.LocalMax) {
		kConstraint = constrainedToLocal
	}
	if kConstraint != i.constraint {
		panic(fmt.Sprintf(
			"iterator with constraint=%d is being used with key %s that has constraint=%d",
			i.constraint, k.String(), kConstraint))
	}
}

func (i *intentInterleavingIter) tryDecodeKey() error {
	i.iterValid, i.err = i.iter.Valid()
	if i.iterValid {
		i.iterKey = i.iter.UnsafeKey()
	}
	if i.err != nil {
		i.valid = false
	}
	return i.err
}

// Assumes that i.err != nil. And i.iterValid, i.iterKey, i.intentIterKey are
// up to date.
func (i *intentInterleavingIter) computePos(isSeekLT bool) {
	if !i.iterValid && i.intentIterKey == nil {
		i.valid = false
		return
	}
	// INVARIANT: i.iterValid || i.intentKey != nil
	if !i.iterValid {
		if i.dir == +1 {
			i.err = errors.Errorf("intent with no version")
			i.valid = false
			return
		}
		// i.dir == -1, i.e., reverse iteration.
		if isSeekLT {
			i.err = errors.Errorf("SeekLT cannot be used to skip the latest version and land on intent")
			i.valid = false
			return
		}
		i.intentCmp = +1
		i.tryInitIntentState()
		return
	}
	if i.intentIterKey == nil {
		i.intentCmp = i.dir
		return
	}
	// iterValid && intentIterKey != nil
	i.intentCmp = i.intentIterKey.Compare(i.iterKey.Key)
	if i.intentCmp <= 0 && i.dir == +1 {
		if i.intentCmp < 0 {
			i.err = errors.Errorf("intent with no version")
			i.valid = false
			return
		}
		i.tryInitIntentState()
	} else if i.intentCmp > 0 && i.dir == -1 {
		if isSeekLT {
			i.err = errors.Errorf("SeekLT cannot be used to skip the latest version and land on intent")
			i.valid = false
			return
		}
		i.tryInitIntentState()
	} else if isSeekLT {
		i.largestVersionSeen = i.iterKey.Timestamp
	}
}

func (i *intentInterleavingIter) tryInitIntentState() error {
	i.mvccMetadata = nil
	i.numIntents = 0
	i.intentKey = append(i.intentKey[:0], i.intentIterKey...)
	nextParseIndex := 0
	for {
		err := protoutil.Unmarshal(i.intentIter.UnsafeValue(), &i.metadataBuf[nextParseIndex])
		i.numIntents++
		if err != nil {
			i.err = err
			i.valid = false
			return err
		}
		meta := &i.metadataBuf[nextParseIndex]
		if i.mvccMetadata != nil {
			if i.mvccMetadata.Timestamp.Less(meta.Timestamp) {
				// Committed.
				i.mvccMetadata = nil
				if !i.commitMap.isPresent(meta.Txn.ID) {
					i.mvccMetadata = meta
					nextParseIndex = 1 - nextParseIndex
				}
			}
			// Else meta is committed.
		} else if !i.commitMap.isPresent(meta.Txn.ID) {
			i.mvccMetadata = meta
			nextParseIndex = 1 - nextParseIndex
		}
		var iterState pebble.IterValidityState
		if i.dir == +1 {
			iterState, err = i.intentIter.NextEngineKeyWithLimit(i.lastLimitKey)
		} else {
			iterState, err = i.intentIter.PrevEngineKeyWithLimit(i.lastLimitKey)
		}
		if err = i.tryDecodeLockKey(iterState, err); err != nil {
			return err
		}
		if i.intentIterKey == nil || !i.intentIterKey.Equal(i.intentKey) {
			break
		}
	}
	// Check that mvccMetadata is not simple-committed because of being an older
	// version.
	if i.mvccMetadata != nil &&
		((i.dir == +1 && i.mvccMetadata.Timestamp.ToTimestamp().Less(i.iterKey.Timestamp)) ||
			(i.dir == -1 && i.mvccMetadata.Timestamp.ToTimestamp().Less(i.largestVersionSeen))) {
		// Simple committed.
		i.mvccMetadata = nil
	}
	if i.mvccMetadata == nil {
		// tryInitIntentState was called in the following cases, both of which
		// need fixing of i.intentCmp, since we don't actually have an intent.
		// - i.dir == +1 and i.intentCmp == 0
		// - i.dir == -1 and i.intentCmp > 0
		if i.dir == +1 {
			// We know there are versioned value(s) with a key equal to all the
			// intents we have skipped over.
			i.intentCmp = +1
		} else {
			// Reverse iteration.
			if i.iterValid {
				i.intentCmp = i.intentIterKey.Compare(i.iterKey.Key)
			} else {
				// There shouldn't be anything more to read from intentIter too.
				if i.intentIterKey != nil {
					i.err = errors.Errorf("intent with no version")
					i.valid = false
					return i.err
				}
			}
		}
		if !i.iterValid && i.intentIterKey == nil {
			i.valid = false
		}
	} else {
		// We found one intent that should be seen by the caller.
		i.intentKeyAsNoTimestampMVCCKey = i.intentKey.Next()
	}

	return nil
}

// tryDecodeLockKey is called after intentIter is moved.
func (i *intentInterleavingIter) tryDecodeLockKey(
	iterState pebble.IterValidityState, err error,
) error {
	if err != nil {
		i.err = err
		i.valid = false
		return err
	}
	i.intentIterState = iterState
	if iterState != pebble.IterValid {
		// NB: this does not set i.valid = false, since this method does not care
		// about the state of i.iter, which may be valid. It is the caller's
		// responsibility to additionally use the state of i.iter to appropriately
		// set i.valid.
		i.intentIterKey = nil
		return nil
	}
	engineKey, err := i.intentIter.UnsafeEngineKey()
	if err != nil {
		i.err = err
		i.valid = false
		return err
	}
	if i.intentIterKey, err = keys.DecodeLockTableSingleKey(engineKey.Key); err != nil {
		i.err = err
		i.valid = false
		return err
	}
	return nil
}

func (i *intentInterleavingIter) Valid() (bool, error) {
	return i.valid, i.err
}

func (i *intentInterleavingIter) Next() {
	if i.err != nil {
		return
	}
	if i.dir < 0 {
		// Switching from reverse to forward iteration.
		isCurAtIntent := i.isCurAtIntentIter()
		i.dir = +1
		if !i.valid {
			// Both iterators are exhausted, since intentKey is synchronized with
			// intentIter for non-prefix iteration, so step both forward.
			i.valid = true
			i.iter.Next()
			if err := i.tryDecodeKey(); err != nil {
				return
			}
			i.makeUpperLimitKey()
			iterState, err := i.intentIter.NextEngineKeyWithLimit(i.lastLimitKey)
			if err = i.tryDecodeLockKey(iterState, err); err != nil {
				return
			}
			i.computePos(false)
			return
		}
		// At least one of the iterators is not exhausted.
		// Cases:
		// - isCurAtIntent:
		//   - iter is physically before intentKey, so need to step it once forward, so that is
		//     looks like how forward iteration looks when the interleaving iter is at an intent.
		//     intentIter needs to be stepped numIntents times.
		// - !isCurAtIntent: intentIter needs to be stepped once so that it looks like forward iteration
		//   when the interleaving iter is at a version.
		if isCurAtIntent {
			// iter precedes the intentKey, so must be at the lowest version of the
			// preceding key or exhausted. So step it forward. It will now point to
			// a key that is the same as the intentKey since an intent always has a
			// corresponding provisional value, and provisional values must have a
			// higher timestamp than any committed value on a key. Note that the
			// code below does not specifically care if a bug (external to this
			// code) violates the invariant that the iter is pointing to the
			// provisional value, but it does care that iter is pointing to some
			// version of that key.
			i.iter.Next()
			if err := i.tryDecodeKey(); err != nil {
				return
			}
			i.intentCmp = 0
			if !i.iterValid {
				i.err = errors.Errorf("intent has no provisional value")
				i.valid = false
				return
			}
			if util.RaceEnabled {
				cmp := i.intentKey.Compare(i.iterKey.Key)
				if cmp != 0 {
					i.err = errors.Errorf("intent has no provisional value, cmp: %d", cmp)
					i.valid = false
					return
				}
			}
			i.makeUpperLimitKey()
			var iterState pebble.IterValidityState
			var err error
			for j := 0; j < i.numIntents; j++ {
				iterState, err = i.intentIter.NextEngineKeyWithLimit(i.lastLimitKey)
			}
			if err := i.tryDecodeLockKey(iterState, err); err != nil {
				return
			}
			// At the last intent for that key.
			if i.intentIterKey == nil {
				i.err = errors.Errorf("intent not found")
				i.valid = false
				return
			}
			// One past the intent since we have consumed all of them to construct
			// i.mvccMetadata.
			iterState, err = i.intentIter.NextEngineKeyWithLimit(i.lastLimitKey)
			if err := i.tryDecodeLockKey(iterState, err); err != nil {
				return
			}
		} else {
			// The intentIter precedes the iter. It could be for the same key, iff
			// this key has an intent, or an earlier key. Either way, stepping
			// forward will take it to an intent for a later key.
			i.makeUpperLimitKey()
			iterState, err := i.intentIter.NextEngineKeyWithLimit(i.lastLimitKey)
			if err = i.tryDecodeLockKey(iterState, err); err != nil {
				return
			}
			i.intentCmp = +1
			if util.RaceEnabled && iterState == pebble.IterValid {
				cmp := i.intentIterKey.Compare(i.iterKey.Key)
				if cmp <= 0 {
					i.err = errors.Errorf("intentIter incorrectly positioned, cmp: %d", cmp)
					i.valid = false
					return
				}
			}
		}
	}
	if !i.valid {
		return
	}
	if i.isCurAtIntentIter() {
		i.mvccMetadata = nil
		// iter must be positioned at the provisional value. Note that the code
		// below does not specifically care if a bug (external to this code)
		// violates the invariant that the iter is pointing to the provisional
		// value, but it does care that iter is pointing to some version of that
		// key.
		if i.intentCmp != 0 {
			i.err = errors.Errorf("intentIter at intent, but iter not at provisional value")
			i.valid = false
			return
		}
		if !i.iterValid {
			i.err = errors.Errorf("iter expected to be at provisional value, but is exhausted")
			i.valid = false
			return
		}
		i.intentCmp = +1
		if util.RaceEnabled && i.intentIterKey != nil {
			cmp := i.intentIterKey.Compare(i.iterKey.Key)
			if cmp <= 0 {
				i.err = errors.Errorf("intentIter incorrectly positioned, cmp: %d", cmp)
				i.valid = false
				return
			}
		}
	} else {
		// Common case:
		// The iterator is positioned at iter.
		i.iter.Next()
		if err := i.tryDecodeKey(); err != nil {
			return
		}
		if i.intentIterState == pebble.IterAtLimit {
			// TODO(sumeer): could avoid doing this if i.iter has stepped to
			// different version of same key.
			i.makeUpperLimitKey()
			iterState, err := i.intentIter.NextEngineKeyWithLimit(i.lastLimitKey)
			if err = i.tryDecodeLockKey(iterState, err); err != nil {
				return
			}
		}
		i.computePos(false)
	}
}

func (i *intentInterleavingIter) NextKey() {
	// NextKey is not called to switch directions, i.e., we must already
	// be in the forward direction.
	if i.dir < 0 {
		i.err = errors.Errorf("NextKey cannot be used to switch iteration direction")
		i.valid = false
		return
	}
	if !i.valid {
		return
	}
	if i.isCurAtIntentIter() {
		i.mvccMetadata = nil
		// iter must be positioned at the provisional value.
		if i.intentCmp != 0 {
			i.err = errors.Errorf("intentIter at intent, but iter not at provisional value")
			i.valid = false
			return
		}
	}
	// Step the iter to NextKey(), i.e., past all the versions of this key.
	i.iter.NextKey()
	if err := i.tryDecodeKey(); err != nil {
		return
	}
	if i.intentIterState == pebble.IterAtLimit {
		i.makeUpperLimitKey()
		iterState, err := i.intentIter.NextEngineKeyWithLimit(i.lastLimitKey)
		if err := i.tryDecodeLockKey(iterState, err); err != nil {
			return
		}
	}
	i.computePos(false)
}

func (i *intentInterleavingIter) isCurAtIntentIter() bool {
	return i.mvccMetadata != nil
}

func (i *intentInterleavingIter) UnsafeKey() MVCCKey {
	if i.isCurAtIntentIter() {
		return MVCCKey{Key: i.intentKey}
	}
	return i.iterKey
}

func (i *intentInterleavingIter) UnsafeValue() []byte {
	if i.mvccMetadata != nil {
		// TODO: the interface change will eliminate this wasteful re-serialization.
		val, err := protoutil.Marshal(i.mvccMetadata)
		if err != nil {
			panic(errors.AssertionFailedf("unmarshaled MVCCMetadata is failing marshaling %s", err))
		}
		return val
	}
	return i.iter.UnsafeValue()
}

func (i *intentInterleavingIter) Key() MVCCKey {
	key := i.UnsafeKey()
	keyCopy := make([]byte, len(key.Key))
	copy(keyCopy, key.Key)
	key.Key = keyCopy
	return key
}

func (i *intentInterleavingIter) Value() []byte {
	if i.mvccMetadata != nil {
		// TODO: the interface change will eliminate this wasteful re-serialization.
		val, err := protoutil.Marshal(i.mvccMetadata)
		if err != nil {
			panic(errors.AssertionFailedf("unmarshaled MVCCMetadata is failing marshaling %s", err))
		}
		return val
	}
	return i.iter.Value()
}

func (i *intentInterleavingIter) Close() {
	i.iter.Close()
	i.intentIter.Close()
	*i = intentInterleavingIter{
		intentKey:         i.intentKey,
		intentSeekKeyBuf:  i.intentSeekKeyBuf,
		intentLimitKeyBuf: i.intentLimitKeyBuf,
	}
	intentInterleavingIterPool.Put(i)
}

func (i *intentInterleavingIter) SeekLT(key MVCCKey) {
	i.dir = -1
	i.valid = true
	i.err = nil
	i.mvccMetadata = nil

	if i.prefix {
		i.err = errors.Errorf("prefix iteration is not permitted with SeekLT")
		i.valid = false
		return
	}
	if i.constraint != notConstrained {
		// If the seek key of SeekLT is the boundary between the local and global
		// keyspaces, iterators constrained in either direction are permitted.
		// Iterators constrained to the local keyspace may be scanning from their
		// upper bound. Iterators constrained to the global keyspace may have found
		// a key on the boundary and may now be scanning before the key, using the
		// boundary as an exclusive upper bound.
		// NB: an iterator with bounds [L, U) is allowed to SeekLT over any key in
		// [L, U]. For local keyspace iterators, U can be LocalMax and for global
		// keyspace iterators L can be LocalMax.
		localMax := bytes.Equal(key.Key, keys.LocalMax)
		if !localMax {
			i.checkConstraint(key.Key, true)
		}
		if localMax && i.constraint == constrainedToLocal {
			// Move it down to below the lock table so can iterate down cleanly into
			// the local key space. Note that we disallow anyone using a seek key
			// that is a local key above the lock table, and there should be no keys
			// in the engine there either (at least not keys that we need to see using
			// an MVCCIterator).
			key.Key = keys.LocalRangeLockTablePrefix
		}
	}

	i.iter.SeekLT(key)
	if err := i.tryDecodeKey(); err != nil {
		return
	}
	var intentSeekKey roachpb.Key
	if key.Timestamp.IsEmpty() {
		// Common case.
		intentSeekKey, i.intentSeekKeyBuf = keys.LockTableSingleKey(key.Key, i.intentSeekKeyBuf)
	} else {
		// Seeking to a specific version, so need to see the intent. Since we need
		// to see the intent for key.Key, and we don't have SeekLE, call Next() on
		// the key before doing SeekLT.
		// TODO: we will need to bar this case if it misses the latest version,
		// since we cannot initialize i.largestVersionSeen which is needed to
		// reduce the unknown disposition intent population to at most 1. It is
		// also not clear why this would be beneficial to a caller. The only
		// current use of SeekLT with a non-zero timestamp is in
		// MVCCGarbageCollect, where the caller wants to find the version that is
		// the one step younger than the GC key, and has already forward iterated
		// over many versions -- so in this case this SeekLT will not miss the
		// latest version. As a hack we throw a runtime error in this case.
		intentSeekKey, i.intentSeekKeyBuf = keys.LockTableSingleKey(key.Key.Next(), i.intentSeekKeyBuf)
	}
	i.makeLowerLimitKey()
	iterState, err := i.intentIter.SeekEngineKeyLTWithLimit(
		EngineKey{Key: intentSeekKey}, i.lastLimitKey)
	if err = i.tryDecodeLockKey(iterState, err); err != nil {
		return
	}
	i.computePos(true)
}

func (i *intentInterleavingIter) Prev() {
	if i.err != nil {
		return
	}
	if i.dir > 0 {
		// Switching from forward to reverse iteration.
		isCurAtIntent := i.isCurAtIntentIter()
		i.dir = -1
		if !i.valid {
			// Both iterators are exhausted, so step both backward.
			i.valid = true
			i.iter.Prev()
			if err := i.tryDecodeKey(); err != nil {
				return
			}
			i.makeLowerLimitKey()
			iterState, err := i.intentIter.PrevEngineKeyWithLimit(i.lastLimitKey)
			if err = i.tryDecodeLockKey(iterState, err); err != nil {
				return
			}
			i.computePos(false)
			return
		}
		// At least one of the iterators is not exhausted.
		if isCurAtIntent {
			// intentIter has moved beyond the intent to the next roachpb.Key. iter
			// must be at the provisional value corresponding to intentKey.
			// Step both backward: intentIter must be stepped back numIntents times.
			//
			// Note that the code below does not specifically care if a
			// bug (external to this code) violates the invariant that the
			// provisional value is the highest timestamp key, but it does care that
			// there is a timestamped value for this key (which it checks below).
			// The internal invariant of this iterator implementation will ensure
			// that iter is pointing to the highest timestamped key.

			if i.intentCmp != 0 {
				i.err = errors.Errorf("iter not at provisional value, cmp: %d", i.intentCmp)
				i.valid = false
				return
			}
			i.iter.Prev()
			if err := i.tryDecodeKey(); err != nil {
				return
			}
			i.intentCmp = +1
			if util.RaceEnabled && i.iterValid {
				cmp := i.intentKey.Compare(i.iterKey.Key)
				if cmp <= 0 {
					i.err = errors.Errorf("intentIter should be after iter, cmp: %d", cmp)
					i.valid = false
					return
				}
			}
			i.makeLowerLimitKey()
			var iterState pebble.IterValidityState
			var err error
			for j := 0; j < i.numIntents; j++ {
				iterState, err = i.intentIter.PrevEngineKeyWithLimit(i.lastLimitKey)
			}
			if err := i.tryDecodeLockKey(iterState, err); err != nil {
				return
			}
			// At the last intent for that key.
			if i.intentIterKey == nil {
				i.err = errors.Errorf("intent not found")
				i.valid = false
				return
			}
			// One before the intent since we have consumed all of them to construct
			// i.mvccMetadata.
			iterState, err = i.intentIter.PrevEngineKeyWithLimit(i.lastLimitKey)
			if err := i.tryDecodeLockKey(iterState, err); err != nil {
				return
			}
		} else {
			// The intentIter is after the iter. We don't know whether the iter key
			// has an intent. Either way, stepping the intentIter back will make it
			// look similar to how backwards iteration would be positioned when the
			// interleaving iter is at a versioned key.
			i.makeLowerLimitKey()
			iterState, err := i.intentIter.PrevEngineKeyWithLimit(i.lastLimitKey)
			if err = i.tryDecodeLockKey(iterState, err); err != nil {
				return
			}
			if i.intentIterKey == nil {
				i.intentCmp = -1
			} else {
				i.intentCmp = i.intentIterKey.Compare(i.iterKey.Key)
			}
		}
	}
	if !i.valid {
		return
	}
	if i.isCurAtIntentIter() {
		// intentIter must be positioned preceding the intent, and iter is
		// exhausted or positioned at a versioned value of a preceding key.
		i.mvccMetadata = nil
		if i.intentIterState == pebble.IterAtLimit {
			// TODO(sumeer): could avoid doing this if i.iter has stepped to
			// different version of same key.
			i.makeLowerLimitKey()
			iterState, err := i.intentIter.PrevEngineKeyWithLimit(i.lastLimitKey)
			if err = i.tryDecodeLockKey(iterState, err); err != nil {
				return
			}
		}
		i.computePos(false)
	} else {
		// Common case:
		// The iterator is positioned at iter, i.e., at a versioned value.

		// There may be intents for this key, though they may be simple-committed,
		// since we have not read them and figured out their disposition yet.
		// Stash the largestVersionSeen, so that it can be used in the future.
		i.largestVersionSeen = i.iterKey.Timestamp
		i.iter.Prev()
		if err := i.tryDecodeKey(); err != nil {
			return
		}
		if i.intentIterState == pebble.IterAtLimit {
			// TODO(sumeer): could avoid doing this if i.iter has stepped to
			// different version of same key.
			i.makeLowerLimitKey()
			iterState, err := i.intentIter.PrevEngineKeyWithLimit(i.lastLimitKey)
			if err = i.tryDecodeLockKey(iterState, err); err != nil {
				return
			}
		}
		i.computePos(false)
	}
}

func (i *intentInterleavingIter) UnsafeRawKey() []byte {
	if i.isCurAtIntentIter() {
		return i.intentIter.UnsafeRawEngineKey()
	}
	return i.iter.UnsafeRawKey()
}

func (i *intentInterleavingIter) UnsafeRawMVCCKey() []byte {
	if i.isCurAtIntentIter() {
		return i.intentKeyAsNoTimestampMVCCKey
	}
	return i.iter.UnsafeRawKey()
}

func (i *intentInterleavingIter) ValueProto(msg protoutil.Message) error {
	value := i.UnsafeValue()
	return protoutil.Unmarshal(value, msg)
}

func (i *intentInterleavingIter) ComputeStats(
	start, end roachpb.Key, nowNanos int64,
) (enginepb.MVCCStats, error) {
	return ComputeStatsForRange(i, start, end, nowNanos)
}

func (i *intentInterleavingIter) FindSplitKey(
	start, end, minSplitKey roachpb.Key, targetSize int64,
) (MVCCKey, error) {
	return findSplitKeyUsingIterator(i, start, end, minSplitKey, targetSize)
}

func (i *intentInterleavingIter) SetUpperBound(key roachpb.Key) {
	i.iter.SetUpperBound(key)
	// Preceding call to SetUpperBound has confirmed that key != nil.
	if i.constraint != notConstrained {
		i.checkConstraint(key, true)
	}
	var intentUpperBound roachpb.Key
	intentUpperBound, i.intentSeekKeyBuf = keys.LockTableSingleKey(key, i.intentSeekKeyBuf)
	i.intentIter.SetUpperBound(intentUpperBound)
}

func (i *intentInterleavingIter) Stats() IteratorStats {
	stats := i.iter.Stats()
	intentStats := i.intentIter.Stats()
	stats.TimeBoundNumSSTs += intentStats.TimeBoundNumSSTs
	for i := pebble.IteratorStatsKind(0); i < pebble.NumStatsKind; i++ {
		stats.Stats.ForwardSeekCount[i] += intentStats.Stats.ForwardSeekCount[i]
		stats.Stats.ReverseSeekCount[i] += intentStats.Stats.ReverseSeekCount[i]
		stats.Stats.ForwardStepCount[i] += intentStats.Stats.ForwardStepCount[i]
		stats.Stats.ReverseStepCount[i] += intentStats.Stats.ReverseStepCount[i]
	}
	stats.Stats.InternalStats.Merge(intentStats.Stats.InternalStats)
	return stats
}

func (i *intentInterleavingIter) SupportsPrev() bool {
	return true
}

// newMVCCIteratorByCloningEngineIter assumes MVCCKeyIterKind and no timestamp
// hints. It uses pebble.Iterator.Clone to ensure that the two iterators see
// the identical engine state.
func newMVCCIteratorByCloningEngineIter(iter EngineIterator, opts IterOptions) MVCCIterator {
	pIter := iter.GetRawIter()
	it := newPebbleIterator(nil, pIter, opts, StandardDurability)
	if iter == nil {
		panic("couldn't create a new iterator")
	}
	return it
}

// unsageMVCCIterator is used in RaceEnabled test builds to randomly inject
// changes to unsafe keys retrieved from MVCCIterators.
type unsafeMVCCIterator struct {
	MVCCIterator
	keyBuf        []byte
	rawKeyBuf     []byte
	rawMVCCKeyBuf []byte
}

func wrapInUnsafeIter(iter MVCCIterator) MVCCIterator {
	return &unsafeMVCCIterator{MVCCIterator: iter}
}

var _ MVCCIterator = &unsafeMVCCIterator{}

func (i *unsafeMVCCIterator) SeekGE(key MVCCKey) {
	i.mangleBufs()
	i.MVCCIterator.SeekGE(key)
}

func (i *unsafeMVCCIterator) Next() {
	i.mangleBufs()
	i.MVCCIterator.Next()
}

func (i *unsafeMVCCIterator) NextKey() {
	i.mangleBufs()
	i.MVCCIterator.NextKey()
}

func (i *unsafeMVCCIterator) SeekLT(key MVCCKey) {
	i.mangleBufs()
	i.MVCCIterator.SeekLT(key)
}

func (i *unsafeMVCCIterator) Prev() {
	i.mangleBufs()
	i.MVCCIterator.Prev()
}

func (i *unsafeMVCCIterator) UnsafeKey() MVCCKey {
	rv := i.MVCCIterator.UnsafeKey()
	i.keyBuf = append(i.keyBuf[:0], rv.Key...)
	rv.Key = i.keyBuf
	return rv
}

func (i *unsafeMVCCIterator) UnsafeRawKey() []byte {
	rv := i.MVCCIterator.UnsafeRawKey()
	i.rawKeyBuf = append(i.rawKeyBuf[:0], rv...)
	return i.rawKeyBuf
}

func (i *unsafeMVCCIterator) UnsafeRawMVCCKey() []byte {
	rv := i.MVCCIterator.UnsafeRawMVCCKey()
	i.rawMVCCKeyBuf = append(i.rawMVCCKeyBuf[:0], rv...)
	return i.rawMVCCKeyBuf
}

func (i *unsafeMVCCIterator) mangleBufs() {
	if rand.Intn(2) == 0 {
		for _, b := range [3][]byte{i.keyBuf, i.rawKeyBuf, i.rawMVCCKeyBuf} {
			for i := range b {
				b[i] = 0
			}
		}
	}
}
