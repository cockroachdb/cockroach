package leasemanager

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

type LeaseManager struct {
	prefix []byte
	db     *client.DB
}

func New(prefix []byte, db *client.DB) *LeaseManager {
	return &LeaseManager{
		prefix: prefix[:len(prefix):len(prefix)],
		db:     db,
	}
}

type Lease interface {
	GetExpiration() hlc.Timestamp
}

// Sketch of safety proof for algorithm coded here:
//
// The proof uses the construct of a totally ordered history of operations,
// known to an oracle. When the proof refers to "before", "after" it is in
// the context of this history. Locks are held for some interval [t1, t2],
// where the timestamps are HLC time. The correctness is defined in terms
// of these intervals: intervals for exclusive locks cannot overlap with
// each other or with intervals of shared locks.
//
// Consider 3 events for both exclusive and shared locks: acquisition,
// hold and release. The reasoning will consider the violations that can
// happen at the event in the history when the lock is acquired, at any
// event in history while the lock is held, and any event in history after
// the lock is released. And will sketch how these violations are impossible.
//
// Exclusive lock:
//   Lock Acquisition:
//   This happens using the following sequence of operations:
//   - put exKey at t1
//   - del range at t2
//   - del range commits at t3
//   - put exKey pushed to t3
//   The lock is considered acquired at t3 (hlc time). t1 <= t2 <= t3.
//
//   The violations that can happen correspond to the following states of
//   the history before the acquire event
//   1 Ongoing hold of a conflicting lock starting at < t3
//   2 Finished hold of a conflicting lock starting at < t3 that extends >= t3
//   3 Finished hold of a conflicting lock starting at >= t3
//   4 Ongoing hold of a conflicting lock starting at >= t3
//
//   The "put exKey"@t3 eliminates 1, 2, 3, 4 for a conflict with an exclusive
//   lock since puts are monotonic.
//   The "del range"@t2 eliminates 1, 2, 3, 4 for a conflict with a shared lock since
//   it implies there was no intent or committed value >= t2 in that range.

//   Lock Hold:
//   While the lock is held, we don't want new violations appearing in the
//   history after the acquisition. The violation cases are the same as listed earlier.
//   The intent created by "put exKey"@t3 eliminates 1, 2, 3, 4 for a
//   conflict with an exclusive lock since it prevents new intents from
//   being created.
//   That intent also prevents shared locks since they also need to "put exKey".
//
//   Lock Release:
//   Two cases:
//   - Transaction commits at t4: Lock should be held [t3, t4]. The "put exKey"
//     commits at t4. Since no one can write to this key at <= t4, it eliminates
//     new violations being created after the lock release that overlap with
//     [t3, t4].
//   - Transaction aborts with heartbeat expiry time t4. The lock should be
//     held [t3, t4]. Since the ts cache is updated to t4, any write after the
//     release cannot write to the key at <= t4, so it cannot be a violation.
//
// Shared lock:
//   Lock Acquisition:
//   This happens using the following sequence of operations:
//   - put exKey at t1
//   - put shKey at t2
//   - put exKey commits at t3
//   The lock is considered acquired at t3. t1 <= t2 <= t3.
//
//   The same violation list applies here. The "put exKey" committing at t3
//   eliminates cases 1, 2, 3, 4.
//
//   Lock Hold:
//   While the lock is held, we don't want new violations appearing in the
//   history due to exclusive lock acquisitions. The "put exKey" committing
//   at t3 eliminates 1, 2 since they can't write to a timestamp <= t3.
//   The intent at shKey@t2 prevents the del range, so eliminates 3, 4.
//
//   Lock Release:
//   Two cases:
//    - Transaction commits at t4: The "put shKey" commits at t4. Any del range
//      has to be > t4. So it prevents all the violations.
//    - Transaction aborts with hearbeat expiry time t4. Since the ts cache for
//      shKey is updated to t4, no del range can happen <= t4.
//
// Alternatives:
// The algorithm implemented here introduces contention between shared lock/lease
// acquisitions since they all need to write to the exKey and commit that write
// in a side transaction. The following alternative avoids this contention.
// It uses the following ideas:
// - Use lossy unreplicated locks to avoid aborts due to violating serializability,
//   by preventing txn races from causing interleaving of operations. This needs
//   unreplicated shared locks.
// - Use a read with failOnMoreRecent, introduced in #44473 for Select For Update.
// - For session scoped locks/leases, which need to be maintained up to the heartbeat
//   expiry time, use a special non-transactional intent that transitions to
//   committed at the abort time.
//
// AcquireExclusive(Txn, Key)
//    exKey, shRange := makeExKeys(key)
//    Txn.AcquireUnreplicatedExLock(exKey)
//    Txn.DelRange(shRange)
//    Txn.Put(exKey)
//
// AcquireShared(Txn, Key)
//    exKey, shKey := makeShKeys(Key)
//    Txn.AcquireUnreplicatedShLock(exKey)  // Blocks on exclusive lock on exKey regardless of timestamp.
//    Txn.Read(exKey, failOnMoreRecent=true)  // #44473
//    Txn.Put(shKey)
//    // Loss of unreplicated shared lock at this point does not result in abort.
//
// I have a sketch of a safety proof of the above, if it ever becomes a serious
// alternative.


// One thing to note about these leases is that there's no way to release them.
// In practive there will be a way - rollback to savepoint but the better way is
// to just hold the transaction open.

// So the deal is say we have a user transaction - for it to acquire exclusive it would need to
// write a row and then it would need to issue a clear range and block on that clear range.
// That clear range will also need to be using the same txn.
//
// When acquiring a shared, you need to read the write key. If it writes that key is there
// a problem? It should need to read the key again. Okay cool.
func (lm *LeaseManager) AcquireShared(
	ctx context.Context, txn *client.Txn, key []byte,
) (Lease, error) {
	exKey, shKey := makeSharedKeys(lm.prefix, key, txn.ID())
	var laidDownExKeyIntent bool
	err := lm.db.Txn(ctx, func(ctx context.Context, sideTxn *client.Txn) (err error) {
		if laidDownExKeyIntent {
			return errors.AssertionFailedf(
				"should not be able to contend on shKey "+
					"-- if there's a retryable error there then there's a programming bug: txn ID %v", txn.ID())
		}
		if err := sideTxn.Put(ctx, exKey, nil); err != nil {
			return err
		}
		laidDownExKeyIntent = true
		txn.PushTo(sideTxn.ProvisionalCommitTimestamp())
		if err := txn.PutLease(ctx, shKey, nil); err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err,
				"should not be able to contend on shKey "+
					"-- if there's a retryable error there then there's a programming bug: txn ID %v", txn.ID())
		}
		sideTxn.PushTo(txn.ProvisionalCommitTimestamp())
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err = txn.ForceHeartbeat(); err != nil {
		return nil, err
	}
	return &leaseImpl{txn: txn, maxOffset: lm.db.Clock().MaxOffset()}, nil
}

func (lm *LeaseManager) AcquireExclusive(
	ctx context.Context, txn *client.Txn, key []byte,
) (Lease, error) {
	// Here we want to write a delete to the constructed key for the name plus maybe a suffix
	// then we're going to DeleteRange over the shared lock suffix.
	exKey, shStart, shEnd := makeExclusiveKeys(lm.prefix, key)
	if err := txn.PutLease(ctx, exKey, nil); err != nil {
		return nil, err
	}

	var sideTxn *client.Txn
	if err := lm.db.Txn(ctx, func(ctx context.Context, st *client.Txn) (err2 error) {
		sideTxn = st
		sideTxn.PushTo(txn.ProvisionalCommitTimestamp())
		return sideTxn.DelRange(ctx, shStart, shEnd)
	}); err != nil {
		return nil, err
	}
	txn.PushTo(sideTxn.CommitTimestamp())
	if err := txn.ForceHeartbeat(); err != nil {
		return nil, err
	}
	return &leaseImpl{txn: txn, maxOffset: lm.db.Clock().MaxOffset()}, nil
}

// TODO(ajwerner): Optimize allocations here by allocating all of the keys from
// a shared buffer/byte allocator.

var (
	sharedSuffix    = []byte("sh")
	exclusiveSuffix = []byte("ex")
)

func makeExclusiveKeys(prefix, key []byte) (exKey, shStart, shEnd roachpb.Key) {
	shStart = makeSharedKeyPrefix(prefix, key)
	shEnd = shStart.PrefixEnd()
	return makeExclusiveKey(prefix, key), shStart, shEnd
}

func makeSharedKeys(prefix, key []byte, id uuid.UUID) (exKey, shKey roachpb.Key) {
	return makeExclusiveKey(prefix, key), makeSharedKey(prefix, key, id)
}

func makeExclusiveKey(prefix, key []byte) roachpb.Key {
	return encoding.EncodeBytesAscending(
		encoding.EncodeBytesAscending(prefix, key), exclusiveSuffix)
}

func makeSharedKeyPrefix(prefix, key []byte) roachpb.Key {
	return encoding.EncodeBytesAscending(
		encoding.EncodeBytesAscending(prefix, key), sharedSuffix)
}

func makeSharedKeyRange(prefix, key []byte) (start, end roachpb.Key) {
	start = makeSharedKeyPrefix(prefix, key)
	return start, start.PrefixEnd()
}

func makeSharedKey(prefix, key []byte, id uuid.UUID) roachpb.Key {
	return encoding.EncodeBytesAscending(makeSharedKeyPrefix(prefix, key), []byte(id.String()))
}

type leaseImpl struct {
	txn       *client.Txn
	maxOffset time.Duration
}

func (l *leaseImpl) GetExpiration() hlc.Timestamp {
	return l.txn.ExpiryTimestamp().Add(-l.maxOffset.Nanoseconds(), 0)
}
