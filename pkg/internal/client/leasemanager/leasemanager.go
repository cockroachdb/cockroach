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
