// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package advisorylock

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// LockIsNotAvailableErr is the error returned when a lock is not available,
// and we are not allowed to wait for it.
var LockIsNotAvailableErr = errors.New("lock is not available")

// lockKeyType is the type of lock key.
type lockKeyType int32

const (
	// lockKeyTypeSingle is lock key consisting of a single 64-bit integer.
	lockKeyTypeSingle lockKeyType = 1
	// lockKeyTypeTwo is lock key consisting of two 32-bit integers.
	lockKeyTypeTwo lockKeyType = 2
)

// LockKey is the key of a single advisory lock.
type LockKey struct {
	// databaseID is the ID of the database that the lock is for.
	databaseID descpb.ID
	// lockType is the type of the lock.
	lockType lockKeyType
	// lockKey is the key of the lock, when lockKeyTypeTwo is used, the lockKey
	// is a packed int64 with the high 32 bits being lockKey and the low 32 bits
	// being lockKey2.
	lockKey int64
}

// Encode encodes the lock key into a byte slice. Base key is
// the primary index key prefix for the advisory lock table.
func (k LockKey) Encode(baseKey roachpb.Key) ([]byte, error) {
	baseKey, err := keyside.Encode(baseKey, tree.NewDInt(tree.DInt(k.databaseID)), encoding.Ascending)
	if err != nil {
		return nil, err
	}
	baseKey, err = keyside.Encode(baseKey, tree.NewDInt(tree.DInt(k.lockType)), encoding.Ascending)
	if err != nil {
		return nil, err
	}
	baseKey, err = keyside.Encode(baseKey, tree.NewDInt((tree.DInt(k.lockKey))), encoding.Ascending)
	if err != nil {
		return nil, err
	}
	return keys.MakeFamilyKey(baseKey, 0), nil
}

func (k LockKey) String() string {
	return fmt.Sprintf("(%d, %d, %d)", k.databaseID, k.lockType, k.lockKey)
}

// SafeValue is part of the redact.SafeValue interface.
func (k LockKey) SafeValue() {}

// MakeLockKeyInt64 creates a lock key consisting of a single 64-bit integer.
func MakeLockKeyInt64(databaseID descpb.ID, lockKey int64) LockKey {
	return LockKey{
		databaseID: databaseID,
		lockType:   lockKeyTypeSingle,
		lockKey:    lockKey,
	}
}

// MakeLockKeyInt32 creates a lock key consisting of two 32-bit integers.
// The packed int64 matches PostgreSQL: the high 32 bits are lockKey; the low
// 32 bits are the unsigned bit pattern of lockKey2 (not sign-extended).
func MakeLockKeyInt32(databaseID descpb.ID, lockKey int32, lockKey2 int32) LockKey {
	return LockKey{
		databaseID: databaseID,
		lockType:   lockKeyTypeTwo,
		lockKey:    int64(lockKey)<<32 | int64(uint32(lockKey2)),
	}
}

// LockMode is the mode of the advisory lock.
type LockMode int32

const (
	// LockModeShare is the mode of the advisory lock for share.
	LockModeShare LockMode = iota
	// LockModeExclusive is the mode of the advisory lock for exclusive.
	LockModeExclusive
)

// Manager is responsible for acquiring and releasing advisory locks, depending
// on the scope of the lock.
type Manager struct {
	descs   *descs.Collection
	codec   keys.SQLCodec
	baseKey roachpb.Key
}

// NewManager creates a new advisory lock manager.
func NewManager(descs *descs.Collection, codec keys.SQLCodec) *Manager {
	return &Manager{
		descs: descs,
		codec: codec,
	}
}

// getBaseKey returns the primary index key for the advisory lock table.
func (m *Manager) getBaseKey(ctx context.Context, txn *kv.Txn) (roachpb.Key, error) {
	if m.baseKey != nil {
		return m.baseKey.Clone(), nil
	}
	systemDatabase, err := m.descs.ByName(txn).Get().Database(ctx, catconstants.SystemDatabaseName)
	if err != nil {
		return nil, err
	}
	publicSchema, err := m.descs.ByName(txn).Get().Schema(ctx, systemDatabase, catconstants.PublicSchemaName)
	if err != nil {
		return nil, err
	}
	advisoryLockTable, err := m.descs.ByName(txn).Get().Table(ctx, systemDatabase, publicSchema, string(catconstants.AdvisoryLocksTableName))
	if err != nil {
		return nil, err
	}
	m.baseKey = m.codec.IndexPrefix(uint32(advisoryLockTable.GetID()), uint32(advisoryLockTable.GetPrimaryIndexID()))
	return m.baseKey.Clone(), nil
}

// AcquireInTxn acquires a lock in the given transaction. Transaction
// locks are automatically released on commit / rollback.
func (m *Manager) AcquireInTxn(
	ctx context.Context, txn *kv.Txn, key LockKey, mode LockMode, wait bool,
) error {
	// Encode the key into the primary index key for the advisory lock table.
	baseKey, err := m.getBaseKey(ctx, txn)
	if err != nil {
		return err
	}
	encodedKey, err := key.Encode(baseKey)
	if err != nil {
		return err
	}
	b := txn.NewBatch()
	// If waiting is disabled, we will return an error if the lock is not available.
	if !wait {
		b.Header.WaitPolicy = lock.WaitPolicy_Error
	}
	// Lock the key in the appropriate mode, we are going to be locking
	// a non-existing key. Additionally, for simplicity this lock will
	// be replicated, since otherwise we need some ability to detect if
	// the lock was lost.
	lockMode := lock.Exclusive
	if mode == LockModeShare {
		lockMode = lock.Shared
	}
	b.AddRawRequest(&kvpb.GetRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: encodedKey,
		},
		KeyLockingStrength:   lockMode,
		LockNonExisting:      true, // Key will not exist.
		KeyLockingDurability: lock.Replicated,
	})
	err = txn.Run(ctx, b)
	// Detect if the lock is not available if we are not waiting for it.
	if err != nil {
		if !wait && isWaitPolicyLockConflict(err) {
			return LockIsNotAvailableErr
		}
		return errors.Wrap(err, "failed to acquire advisory lock in transaction")
	}
	if len(b.Results) > 0 && b.Results[0].Err != nil {
		resErr := b.Results[0].Err
		if !wait && isWaitPolicyLockConflict(resErr) {
			return LockIsNotAvailableErr
		}
		return errors.Wrap(resErr, "failed to acquire advisory lock in transaction")
	}
	return nil
}

// isWaitPolicyLockConflict checks if the error is a lock conflict error
// and the wait policy is set to Error.
func isWaitPolicyLockConflict(err error) bool {
	var wi *kvpb.WriteIntentError
	if !errors.As(err, &wi) {
		return false
	}
	return wi.Reason == kvpb.WriteIntentError_REASON_WAIT_POLICY
}
