// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvnemesis

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvnemesis/kvnemesisutil"
	kvpb "github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// GeneratorConfig contains all the tunable knobs necessary to run a Generator.
type GeneratorConfig struct {
	Ops                   OperationConfig
	NumNodes, NumReplicas int
}

// OperationConfig configures the relative probabilities of producing various
// operations.
//
// In this struct and all sub-configurations, wording such as "likely exists" or
// "definitely doesn't exist" is according to previously generated steps.
// "likely" is a result of non-determinism due to concurrent execution of the
// generated operations.
type OperationConfig struct {
	DB             ClientOperationConfig
	Batch          BatchOperationConfig
	ClosureTxn     ClosureTxnConfig
	Split          SplitConfig
	Merge          MergeConfig
	ChangeReplicas ChangeReplicasConfig
	ChangeLease    ChangeLeaseConfig
	ChangeSetting  ChangeSettingConfig
	ChangeZone     ChangeZoneConfig
}

// ClosureTxnConfig configures the relative probability of running some
// operations in a transaction by using the closure-based kv.DB.Txn method. This
// family of operations mainly varies in how it commits (or doesn't commit). The
// composition of the operations in the txn is controlled by TxnClientOps and
// TxnBatchOps
type ClosureTxnConfig struct {
	// CommitSerializable is a serializable transaction that commits normally.
	CommitSerializable int
	// CommitSnapshot is a snapshot transaction that commits normally.
	CommitSnapshot int
	// CommitReadCommitted is a read committed transaction that commits normally.
	CommitReadCommitted int
	// RollbackSerializable is a serializable transaction that encounters an error
	// at the end and has to roll back.
	RollbackSerializable int
	// RollbackSnapshot is a snapshot transaction that encounters an error at the
	// end and has to roll back.
	RollbackSnapshot int
	// RollbackReadCommitted is a read committed transaction that encounters an
	// error at the end and has to roll back.
	RollbackReadCommitted int
	// CommitSerializableInBatch is a serializable transaction that commits via
	// the CommitInBatchMethod. This is an important part of the 1pc txn fastpath.
	CommitSerializableInBatch int
	// CommitSnapshotInBatch is a snapshot transaction that commits via the
	// CommitInBatchMethod. This is an important part of the 1pc txn fastpath.
	CommitSnapshotInBatch int
	// CommitReadCommittedInBatch is a read committed transaction that commits
	// via the CommitInBatchMethod. This is an important part of the 1pc txn
	// fastpath.
	CommitReadCommittedInBatch int

	TxnClientOps ClientOperationConfig
	TxnBatchOps  BatchOperationConfig
	// When CommitInBatch is selected, CommitBatchOps controls the composition of
	// the kv.Batch used.
	CommitBatchOps ClientOperationConfig
	SavepointOps   SavepointConfig
}

// ClientOperationConfig configures the relative probabilities of the
// bread-and-butter kv operations such as Get/Put/Delete/etc. These can all be
// run on a DB, a Txn, or a Batch.
type ClientOperationConfig struct {
	// GetMissing is an operation that Gets a key that definitely doesn't exist.
	GetMissing int
	// GetMissingForUpdate is an operation that Gets a key that definitely doesn't
	// exist using a locking read with strength lock.Exclusive.
	GetMissingForUpdate int
	// GetMissingForUpdateGuaranteedDurability is an operation that Gets a key
	// that definitely doesn't exist using a locking read with strength
	// lock.Exclusive and durability lock.Replicated.
	GetMissingForUpdateGuaranteedDurability int
	// GetMissingForShare is an operation that Gets a key that definitely doesn't
	// exist using a locking read with strength lock.Shared.
	GetMissingForShare int
	// GetMissingForShareGuaranteedDurability is an operation that Gets a key
	// that definitely doesn't exist using a locking read with strength
	// lock.Shared and durability lock.Replicated.
	GetMissingForShareGuaranteedDurability int
	// GetMissingSkipLocked is an operation that Gets a key that definitely
	// doesn't exist while skipping locked keys.
	GetMissingSkipLocked int
	// GetMissingForUpdateSkipLocked is an operation that Gets a key that
	// definitely doesn't exist using a locking read, with strength
	// lock.Exclusive, while skipping locked keys.
	GetMissingForUpdateSkipLocked int
	// GetMissingForUpdateSkipLocked is an operation that Gets a key that
	// definitely doesn't exist using a locking read, with strength
	// lock.Exclusive and durability lock.Replicated, while skipping locked keys.
	GetMissingForUpdateSkipLockedGuaranteedDurability int
	// GetMissingForShareSkipLocked is an operation that Gets a key that
	// definitely doesn't exist using a locking read, with strength lock.Shared,
	// while skipping locked keys.
	GetMissingForShareSkipLocked int
	// GetMissingForShareSkipLockedGuaranteedDurability is an operation that Gets
	// a key that definitely doesn't exist using a locking read, with strength
	// lock.Shared and durability lock.Replicated, while skipping locked keys.
	GetMissingForShareSkipLockedGuaranteedDurability int
	// GetExisting is an operation that Gets a key that likely exists.
	GetExisting int
	// GetExistingForUpdate is an operation that Gets a key that likely exists
	// using a locking read with strength lock.Exclusive.
	GetExistingForUpdate int
	// GetExistingForUpdateGuaranteedDurability is an operation that Gets a key
	// that likely exists using a locking read with strength lock.Exclusive and
	// durability lock.Replicated.
	GetExistingForUpdateGuaranteedDurability int
	// GetExistingForShare is an operation that Gets a key that likely exists
	// using a locking read with strength lock.Shared.
	GetExistingForShare int
	// GetExistingForShareGuaranteedDurability is an operation that Gets a key
	// that likely exists using a locking read with strength lock.Shared and
	// durability lock.Replicated.
	GetExistingForShareGuaranteedDurability int
	// GetExistingSkipLocked is an operation that Gets a key that likely exists
	// while skipping locked keys.
	GetExistingSkipLocked int
	// GetExistingForUpdateSkipLocked is an operation that Gets a key that likely
	// exists using a locking read, with strength lock.Exclusive, while skipping
	// locked keys.
	GetExistingForUpdateSkipLocked int
	// GetExistingForUpdateSkipLockedGuaranteedDurability is an operation that
	// Gets a key that likely exists using a locking read, with strength
	// lock.Exclusive and durability lock.Replicated, while skipping locked keys.
	GetExistingForUpdateSkipLockedGuaranteedDurability int
	// GetExistingForShareSkipLocked is an operation that Gets a key that likely
	// exists using a locking read, with strength lock.Shared, while skipping
	// locked keys.
	GetExistingForShareSkipLocked int
	// GetExistingForShareSkipLockedGuaranteedDurability is an operation that Gets
	// a key that likely exists using a locking read, with strength lock.Shared
	// and durability lock.Replicated, while skipping locked keys.
	GetExistingForShareSkipLockedGuaranteedDurability int
	// PutMissing is an operation that Puts a key that definitely doesn't exist.
	PutMissing int
	// PutExisting is an operation that Puts a key that likely exists.
	PutExisting int
	// Scan is an operation that Scans a key range that may contain values.
	Scan int
	// ScanForUpdate is an operation that Scans a key range that may contain
	// values using a per-key locking scan with strength lock.Exclusive.
	ScanForUpdate int
	// ScanForUpdateGuaranteedDurability is an operation that Scans a key range
	// that may contain values using a per-key locking scan with strength
	// lock.Exclusive and durability lock.Replicated.
	ScanForUpdateGuaranteedDurability int
	// ScanForShare is an operation that Scans a key range that may contain values
	// using a per-key locking scan with strength lock.Shared.
	ScanForShare int
	// ScanForShareGuaranteedDurability is an operation that Scans a key range
	// that may contain values using a per-key locking scan with strength
	// lock.Shared and durability lock.Replicated.
	ScanForShareGuaranteedDurability int
	// ScanSkipLocked is an operation that Scans a key range that may contain
	// values while skipping locked keys.
	ScanSkipLocked int
	// ScanForUpdateSkipLocked is an operation that Scans a key range that may
	// contain values using a per-key locking scan, with strength lock.Exclusive,
	// while skipping locked keys.
	ScanForUpdateSkipLocked int
	// ScanForUpdateSkipLockedGuaranteedDurability is an operation that Scans a
	// key range that may contain values using a per-key locking scan, with
	// strength lock.Exclusive and durability lock.Replicated, while skipping
	// locked keys.
	ScanForUpdateSkipLockedGuaranteedDurability int
	// ScanForShareSkipLocked is an operation that Scans a key range that may
	// contain values using a per-key locking scan, with strength lock.Shared,
	// while skipping locked keys.
	ScanForShareSkipLocked int
	// ScanForShareSkipLockedGuaranteedDurability is an operation that Scans a key
	// range that may contain values using a per-key locking scan, with strength
	// lock.Shared and durability lock.Replicated, while skipping locked keys.
	ScanForShareSkipLockedGuaranteedDurability int
	// ReverseScan is an operation that Scans a key range that may contain
	// values in reverse key order.
	ReverseScan int
	// ReverseScanForUpdate is an operation that Scans a key range that may
	// contain values using a per-key locking scan with strength lock.Exclusive in
	// reverse key order.
	ReverseScanForUpdate int
	// ReverseScanForUpdateGuaranteedDurability is an operation that Scans a key
	// range that may contain values using a per-key locking scan with strength
	// lock.Exclusive and durability lock.Replicated in reverse key order.
	ReverseScanForUpdateGuaranteedDurability int
	// ReverseScanForUpdate is an operation that Scans a key range that may
	// contain values using a per-key locking scan with strength lock.Shared in
	// reverse key order.
	ReverseScanForShare int
	// ReverseScanForUpdateGuaranteedDurability is an operation that Scans a key
	// range that may contain values using a per-key locking scan with strength
	// lock.Shared and durability lock.Replicated in reverse key order.
	ReverseScanForShareGuaranteedDurability int
	// ReverseScanSkipLocked is an operation that Scans a key range that may
	// contain values in reverse key order while skipping locked keys.
	ReverseScanSkipLocked int
	// ReverseScanForUpdateSkipLocked is an operation that Scans a key range that
	// may contain values using a per-key locking scan, with strength
	// lock.Exclusive, in reverse key order while skipping locked keys.
	ReverseScanForUpdateSkipLocked int
	// ReverseScanForUpdateSkipLockedGuaranteedDurability is an operation that
	// Scans a key range that may contain values using a per-key locking scan,
	// with strength lock.Exclusive and durability lock.Replicated, in reverse key
	// order while skipping locked keys.
	ReverseScanForUpdateSkipLockedGuaranteedDurability int
	// ReverseScanForShareSkipLocked is an operation that Scans a key range that
	// may contain values using a per-key locking scan, with strength lock.Share,
	// in reverse key order while skipping locked keys.
	ReverseScanForShareSkipLocked int
	// ReverseScanForShareSkipLockedGuaranteedDurability is an operation that
	// Scans a key range that may contain values using a per-key locking scan,
	// with strength lock.Share and durability lock.Replicated, in reverse key
	// order while skipping locked keys.
	ReverseScanForShareSkipLockedGuaranteedDurability int
	// DeleteMissing is an operation that Deletes a key that definitely doesn't exist.
	DeleteMissing int
	// DeleteExisting is an operation that Deletes a key that likely exists.
	DeleteExisting int
	// DeleteRange is an operation that Deletes a key range that may contain values.
	DeleteRange int
	// DeleteRange is an operation that invokes DeleteRangeUsingTombstone.
	DeleteRangeUsingTombstone int
	// AddSSTable is an operations that ingests an SSTable with random KV pairs.
	AddSSTable int
	// Barrier is an operation that waits for in-flight writes to complete.
	Barrier int
}

// BatchOperationConfig configures the relative probability of generating a
// kv.Batch of some number of operations as well as the composition of the
// operations in the batch itself. These can be run in various ways including
// kv.DB.Run or kv.Txn.Run.
type BatchOperationConfig struct {
	Batch int
	Ops   ClientOperationConfig
}

// SplitConfig configures the relative probability of generating a Split
// operation.
type SplitConfig struct {
	// SplitNew is an operation that Splits at a key that has never previously
	// been a split point.
	SplitNew int
	// SplitAgain is an operation that Splits at a key that likely has
	// previously been a split point, though it may or may not have been merged
	// since.
	SplitAgain int
}

// MergeConfig configures the relative probability of generating a Merge
// operation.
type MergeConfig struct {
	// MergeNotSplit is an operation that Merges at a key that has never been
	// split at (meaning this should be a no-op).
	MergeNotSplit int
	// MergeIsSplit is an operation that Merges at a key that is likely to
	// currently be split.
	MergeIsSplit int
}

// ChangeReplicasConfig configures the relative probability of generating a
// ChangeReplicas operation.
type ChangeReplicasConfig struct {
	// AddVotingReplica adds a single voting replica.
	AddVotingReplica int
	// RemoveVotingReplica removes a single voting replica.
	RemoveVotingReplica int
	// AtomicSwapVotingReplica adds 1 voting replica and removes 1 voting replica
	// in a single ChangeReplicas call.
	AtomicSwapVotingReplica int
	// AddNonVotingReplica adds a single non-voting replica.
	AddNonVotingReplica int
	// RemoveNonVotingReplica removes a single non-voting replica.
	RemoveNonVotingReplica int
	// AtomicSwapNonVotingReplica adds 1 non-voting replica and removes 1 non-voting
	// replica in a single ChangeReplicas call.
	AtomicSwapNonVotingReplica int
	// PromoteReplica promotes a non-voting replica to voting.
	PromoteReplica int
	// DemoteReplica demotes a voting replica to non-voting.
	DemoteReplica int
}

// ChangeLeaseConfig configures the relative probability of generating an
// operation that causes a leaseholder change.
type ChangeLeaseConfig struct {
	// Transfer the lease to a random replica.
	TransferLease int
}

// ChangeSettingConfig configures the relative probability of generating a
// cluster setting change operation.
type ChangeSettingConfig struct {
	// SetLeaseType changes the default range lease type.
	SetLeaseType int
}

// ChangeZoneConfig configures the relative probability of generating a zone
// configuration change operation.
type ChangeZoneConfig struct {
	// ToggleGlobalReads sets global_reads to a new value.
	ToggleGlobalReads int
}

type SavepointConfig struct {
	// SavepointCreate is an operation that creates a new savepoint with a given id.
	SavepointCreate int
	// SavepointRelease is an operation that releases a savepoint with a given id.
	SavepointRelease int
	// SavepointRollback is an operation that rolls back a savepoint with a given id.
	SavepointRollback int
}

// newAllOperationsConfig returns a GeneratorConfig that exercises *all*
// options. You probably want NewDefaultConfig. Most of the time, these will be
// the same, but having both allows us to merge code for operations that do not
// yet pass (for example, if the new operation finds a kv bug or edge case).
func newAllOperationsConfig() GeneratorConfig {
	clientOpConfig := ClientOperationConfig{
		GetMissing:                                        1,
		GetMissingForUpdate:                               1,
		GetMissingForUpdateGuaranteedDurability:           1,
		GetMissingForUpdateSkipLocked:                     1,
		GetMissingForUpdateSkipLockedGuaranteedDurability: 1,
		GetMissingForShare:                                1,
		GetMissingForShareGuaranteedDurability:            1,
		GetMissingForShareSkipLocked:                      1,
		GetMissingForShareSkipLockedGuaranteedDurability:  1,
		GetExisting:                                        1,
		GetExistingForUpdate:                               1,
		GetExistingForUpdateGuaranteedDurability:           1,
		GetExistingForShare:                                1,
		GetExistingForShareGuaranteedDurability:            1,
		GetExistingSkipLocked:                              1,
		GetExistingForUpdateSkipLocked:                     1,
		GetExistingForUpdateSkipLockedGuaranteedDurability: 1,
		GetExistingForShareSkipLocked:                      1,
		GetExistingForShareSkipLockedGuaranteedDurability:  1,
		PutMissing:                        1,
		PutExisting:                       1,
		Scan:                              1,
		ScanForUpdate:                     1,
		ScanForUpdateGuaranteedDurability: 1,
		ScanForShare:                      1,
		ScanForShareGuaranteedDurability:  1,
		ScanSkipLocked:                    1,
		ScanForUpdateSkipLocked:           1,
		ScanForUpdateSkipLockedGuaranteedDurability: 1,
		ScanForShareSkipLocked:                      1,
		ScanForShareSkipLockedGuaranteedDurability:  1,
		ReverseScan:                                        1,
		ReverseScanForUpdate:                               1,
		ReverseScanForUpdateGuaranteedDurability:           1,
		ReverseScanForShare:                                1,
		ReverseScanForShareGuaranteedDurability:            1,
		ReverseScanSkipLocked:                              1,
		ReverseScanForUpdateSkipLocked:                     1,
		ReverseScanForUpdateSkipLockedGuaranteedDurability: 1,
		ReverseScanForShareSkipLocked:                      1,
		ReverseScanForShareSkipLockedGuaranteedDurability:  1,
		DeleteMissing:                                      1,
		DeleteExisting:                                     1,
		DeleteRange:                                        1,
		DeleteRangeUsingTombstone:                          1,
		AddSSTable:                                         1,
		Barrier:                                            1,
	}
	batchOpConfig := BatchOperationConfig{
		Batch: 4,
		Ops:   clientOpConfig,
	}
	// SavepointConfig is only relevant in ClosureTxnConfig.
	savepointConfig := SavepointConfig{
		SavepointCreate:   1,
		SavepointRelease:  1,
		SavepointRollback: 1,
	}
	return GeneratorConfig{Ops: OperationConfig{
		DB:    clientOpConfig,
		Batch: batchOpConfig,
		ClosureTxn: ClosureTxnConfig{
			CommitSerializable:         2,
			CommitSnapshot:             2,
			CommitReadCommitted:        2,
			RollbackSerializable:       2,
			RollbackSnapshot:           2,
			RollbackReadCommitted:      2,
			CommitSerializableInBatch:  2,
			CommitSnapshotInBatch:      2,
			CommitReadCommittedInBatch: 2,
			TxnClientOps:               clientOpConfig,
			TxnBatchOps:                batchOpConfig,
			CommitBatchOps:             clientOpConfig,
			SavepointOps:               savepointConfig,
		},
		Split: SplitConfig{
			SplitNew:   1,
			SplitAgain: 1,
		},
		Merge: MergeConfig{
			MergeNotSplit: 1,
			MergeIsSplit:  1,
		},
		ChangeReplicas: ChangeReplicasConfig{
			AddVotingReplica:           1,
			RemoveVotingReplica:        1,
			AtomicSwapVotingReplica:    1,
			AddNonVotingReplica:        1,
			RemoveNonVotingReplica:     1,
			AtomicSwapNonVotingReplica: 1,
			PromoteReplica:             1,
			DemoteReplica:              1,
		},
		ChangeLease: ChangeLeaseConfig{
			TransferLease: 1,
		},
		ChangeSetting: ChangeSettingConfig{
			SetLeaseType: 1,
		},
		ChangeZone: ChangeZoneConfig{
			ToggleGlobalReads: 1,
		},
	}}
}

// NewDefaultConfig returns a GeneratorConfig that is a reasonable default
// starting point for general KV usage. Nemesis test variants that want to
// stress particular areas may want to start with this and eliminate some
// operations/make some operations more likely.
func NewDefaultConfig() GeneratorConfig {
	config := newAllOperationsConfig()
	// DeleteRangeUsingTombstone does not support transactions.
	config.Ops.ClosureTxn.TxnClientOps.DeleteRangeUsingTombstone = 0
	config.Ops.ClosureTxn.TxnBatchOps.Ops.DeleteRangeUsingTombstone = 0
	config.Ops.ClosureTxn.CommitBatchOps.DeleteRangeUsingTombstone = 0
	// DeleteRangeUsingTombstone does in principle support batches, but
	// in kvnemesis we don't let it span ranges non-atomically (as it
	// is allowed to do in CRDB). The generator already tries to avoid
	// crossing range boundaries quite a fair bit, so we could enable this
	// after some investigation to ensure that significant enough coverage
	// remains on the batch path.
	// Note also that at the time of writing `config.Ops.Batch` is cleared in its
	// entirety below, so changing this line alonewon't have an effect.
	config.Ops.Batch.Ops.DeleteRangeUsingTombstone = 0
	// TODO(sarkesian): Enable DeleteRange in comingled batches once #71236 is fixed.
	config.Ops.ClosureTxn.CommitBatchOps.DeleteRange = 0
	config.Ops.ClosureTxn.TxnBatchOps.Ops.DeleteRange = 0
	// TODO(dan): This fails with a WriteTooOld error if the same key is Put twice
	// in a single batch. However, if the same Batch is committed using txn.Run,
	// then it works and only the last one is materialized. We could make the
	// db.Run behavior match txn.Run by ensuring that all requests in a
	// nontransactional batch are disjoint and upgrading to a transactional batch
	// (see CrossRangeTxnWrapperSender) if they are. roachpb.SpanGroup can be used
	// to efficiently check this.
	//
	// TODO(tbg): could make this `config.Ops.Batch.Ops.PutExisting = 0` (and
	// DeleteRange, etc, all ops that can overwrite existing keys basically), as
	// #46081 has long been fixed. Then file an issue about generating
	// non-self-overlapping operations for batches.
	config.Ops.Batch = BatchOperationConfig{}
	// SkipLocked is a batch-level attribute, not an operation-level attribute. To
	// avoid mixing skip locked and non-skip locked requests, we disable these ops
	// in the batchOpConfig.
	// TODO(nvanbenschoten): support multi-operation SkipLocked batches.
	for _, batchOps := range []*ClientOperationConfig{
		&config.Ops.Batch.Ops,
		&config.Ops.ClosureTxn.TxnBatchOps.Ops,
		&config.Ops.ClosureTxn.CommitBatchOps,
	} {
		batchOps.GetMissingSkipLocked = 0
		batchOps.GetMissingForUpdateSkipLocked = 0
		batchOps.GetMissingForUpdateSkipLockedGuaranteedDurability = 0
		batchOps.GetMissingForShareSkipLocked = 0
		batchOps.GetMissingForShareSkipLockedGuaranteedDurability = 0
		batchOps.GetExistingSkipLocked = 0
		batchOps.GetExistingForUpdateSkipLocked = 0
		batchOps.GetExistingForUpdateSkipLockedGuaranteedDurability = 0
		batchOps.GetExistingForShareSkipLocked = 0
		batchOps.GetExistingForShareSkipLockedGuaranteedDurability = 0
		batchOps.ScanSkipLocked = 0
		batchOps.ScanForUpdateSkipLocked = 0
		batchOps.ScanForUpdateSkipLockedGuaranteedDurability = 0
		batchOps.ScanForShareSkipLocked = 0
		batchOps.ScanForShareSkipLockedGuaranteedDurability = 0
		batchOps.ReverseScanSkipLocked = 0
		batchOps.ReverseScanForUpdateSkipLocked = 0
		batchOps.ReverseScanForUpdateSkipLockedGuaranteedDurability = 0
		batchOps.ReverseScanForShareSkipLocked = 0
		batchOps.ReverseScanForShareSkipLockedGuaranteedDurability = 0
	}
	// AddSSTable cannot be used in transactions, nor in batches.
	config.Ops.Batch.Ops.AddSSTable = 0
	config.Ops.ClosureTxn.CommitBatchOps.AddSSTable = 0
	config.Ops.ClosureTxn.TxnClientOps.AddSSTable = 0
	config.Ops.ClosureTxn.TxnBatchOps.Ops.AddSSTable = 0
	// Barrier cannot be used in batches, and we omit it in txns too because it
	// can result in spurious RangeKeyMismatchErrors that fail the txn operation.
	config.Ops.Batch.Ops.Barrier = 0
	config.Ops.ClosureTxn.CommitBatchOps.Barrier = 0
	config.Ops.ClosureTxn.TxnClientOps.Barrier = 0
	config.Ops.ClosureTxn.TxnBatchOps.Ops.Barrier = 0
	return config
}

// GeneratorDataTableID is the table ID that corresponds to GeneratorDataSpan.
// This must be a table ID that is not used in a new cluster.
var GeneratorDataTableID = bootstrap.TestingMinUserDescID()

// GeneratorDataSpan returns a span that contains all of the operations created
// by this Generator.
func GeneratorDataSpan() roachpb.Span {
	return roachpb.Span{
		Key:    keys.SystemSQLCodec.TablePrefix(GeneratorDataTableID),
		EndKey: keys.SystemSQLCodec.TablePrefix(GeneratorDataTableID + 1),
	}
}

// GetReplicasFn is a function that returns the current voting and non-voting
// replicas, respectively, for the range containing a key.
type GetReplicasFn func(roachpb.Key) ([]roachpb.ReplicationTarget, []roachpb.ReplicationTarget)

// Generator incrementally constructs KV traffic designed to maximally test edge
// cases.
//
// The expected usage is that a number of concurrent worker threads will each
// repeatedly ask for a Step, finish executing it, then ask for the next Step.
//
// A Step consists of a single Operation, which is a unit of work that must be
// done serially. It often corresponds 1:1 to a single call to some method on
// the KV api (such as Get or Put), but some Operations have a set of steps
// (such as using a transaction).
//
// Generator in itself is deterministic, but it's intended usage is that
// multiple worker goroutines take turns pulling steps (sequentially) which they
// then execute concurrently. To improve the efficiency of this pattern,
// Generator will track which splits and merges could possibly have taken place
// and takes this into account when generating operations. For example,
// Generator won't take a OpMergeIsSplit step if it has never previously emitted
// a split, but it may emit an OpMerge once it has produced an OpSplit even
// though the worker executing the split may find that the merge has not yet
// been executed.
type Generator struct {
	// TODO(dan): This is awkward, unify Generator and generator.
	mu struct {
		syncutil.Mutex
		generator
	}
}

// MakeGenerator constructs a Generator.
func MakeGenerator(config GeneratorConfig, replicasFn GetReplicasFn) (*Generator, error) {
	if config.NumNodes <= 0 {
		return nil, errors.Errorf(`NumNodes must be positive got: %d`, config.NumNodes)
	}
	if config.NumReplicas <= 0 {
		return nil, errors.Errorf(`NumReplicas must be positive got: %d`, config.NumReplicas)
	}
	if config.NumReplicas > config.NumNodes {
		return nil, errors.Errorf(`NumReplicas (%d) must <= NumNodes (%d)`,
			config.NumReplicas, config.NumNodes)
	}
	g := &Generator{}
	g.mu.generator = generator{
		Config:           config,
		replicasFn:       replicasFn,
		keys:             make(map[string]struct{}),
		currentSplits:    make(map[string]struct{}),
		historicalSplits: make(map[string]struct{}),
	}
	return g, nil
}

// RandStep returns a single randomly generated next operation to execute.
//
// RandStep is concurrency safe.
func (g *Generator) RandStep(rng *rand.Rand) Step {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.mu.RandStep(rng)
}

type generator struct {
	Config     GeneratorConfig
	replicasFn GetReplicasFn

	seqGen kvnemesisutil.Seq

	// keys is the set of every key that has been written to, including those
	// deleted or in rolled back transactions.
	keys map[string]struct{}

	// currentSplits is approximately the set of every split that has been made
	// within DataSpan. The exact accounting is hard because Generator can hand
	// out a concurrent split and merge for the same key, which is racey. These
	// races can result in a currentSplit that is not in fact a split at the KV
	// level. Luckily we don't need exact accounting.
	currentSplits map[string]struct{}
	// historicalSplits is the set of every key for which a split has been
	// emitted, regardless of whether the split has since been applied or been
	// merged away again.
	historicalSplits map[string]struct{}
}

// RandStep returns a single randomly generated next operation to execute.
//
// RandStep is not concurrency safe.
func (g *generator) RandStep(rng *rand.Rand) Step {
	var allowed []opGen
	g.registerClientOps(&allowed, &g.Config.Ops.DB)
	g.registerBatchOps(&allowed, &g.Config.Ops.Batch)
	g.registerClosureTxnOps(&allowed, &g.Config.Ops.ClosureTxn)

	addOpGen(&allowed, randSplitNew, g.Config.Ops.Split.SplitNew)
	if len(g.historicalSplits) > 0 {
		addOpGen(&allowed, randSplitAgain, g.Config.Ops.Split.SplitAgain)
	}

	addOpGen(&allowed, randMergeNotSplit, g.Config.Ops.Merge.MergeNotSplit)
	if len(g.currentSplits) > 0 {
		addOpGen(&allowed, randMergeIsSplit, g.Config.Ops.Merge.MergeIsSplit)
	}

	key := randKey(rng)
	voters, nonVoters := g.replicasFn(roachpb.Key(key))
	numVoters, numNonVoters := len(voters), len(nonVoters)
	numReplicas := numVoters + numNonVoters
	if numReplicas < g.Config.NumNodes {
		addVoterFn := makeAddReplicaFn(key, voters, false /* atomicSwap */, true /* voter */)
		addOpGen(&allowed, addVoterFn, g.Config.Ops.ChangeReplicas.AddVotingReplica)
		addNonVoterFn := makeAddReplicaFn(key, nonVoters, false /* atomicSwap */, false /* voter */)
		addOpGen(&allowed, addNonVoterFn, g.Config.Ops.ChangeReplicas.AddNonVotingReplica)
	}
	if numReplicas == g.Config.NumReplicas && numReplicas < g.Config.NumNodes {
		atomicSwapVoterFn := makeAddReplicaFn(key, voters, true /* atomicSwap */, true /* voter */)
		addOpGen(&allowed, atomicSwapVoterFn, g.Config.Ops.ChangeReplicas.AtomicSwapVotingReplica)
		if numNonVoters > 0 {
			atomicSwapNonVoterFn := makeAddReplicaFn(key, nonVoters, true /* atomicSwap */, false /* voter */)
			addOpGen(&allowed, atomicSwapNonVoterFn, g.Config.Ops.ChangeReplicas.AtomicSwapNonVotingReplica)
		}
	}
	if numReplicas > g.Config.NumReplicas {
		removeVoterFn := makeRemoveReplicaFn(key, voters, true /* voter */)
		addOpGen(&allowed, removeVoterFn, g.Config.Ops.ChangeReplicas.RemoveVotingReplica)
		if numNonVoters > 0 {
			removeNonVoterFn := makeRemoveReplicaFn(key, nonVoters, false /* voter */)
			addOpGen(&allowed, removeNonVoterFn, g.Config.Ops.ChangeReplicas.RemoveNonVotingReplica)
		}
	}
	if numVoters > 0 {
		demoteVoterFn := makeDemoteReplicaFn(key, voters)
		addOpGen(&allowed, demoteVoterFn, g.Config.Ops.ChangeReplicas.DemoteReplica)
	}
	if numNonVoters > 0 {
		promoteNonVoterFn := makePromoteReplicaFn(key, nonVoters)
		addOpGen(&allowed, promoteNonVoterFn, g.Config.Ops.ChangeReplicas.PromoteReplica)
	}
	transferLeaseFn := makeTransferLeaseFn(key, append(voters, nonVoters...))
	addOpGen(&allowed, transferLeaseFn, g.Config.Ops.ChangeLease.TransferLease)

	addOpGen(&allowed, setLeaseType, g.Config.Ops.ChangeSetting.SetLeaseType)
	addOpGen(&allowed, toggleGlobalReads, g.Config.Ops.ChangeZone.ToggleGlobalReads)

	return step(g.selectOp(rng, allowed))
}

func (g *generator) nextSeq() kvnemesisutil.Seq {
	g.seqGen++
	return g.seqGen
}

type opGenFunc func(*generator, *rand.Rand) Operation

type opGen struct {
	fn     opGenFunc
	weight int
}

func addOpGen(valid *[]opGen, fn opGenFunc, weight int) {
	*valid = append(*valid, opGen{fn: fn, weight: weight})
}

func (g *generator) selectOp(rng *rand.Rand, contextuallyValid []opGen) Operation {
	var total int
	for _, x := range contextuallyValid {
		total += x.weight
	}
	target := rng.Intn(total)
	var sum int
	for _, x := range contextuallyValid {
		sum += x.weight
		if sum > target {
			return x.fn(g, rng)
		}
	}
	panic(`unreachable`)
}

func (g *generator) registerClientOps(allowed *[]opGen, c *ClientOperationConfig) {
	addOpGen(allowed, randGetMissing, c.GetMissing)
	addOpGen(allowed, randGetMissingForUpdate, c.GetMissingForUpdate)
	addOpGen(
		allowed, randGetMissingForUpdateGuaranteedDurability, c.GetMissingForUpdateGuaranteedDurability,
	)
	addOpGen(allowed, randGetMissingForShare, c.GetMissingForShare)
	addOpGen(
		allowed, randGetMissingForShareGuaranteedDurability, c.GetMissingForShareGuaranteedDurability,
	)
	addOpGen(allowed, randGetMissingSkipLocked, c.GetMissingSkipLocked)
	addOpGen(allowed, randGetMissingForUpdateSkipLocked, c.GetMissingForUpdateSkipLocked)
	addOpGen(
		allowed,
		randGetMissingForUpdateSkipLockedGuaranteedDurability,
		c.GetMissingForUpdateSkipLockedGuaranteedDurability,
	)
	addOpGen(allowed, randGetMissingForShareSkipLocked, c.GetMissingForShareSkipLocked)
	addOpGen(
		allowed,
		randGetMissingForShareSkipLockedGuaranteedDurability,
		c.GetMissingForShareSkipLockedGuaranteedDurability,
	)
	addOpGen(allowed, randPutMissing, c.PutMissing)
	addOpGen(allowed, randDelMissing, c.DeleteMissing)
	if len(g.keys) > 0 {
		addOpGen(allowed, randGetExisting, c.GetExisting)
		addOpGen(allowed, randGetExistingForUpdate, c.GetExistingForUpdate)
		addOpGen(
			allowed,
			randGetExistingForUpdateGuaranteedDurability,
			c.GetExistingForUpdateGuaranteedDurability,
		)
		addOpGen(allowed, randGetExistingForShare, c.GetExistingForShare)
		addOpGen(
			allowed,
			randGetExistingForShareGuaranteedDurability,
			c.GetExistingForShareGuaranteedDurability,
		)
		addOpGen(allowed, randGetExistingSkipLocked, c.GetExistingSkipLocked)
		addOpGen(allowed, randGetExistingForUpdateSkipLocked, c.GetExistingForUpdateSkipLocked)
		addOpGen(
			allowed,
			randGetExistingForUpdateSkipLockedGuaranteedDurability,
			c.GetExistingForUpdateSkipLockedGuaranteedDurability,
		)
		addOpGen(allowed, randGetExistingForShareSkipLocked, c.GetExistingForShareSkipLocked)
		addOpGen(
			allowed,
			randGetExistingForShareSkipLockedGuaranteedDurability,
			c.GetExistingForShareSkipLockedGuaranteedDurability,
		)
		addOpGen(allowed, randPutExisting, c.PutExisting)
		addOpGen(allowed, randDelExisting, c.DeleteExisting)
	}
	addOpGen(allowed, randScan, c.Scan)
	addOpGen(allowed, randScanForUpdate, c.ScanForUpdate)
	addOpGen(allowed, randScanForUpdateGuaranteedDurability, c.ScanForUpdateGuaranteedDurability)
	addOpGen(allowed, randScanForShare, c.ScanForShare)
	addOpGen(allowed, randScanForShareGuaranteedDurability, c.ScanForShareGuaranteedDurability)
	addOpGen(allowed, randScanSkipLocked, c.ScanSkipLocked)
	addOpGen(allowed, randScanForUpdateSkipLocked, c.ScanForUpdateSkipLocked)
	addOpGen(
		allowed,
		randScanForUpdateSkipLockedGuaranteedDurability,
		c.ScanForUpdateSkipLockedGuaranteedDurability,
	)
	addOpGen(allowed, randScanForShareSkipLocked, c.ScanForShareSkipLocked)
	addOpGen(
		allowed,
		randScanForShareSkipLockedGuaranteedDurability,
		c.ScanForShareSkipLockedGuaranteedDurability,
	)
	addOpGen(allowed, randReverseScan, c.ReverseScan)
	addOpGen(allowed, randReverseScanForUpdate, c.ReverseScanForUpdate)
	addOpGen(
		allowed,
		randReverseScanForUpdateGuaranteedDurability,
		c.ReverseScanForUpdateGuaranteedDurability,
	)
	addOpGen(allowed, randReverseScanForShare, c.ReverseScanForShare)
	addOpGen(
		allowed, randReverseScanForShareGuaranteedDurability, c.ReverseScanForShareGuaranteedDurability,
	)
	addOpGen(allowed, randReverseScanSkipLocked, c.ReverseScanSkipLocked)
	addOpGen(allowed, randReverseScanForUpdateSkipLocked, c.ReverseScanForUpdateSkipLocked)
	addOpGen(
		allowed,
		randReverseScanForUpdateSkipLockedGuaranteedDurability,
		c.ReverseScanForUpdateSkipLockedGuaranteedDurability,
	)
	addOpGen(allowed, randReverseScanForShareSkipLocked, c.ReverseScanForShareSkipLocked)
	addOpGen(
		allowed,
		randReverseScanForShareSkipLockedGuaranteedDurability,
		c.ReverseScanForShareSkipLockedGuaranteedDurability,
	)
	addOpGen(allowed, randDelRange, c.DeleteRange)
	addOpGen(allowed, randDelRangeUsingTombstone, c.DeleteRangeUsingTombstone)
	addOpGen(allowed, randAddSSTable, c.AddSSTable)
	addOpGen(allowed, randBarrier, c.Barrier)
}

func (g *generator) registerBatchOps(allowed *[]opGen, c *BatchOperationConfig) {
	addOpGen(allowed, makeRandBatch(&c.Ops), c.Batch)
}

func randGetMissing(_ *generator, rng *rand.Rand) Operation {
	return get(randKey(rng))
}

func randGetMissingForUpdate(g *generator, rng *rand.Rand) Operation {
	op := randGetMissing(g, rng)
	op.Get.ForUpdate = true
	return op
}

func randGetMissingForUpdateGuaranteedDurability(g *generator, rng *rand.Rand) Operation {
	op := randGetMissing(g, rng)
	op.Get.ForUpdate = true
	op.Get.GuaranteedDurability = true
	return op
}

func randGetMissingForShare(g *generator, rng *rand.Rand) Operation {
	op := randGetMissing(g, rng)
	op.Get.ForShare = true
	return op
}

func randGetMissingForShareGuaranteedDurability(g *generator, rng *rand.Rand) Operation {
	op := randGetMissing(g, rng)
	op.Get.ForShare = true
	op.Get.GuaranteedDurability = true
	return op
}

func randGetMissingSkipLocked(g *generator, rng *rand.Rand) Operation {
	op := randGetMissing(g, rng)
	op.Get.SkipLocked = true
	return op
}

func randGetMissingForUpdateSkipLocked(g *generator, rng *rand.Rand) Operation {
	op := randGetMissingForUpdate(g, rng)
	op.Get.SkipLocked = true
	return op
}

func randGetMissingForUpdateSkipLockedGuaranteedDurability(g *generator, rng *rand.Rand) Operation {
	op := randGetMissingForUpdate(g, rng)
	op.Get.SkipLocked = true
	op.Get.GuaranteedDurability = true
	return op
}

func randGetMissingForShareSkipLocked(g *generator, rng *rand.Rand) Operation {
	op := randGetMissingForShare(g, rng)
	op.Get.SkipLocked = true
	return op
}

func randGetMissingForShareSkipLockedGuaranteedDurability(g *generator, rng *rand.Rand) Operation {
	op := randGetMissingForShare(g, rng)
	op.Get.SkipLocked = true
	op.Get.GuaranteedDurability = true
	return op
}

func randGetExisting(g *generator, rng *rand.Rand) Operation {
	key := randMapKey(rng, g.keys)
	return get(key)
}

func randGetExistingForUpdate(g *generator, rng *rand.Rand) Operation {
	op := randGetExisting(g, rng)
	op.Get.ForUpdate = true
	return op
}

func randGetExistingForUpdateGuaranteedDurability(g *generator, rng *rand.Rand) Operation {
	op := randGetExisting(g, rng)
	op.Get.ForUpdate = true
	op.Get.GuaranteedDurability = true
	return op
}

func randGetExistingForShare(g *generator, rng *rand.Rand) Operation {
	op := randGetExisting(g, rng)
	op.Get.ForShare = true
	return op
}

func randGetExistingForShareGuaranteedDurability(g *generator, rng *rand.Rand) Operation {
	op := randGetExisting(g, rng)
	op.Get.ForShare = true
	op.Get.GuaranteedDurability = true
	return op
}

func randGetExistingSkipLocked(g *generator, rng *rand.Rand) Operation {
	op := randGetExisting(g, rng)
	op.Get.SkipLocked = true
	return op
}

func randGetExistingForUpdateSkipLocked(g *generator, rng *rand.Rand) Operation {
	op := randGetExistingForUpdate(g, rng)
	op.Get.SkipLocked = true
	return op
}

func randGetExistingForUpdateSkipLockedGuaranteedDurability(
	g *generator, rng *rand.Rand,
) Operation {
	op := randGetExistingForUpdate(g, rng)
	op.Get.SkipLocked = true
	op.Get.GuaranteedDurability = true
	return op
}

func randGetExistingForShareSkipLocked(g *generator, rng *rand.Rand) Operation {
	op := randGetExistingForShare(g, rng)
	op.Get.SkipLocked = true
	return op
}

func randGetExistingForShareSkipLockedGuaranteedDurability(g *generator, rng *rand.Rand) Operation {
	op := randGetExistingForShare(g, rng)
	op.Get.SkipLocked = true
	op.Get.GuaranteedDurability = true
	return op
}

func randPutMissing(g *generator, rng *rand.Rand) Operation {
	seq := g.nextSeq()
	key := randKey(rng)
	g.keys[key] = struct{}{}
	return put(key, seq)
}

func randPutExisting(g *generator, rng *rand.Rand) Operation {
	seq := g.nextSeq()
	key := randMapKey(rng, g.keys)
	return put(key, seq)
}

func randAddSSTable(g *generator, rng *rand.Rand) Operation {
	ctx := context.Background()

	sstTimestamp := hlc.MinTimestamp // replaced via SSTTimestampToRequestTimestamp
	numPointKeys := rng.Intn(16) + 1 // number of point keys (but see below)
	numRangeKeys := rng.Intn(3) + 1  // number of range keys (but see below)
	probReplace := 0.2               // probability to replace existing key, if possible
	probTombstone := 0.2             // probability to write a tombstone
	asWrites := rng.Float64() < 0.2  // IngestAsWrites

	if r := rng.Float64(); r < 0.8 {
		// 80% probability of only point keys.
		numRangeKeys = 0
	} else if r < 0.9 {
		// 10% probability of only range keys.
		numPointKeys = 0
	}
	// else 10% probability of mixed point/range keys.

	// AddSSTable requests cannot span multiple ranges, so we try to fit them
	// within an existing range. This may race with a concurrent split, in which
	// case the AddSSTable will fail, but that's ok -- most should still succeed.
	rangeStart, rangeEnd := randRangeSpan(rng, g.currentSplits)
	curKeys := keysBetween(g.keys, rangeStart, rangeEnd)

	// Generate keys first, to write them in order and without duplicates. We pick
	// either existing or new keys depending on probReplace, making sure they're
	// unique. We generate keys both for point keys and for the start bound of
	// range keys, such that we afterwards can pick out a set of range keys that
	// don't overlap any other keys.
	sstKeys := []string{}
	sstKeysMap := map[string]struct{}{}
	for len(sstKeys) < numPointKeys+numRangeKeys {
		var key string
		if len(curKeys) > 0 && rng.Float64() < probReplace {
			// Pick a random existing key when appropriate.
			key = curKeys[rng.Intn(len(curKeys))]
		} else {
			// Generate a new random key in the range.
			key = randKeyBetween(rng, rangeStart, rangeEnd)
		}
		if _, ok := sstKeysMap[key]; !ok {
			sstKeysMap[key] = struct{}{}
			sstKeys = append(sstKeys, key)
		}
	}
	sort.Strings(sstKeys)

	// Pick range key slots. We generated range key start bounds and point keys in
	// sstKeys above, so we can pick random free range key slots between a random
	// sstKeys and the next one. Later, we'll randomly shorten the range keys.
	sstRangeKeysSlots := map[string]string{} // startKey->endKey
	for len(sstRangeKeysSlots) < numRangeKeys {
		i := rng.Intn(len(sstKeys))
		startKey := sstKeys[i]
		endKey := tk(math.MaxUint64)
		if i+1 < len(sstKeys) {
			endKey = sstKeys[i+1]
		}
		if _, ok := sstRangeKeysSlots[startKey]; !ok {
			sstRangeKeysSlots[startKey] = endKey
		}
	}

	// Separate sstKeys out into point keys and range keys. For the range keys,
	// randomly constrain the bounds within their slot.
	var sstPointKeys []storage.MVCCKey
	var sstRangeKeys []storage.MVCCRangeKey
	for _, key := range sstKeys {
		if endKey, ok := sstRangeKeysSlots[key]; !ok {
			// Point key. Just add it to sstPointKeys.
			sstPointKeys = append(sstPointKeys, storage.MVCCKey{
				Key:       roachpb.Key(key),
				Timestamp: sstTimestamp,
			})
		} else {
			// Range key. With 50% probability, shorten the start/end keys.
			if rng.Float64() < 0.5 {
				key = randKeyBetween(rng, key, endKey)
			}
			if rng.Float64() < 0.5 {
				endKey = randKeyBetween(rng, tk(fk(key)+1), endKey)
			}
			sstRangeKeys = append(sstRangeKeys, storage.MVCCRangeKey{
				StartKey:  roachpb.Key(key),
				EndKey:    roachpb.Key(endKey),
				Timestamp: sstTimestamp,
			})
		}
	}

	// Determine the SST span.
	sstSpan := roachpb.Span{
		Key:    roachpb.Key(sstKeys[0]),
		EndKey: roachpb.Key(tk(fk(sstKeys[len(sstKeys)-1]) + 1)),
	}
	if len(sstRangeKeys) > 0 {
		if last := sstRangeKeys[len(sstRangeKeys)-1]; last.EndKey.Compare(sstSpan.EndKey) > 0 {
			sstSpan.EndKey = last.EndKey.Clone()
		}
	}

	// Unlike other write operations, AddSSTable sends raw MVCC values directly
	// through to storage. We therefore don't need to pass the sequence number via
	// the RequestHeader, but instead write them directly into the MVCCValueHeader
	// of the MVCC values.
	seq := g.nextSeq()
	sstValueHeader := enginepb.MVCCValueHeader{}
	sstValueHeader.KVNemesisSeq.Set(seq)
	sstValue := storage.MVCCValue{
		MVCCValueHeader: sstValueHeader,
		Value:           roachpb.MakeValueFromString(sv(seq)),
	}
	sstTombstone := storage.MVCCValue{MVCCValueHeader: sstValueHeader}

	// Write key/value pairs to the SST.
	f := &storage.MemObject{}
	st := cluster.MakeTestingClusterSettings()
	w := storage.MakeIngestionSSTWriter(ctx, st, f)
	defer w.Close()

	for _, key := range sstPointKeys {
		// Randomly write a tombstone instead of a value.
		value := sstValue
		if rng.Float64() < probTombstone {
			value = sstTombstone
		}
		if err := w.PutMVCC(key, value); err != nil {
			panic(err)
		}
	}
	for _, rangeKey := range sstRangeKeys {
		// Range keys are always range tombstones.
		if err := w.PutMVCCRangeKey(rangeKey, sstTombstone); err != nil {
			panic(err)
		}
	}
	if err := w.Finish(); err != nil {
		panic(err)
	}

	return addSSTable(f.Data(), sstSpan, sstTimestamp, seq, asWrites)
}

func randBarrier(g *generator, rng *rand.Rand) Operation {
	withLAI := rng.Float64() < 0.5
	var key, endKey string
	if withLAI {
		// Barriers requesting LAIs can't span multiple ranges, so we try to fit
		// them within an existing range. This may race with a concurrent split, in
		// which case the Barrier will fail, but that's ok -- most should still
		// succeed. These errors are ignored by the validator.
		key, endKey = randRangeSpan(rng, g.currentSplits)
	} else {
		key, endKey = randSpan(rng)
	}
	return barrier(key, endKey, withLAI)
}

func randScan(g *generator, rng *rand.Rand) Operation {
	key, endKey := randSpan(rng)
	return scan(key, endKey)
}

func randScanForUpdate(g *generator, rng *rand.Rand) Operation {
	op := randScan(g, rng)
	op.Scan.ForUpdate = true
	return op
}

func randScanForUpdateGuaranteedDurability(g *generator, rng *rand.Rand) Operation {
	op := randScan(g, rng)
	op.Scan.ForUpdate = true
	op.Scan.GuaranteedDurability = true
	return op
}

func randScanForShare(g *generator, rng *rand.Rand) Operation {
	op := randScan(g, rng)
	op.Scan.ForShare = true
	return op
}

func randScanForShareGuaranteedDurability(g *generator, rng *rand.Rand) Operation {
	op := randScan(g, rng)
	op.Scan.ForShare = true
	op.Scan.GuaranteedDurability = true
	return op
}

func randScanSkipLocked(g *generator, rng *rand.Rand) Operation {
	op := randScan(g, rng)
	op.Scan.SkipLocked = true
	return op
}

func randScanForUpdateSkipLocked(g *generator, rng *rand.Rand) Operation {
	op := randScanForUpdate(g, rng)
	op.Scan.SkipLocked = true
	return op
}

func randScanForUpdateSkipLockedGuaranteedDurability(g *generator, rng *rand.Rand) Operation {
	op := randScanForUpdate(g, rng)
	op.Scan.SkipLocked = true
	op.Scan.GuaranteedDurability = true
	return op
}

func randScanForShareSkipLocked(g *generator, rng *rand.Rand) Operation {
	op := randScanForShare(g, rng)
	op.Scan.SkipLocked = true
	return op
}

func randScanForShareSkipLockedGuaranteedDurability(g *generator, rng *rand.Rand) Operation {
	op := randScanForShare(g, rng)
	op.Scan.SkipLocked = true
	op.Scan.GuaranteedDurability = true
	return op
}

func randReverseScan(g *generator, rng *rand.Rand) Operation {
	op := randScan(g, rng)
	op.Scan.Reverse = true
	return op
}

func randReverseScanForUpdate(g *generator, rng *rand.Rand) Operation {
	op := randReverseScan(g, rng)
	op.Scan.ForUpdate = true
	return op
}

func randReverseScanForUpdateGuaranteedDurability(g *generator, rng *rand.Rand) Operation {
	op := randReverseScan(g, rng)
	op.Scan.ForUpdate = true
	op.Scan.GuaranteedDurability = true
	return op
}

func randReverseScanForShare(g *generator, rng *rand.Rand) Operation {
	op := randReverseScan(g, rng)
	op.Scan.ForShare = true
	return op
}

func randReverseScanForShareGuaranteedDurability(g *generator, rng *rand.Rand) Operation {
	op := randReverseScan(g, rng)
	op.Scan.ForShare = true
	op.Scan.GuaranteedDurability = true
	return op
}

func randReverseScanSkipLocked(g *generator, rng *rand.Rand) Operation {
	op := randReverseScan(g, rng)
	op.Scan.SkipLocked = true
	return op
}

func randReverseScanForUpdateSkipLocked(g *generator, rng *rand.Rand) Operation {
	op := randReverseScanForUpdate(g, rng)
	op.Scan.SkipLocked = true
	return op
}

func randReverseScanForUpdateSkipLockedGuaranteedDurability(
	g *generator, rng *rand.Rand,
) Operation {
	op := randReverseScanForUpdate(g, rng)
	op.Scan.SkipLocked = true
	op.Scan.GuaranteedDurability = true
	return op
}

func randReverseScanForShareSkipLocked(g *generator, rng *rand.Rand) Operation {
	op := randReverseScanForShare(g, rng)
	op.Scan.SkipLocked = true
	return op
}

func randReverseScanForShareSkipLockedGuaranteedDurability(g *generator, rng *rand.Rand) Operation {
	op := randReverseScanForShare(g, rng)
	op.Scan.SkipLocked = true
	op.Scan.GuaranteedDurability = true
	return op
}

func randDelMissing(g *generator, rng *rand.Rand) Operation {
	key := randKey(rng)
	g.keys[key] = struct{}{}
	seq := g.nextSeq()
	return del(key, seq)
}

func randDelExisting(g *generator, rng *rand.Rand) Operation {
	key := randMapKey(rng, g.keys)
	seq := g.nextSeq()
	return del(key, seq)
}

func randDelRange(g *generator, rng *rand.Rand) Operation {
	// We don't write any new keys to `g.keys` on a DeleteRange operation,
	// because DelRange(..) only deletes existing keys.
	key, endKey := randSpan(rng)
	seq := g.nextSeq()
	return delRange(key, endKey, seq)
}

func randDelRangeUsingTombstone(g *generator, rng *rand.Rand) Operation {
	return randDelRangeUsingTombstoneImpl(g.currentSplits, g.keys, g.nextSeq, rng)
}

func randDelRangeUsingTombstoneImpl(
	currentSplits, keys map[string]struct{}, nextSeq func() kvnemesisutil.Seq, rng *rand.Rand,
) Operation {
	yn := func(probY float64) bool {
		return rng.Float64() <= probY
	}

	var k, ek string
	if yn(0.90) {
		// 90% chance of picking an entire existing range.
		//
		// In kvnemesis, DeleteRangeUsingTombstone is prevented from spanning ranges since
		// CRDB executes such requests non-atomically and so we can't verify them
		// well. Thus, pick spans that are likely single-range most of the time.
		//
		// 75% (of the 90%) of the time we'll also modify the bounds.
		k, ek = randRangeSpan(rng, currentSplits)
		if yn(0.5) {
			// In 50% of cases, move startKey forward.
			k = randKeyBetween(rng, k, ek)
		}
		if yn(0.5) {
			// In 50% of cases, move endKey backward.
			nk := fk(k) + 1
			nek := fk(ek)
			if nek < math.MaxUint64 {
				nek++
			}
			ek = randKeyBetween(rng, tk(nk), tk(nek))
		}
	} else if yn(0.5) {
		// (100%-90%)*50% = 5% chance of turning the span we have now into a
		// point write. Half the time random key, otherwise prefer existing key.
		if yn(0.5) || len(keys) == 0 {
			k = randKey(rng)
		} else {
			k = randMapKey(rng, keys)
		}
		ek = tk(fk(k) + 1)
	} else {
		// 5% chance of picking a completely random span. This will often span range
		// boundaries and be rejected, so these are essentially doomed to fail.
		k, ek = randKey(rng), randKey(rng)
		if ek < k {
			// NB: if they're equal, that's just tough luck; we'll have an empty range.
			k, ek = ek, k
		}
	}

	return delRangeUsingTombstone(k, ek, nextSeq())
}

func randSplitNew(g *generator, rng *rand.Rand) Operation {
	key := randKey(rng)
	g.currentSplits[key] = struct{}{}
	g.historicalSplits[key] = struct{}{}
	return split(key)
}

func randSplitAgain(g *generator, rng *rand.Rand) Operation {
	key := randMapKey(rng, g.historicalSplits)
	g.currentSplits[key] = struct{}{}
	return split(key)
}

func randMergeNotSplit(g *generator, rng *rand.Rand) Operation {
	key := randKey(rng)
	return merge(key)
}

func randMergeIsSplit(g *generator, rng *rand.Rand) Operation {
	key := randMapKey(rng, g.currentSplits)
	// Assume that this split actually got merged, even though we may have handed
	// out a concurrent split for the same key.
	delete(g.currentSplits, key)
	return merge(key)
}

func makeRemoveReplicaFn(key string, current []roachpb.ReplicationTarget, voter bool) opGenFunc {
	return func(g *generator, rng *rand.Rand) Operation {
		var changeType roachpb.ReplicaChangeType
		if voter {
			changeType = roachpb.REMOVE_VOTER
		} else {
			changeType = roachpb.REMOVE_NON_VOTER
		}
		change := kvpb.ReplicationChange{
			ChangeType: changeType,
			Target:     current[rng.Intn(len(current))],
		}
		return changeReplicas(key, change)
	}
}

func makeAddReplicaFn(
	key string, current []roachpb.ReplicationTarget, atomicSwap bool, voter bool,
) opGenFunc {
	return func(g *generator, rng *rand.Rand) Operation {
		candidatesMap := make(map[roachpb.ReplicationTarget]struct{})
		for i := 0; i < g.Config.NumNodes; i++ {
			t := roachpb.ReplicationTarget{NodeID: roachpb.NodeID(i + 1), StoreID: roachpb.StoreID(i + 1)}
			candidatesMap[t] = struct{}{}
		}
		for _, replica := range current {
			delete(candidatesMap, replica)
		}
		var candidates []roachpb.ReplicationTarget
		for candidate := range candidatesMap {
			candidates = append(candidates, candidate)
		}
		candidate := candidates[rng.Intn(len(candidates))]
		var addType, removeType roachpb.ReplicaChangeType
		if voter {
			addType, removeType = roachpb.ADD_VOTER, roachpb.REMOVE_VOTER
		} else {
			addType, removeType = roachpb.ADD_NON_VOTER, roachpb.REMOVE_NON_VOTER
		}
		changes := []kvpb.ReplicationChange{{
			ChangeType: addType,
			Target:     candidate,
		}}
		if atomicSwap {
			changes = append(changes, kvpb.ReplicationChange{
				ChangeType: removeType,
				Target:     current[rng.Intn(len(current))],
			})
		}
		return changeReplicas(key, changes...)
	}
}

func makePromoteReplicaFn(key string, nonVoters []roachpb.ReplicationTarget) opGenFunc {
	return func(g *generator, rng *rand.Rand) Operation {
		target := nonVoters[rng.Intn(len(nonVoters))]
		changes := []kvpb.ReplicationChange{
			{
				ChangeType: roachpb.REMOVE_NON_VOTER,
				Target:     target,
			},
			{
				ChangeType: roachpb.ADD_VOTER,
				Target:     target,
			},
		}
		return changeReplicas(key, changes...)
	}
}

func makeDemoteReplicaFn(key string, voters []roachpb.ReplicationTarget) opGenFunc {
	return func(g *generator, rng *rand.Rand) Operation {
		target := voters[rng.Intn(len(voters))]
		changes := []kvpb.ReplicationChange{
			{
				ChangeType: roachpb.REMOVE_VOTER,
				Target:     target,
			},
			{
				ChangeType: roachpb.ADD_NON_VOTER,
				Target:     target,
			},
		}
		return changeReplicas(key, changes...)
	}
}

func makeTransferLeaseFn(key string, current []roachpb.ReplicationTarget) opGenFunc {
	return func(g *generator, rng *rand.Rand) Operation {
		target := current[rng.Intn(len(current))]
		return transferLease(key, target.StoreID)
	}
}

func setLeaseType(_ *generator, rng *rand.Rand) Operation {
	leaseTypes := roachpb.TestingAllLeaseTypes()
	leaseType := leaseTypes[rng.Intn(len(leaseTypes))]
	op := changeSetting(ChangeSettingType_SetLeaseType)
	op.ChangeSetting.LeaseType = leaseType
	return op
}

func toggleGlobalReads(_ *generator, _ *rand.Rand) Operation {
	return changeZone(ChangeZoneType_ToggleGlobalReads)
}

func makeRandBatch(c *ClientOperationConfig) opGenFunc {
	return func(g *generator, rng *rand.Rand) Operation {
		var allowed []opGen
		g.registerClientOps(&allowed, c)
		numOps := rng.Intn(4)
		ops := make([]Operation, numOps)
		for i := range ops {
			ops[i] = g.selectOp(rng, allowed)
		}
		return batch(ops...)
	}
}

func (g *generator) registerClosureTxnOps(allowed *[]opGen, c *ClosureTxnConfig) {
	const Commit, Rollback = ClosureTxnType_Commit, ClosureTxnType_Rollback
	const SSI, SI, RC = isolation.Serializable, isolation.Snapshot, isolation.ReadCommitted
	addOpGen(allowed,
		makeClosureTxn(Commit, SSI, &c.TxnClientOps, &c.TxnBatchOps, nil /* commitInBatch*/, &c.SavepointOps), c.CommitSerializable)
	addOpGen(allowed,
		makeClosureTxn(Commit, SI, &c.TxnClientOps, &c.TxnBatchOps, nil /* commitInBatch*/, &c.SavepointOps), c.CommitSnapshot)
	addOpGen(allowed,
		makeClosureTxn(Commit, RC, &c.TxnClientOps, &c.TxnBatchOps, nil /* commitInBatch*/, &c.SavepointOps), c.CommitReadCommitted)
	addOpGen(allowed,
		makeClosureTxn(Rollback, SSI, &c.TxnClientOps, &c.TxnBatchOps, nil /* commitInBatch*/, &c.SavepointOps), c.RollbackSerializable)
	addOpGen(allowed,
		makeClosureTxn(Rollback, SI, &c.TxnClientOps, &c.TxnBatchOps, nil /* commitInBatch*/, &c.SavepointOps), c.RollbackSnapshot)
	addOpGen(allowed,
		makeClosureTxn(Rollback, RC, &c.TxnClientOps, &c.TxnBatchOps, nil /* commitInBatch*/, &c.SavepointOps), c.RollbackReadCommitted)
	addOpGen(allowed,
		makeClosureTxn(Commit, SSI, &c.TxnClientOps, &c.TxnBatchOps, &c.CommitBatchOps, &c.SavepointOps), c.CommitSerializableInBatch)
	addOpGen(allowed,
		makeClosureTxn(Commit, SI, &c.TxnClientOps, &c.TxnBatchOps, &c.CommitBatchOps, &c.SavepointOps), c.CommitSnapshotInBatch)
	addOpGen(allowed,
		makeClosureTxn(Commit, RC, &c.TxnClientOps, &c.TxnBatchOps, &c.CommitBatchOps, &c.SavepointOps), c.CommitReadCommittedInBatch)
}

func makeClosureTxn(
	txnType ClosureTxnType,
	iso isolation.Level,
	txnClientOps *ClientOperationConfig,
	txnBatchOps *BatchOperationConfig,
	commitInBatch *ClientOperationConfig,
	savepointOps *SavepointConfig,
) opGenFunc {
	return func(g *generator, rng *rand.Rand) Operation {
		// All allowed non-savepoint ops. These don't change as we iteratively
		// select ops in the loop below.
		var allowed []opGen
		g.registerClientOps(&allowed, txnClientOps)
		g.registerBatchOps(&allowed, txnBatchOps)
		const maxOps = 20
		numOps := rng.Intn(maxOps + 1)
		ops := make([]Operation, numOps)
		// Stack of savepoint indexes/ids.
		// The last element of the slice is the top of the stack.
		var spIDs []int
		for i := range ops {
			// In each iteration, we start with the allowed non-savepoint ops,
			// and we add the valid savepoint ops in registerSavepointOps.
			allowedIncludingSavepointOps := allowed
			// Savepoints are registered on each iteration of the loop (unlike other
			// ops that are registered once at the start) because depending on what
			// savepoint ops are randomly selected in selectOp below, the set of
			// allowed savepoint ops changes. See registerSavepointOps.
			g.registerSavepointOps(&allowedIncludingSavepointOps, savepointOps, spIDs, i)
			ops[i] = g.selectOp(rng, allowedIncludingSavepointOps)
			// Now that a random op is selected, we may need to update the stack of
			// existing savepoints. See maybeUpdateSavepoints.
			maybeUpdateSavepoints(&spIDs, ops[i])
		}
		op := closureTxn(txnType, iso, ops...)
		if commitInBatch != nil {
			if txnType != ClosureTxnType_Commit {
				panic(errors.AssertionFailedf(`CommitInBatch must commit got: %s`, txnType))
			}
			op.ClosureTxn.CommitInBatch = makeRandBatch(commitInBatch)(g, rng).Batch
		}
		return op
	}
}

// registerSavepointOps assumes existingSp is the current stack of savepoints
// and uses it to register only valid savepoint ops. I.e. releasing or rolling
// back a savepoint that hasn't been created or has already been released or
// rolled back is not allowed.
func (g *generator) registerSavepointOps(
	allowed *[]opGen, s *SavepointConfig, existingSp []int, idx int,
) {
	// A savepoint creation is always a valid. The index of the op in the txn is
	// used as its id.
	addOpGen(allowed, makeSavepointCreate(idx), s.SavepointCreate)
	// For each existing savepoint, a rollback and a release op are valid.
	for _, id := range existingSp {
		addOpGen(allowed, makeSavepointRelease(id), s.SavepointRelease)
		addOpGen(allowed, makeSavepointRollback(id), s.SavepointRollback)
	}
}

func makeSavepointCreate(id int) opGenFunc {
	return func(_ *generator, _ *rand.Rand) Operation {
		return createSavepoint(id)
	}
}

func makeSavepointRelease(id int) opGenFunc {
	return func(_ *generator, _ *rand.Rand) Operation {
		return releaseSavepoint(id)
	}
}

func makeSavepointRollback(id int) opGenFunc {
	return func(_ *generator, _ *rand.Rand) Operation {
		return rollbackSavepoint(id)
	}
}

// maybeUpdateSavepoints modifies the slice of existing savepoints based on the
// previously selected op to either: (1) add a new savepoint if the previous op
// was SavepointCreateOperation, or (2) remove a suffix of savepoints if the
// previous op was SavepointReleaseOperation or SavepointRollbackOperation.
func maybeUpdateSavepoints(existingSp *[]int, prevOp Operation) {
	switch op := prevOp.GetValue().(type) {
	case *SavepointCreateOperation:
		// If the previous selected op was a savepoint creation, add it to the stack
		// of existing savepoints.
		if slices.Index(*existingSp, int(op.ID)) != -1 {
			panic(errors.AssertionFailedf(`generating a savepoint create op: ID %d already exists`, int(op.ID)))
		}
		*existingSp = append(*existingSp, int(op.ID))
	case *SavepointReleaseOperation:
		// If the previous selected op was a savepoint release, remove it from the
		// stack of existing savepoints, together with all other savepoint above it
		// on the stack. E.g. if the existing savepoints are [1, 2, 3], releasing
		// savepoint 2 means savepoint 3 also needs to be released.
		index := slices.Index(*existingSp, int(op.ID))
		if index == -1 {
			panic(errors.AssertionFailedf(`generating a savepoint release op: ID %d does not exist`, int(op.ID)))
		}
		*existingSp = (*existingSp)[:index]
	case *SavepointRollbackOperation:
		// If the previous selected op was a savepoint rollback, remove it from the
		// stack of existing savepoints, together with all other savepoint above it
		// on the stack. E.g. if the existing savepoints are [1, 2, 3], rolling back
		// savepoint 2 means savepoint 3 also needs to be rolled back.
		index := slices.Index(*existingSp, int(op.ID))
		if index == -1 {
			panic(errors.AssertionFailedf(`generating a savepoint rollback op: ID %d does not exist`, int(op.ID)))
		}
		*existingSp = (*existingSp)[:index]
	default:
		// prevOp is not a savepoint operation, no need to adjust existingSp.
	}
}

// fk stands for "from key", i.e. decode the uint64 the key represents.
// Panics on error.
func fk(k string) uint64 {
	i, err := fkE(k)
	if err != nil {
		panic(err)
	}
	return i
}

// fkE is like fk, but returns an error instead of panicking.
func fkE(k string) (uint64, error) {
	span := GeneratorDataSpan()
	if !span.ContainsKey(roachpb.Key(k)) {
		return 0, errors.New("key too short")
	}
	k = k[len(span.Key):]
	_, s, err := encoding.DecodeUnsafeStringAscendingDeepCopy([]byte(k), nil)
	if err != nil {
		return 0, err
	}
	sl, err := hex.DecodeString(s)
	if err != nil {
		return 0, err
	}
	if len(sl) < 8 {
		return 0, errors.New("slice too short")
	}
	return binary.BigEndian.Uint64(sl), nil
}

// tk stands for toKey, i.e. encode the uint64 into its key representation.
func tk(n uint64) string {
	var sl [8]byte
	binary.BigEndian.PutUint64(sl[:8], n)
	s := hex.EncodeToString(sl[:8])
	key := GeneratorDataSpan().Key
	key = encoding.EncodeStringAscending(key, s)
	return string(key)
}

// keysBetween returns the keys between the given [start,end) span
// in an undefined order. It takes a map for use with g.keys.
func keysBetween(keys map[string]struct{}, start, end string) []string {
	between := []string{}
	s, e := fk(start), fk(end)
	for key := range keys {
		if nk := fk(key); nk >= s && nk < e {
			between = append(between, key)
		}
	}
	return between
}

func randKey(rng *rand.Rand) string {
	// Avoid the endpoints because having point writes at the
	// endpoints complicates randRangeSpan.
	n := rng.Uint64()
	if n == 0 {
		n++
	}
	if n == math.MaxUint64 {
		n--
	}
	return tk(n)
}

// Interprets the provided map as the split points of the key space and returns
// the boundaries of a random range.
func randRangeSpan(rng *rand.Rand, curOrHistSplits map[string]struct{}) (string, string) {
	keys := make([]string, 0, len(curOrHistSplits))
	for key := range curOrHistSplits {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		// No splits.
		return tk(0), tk(math.MaxUint64)
	}
	idx := rng.Intn(len(keys) + 1)
	if idx == len(keys) {
		// Last range.
		return keys[idx-1], tk(math.MaxUint64)
	}
	if idx == 0 {
		// First range. We avoid having splits at 0 so this will be a well-formed
		// range. (If it isn't, we'll likely catch an error because we'll send an
		// ill-formed request and kvserver will error it out).
		return tk(0), keys[0]
	}
	return keys[idx-1], keys[idx]
}

func randMapKey(rng *rand.Rand, m map[string]struct{}) string {
	if len(m) == 0 {
		return randKey(rng)
	}
	k, ek := randRangeSpan(rng, m)
	// If there is only one key in the map we will get [0,x) or [x,max)
	// back and want to return `x` to avoid the endpoints, which are
	// reserved.
	if fk(k) == 0 {
		return ek
	}
	return k
}

// Returns a key that falls into `[k,ek)`.
func randKeyBetween(rng *rand.Rand, k, ek string) string {
	a, b := fk(k), fk(ek)
	if b <= a {
		b = a + 1 // we will return `a`
	}
	defer func() {
		if r := recover(); r != nil {
			panic(fmt.Sprintf("a=%d b=%d b-a=%d: %v", a, b, int64(b-a), r))
		}
	}()
	return tk(a + (rng.Uint64() % (b - a)))
}

func randSpan(rng *rand.Rand) (string, string) {
	key, endKey := randKey(rng), randKey(rng)
	if endKey < key {
		key, endKey = endKey, key
	} else if endKey == key {
		endKey = string(roachpb.Key(key).Next())
	}
	return key, endKey
}

func step(op Operation) Step {
	return Step{Op: op}
}

func batch(ops ...Operation) Operation {
	return Operation{Batch: &BatchOperation{
		Ops: ops,
	}}
}

func opSlice(ops ...Operation) []Operation {
	return ops
}

func closureTxn(typ ClosureTxnType, iso isolation.Level, ops ...Operation) Operation {
	return Operation{ClosureTxn: &ClosureTxnOperation{Ops: ops, Type: typ, IsoLevel: iso}}
}

func closureTxnSSI(typ ClosureTxnType, ops ...Operation) Operation {
	return closureTxn(typ, isolation.Serializable, ops...)
}

func closureTxnCommitInBatch(
	iso isolation.Level, commitInBatch []Operation, ops ...Operation,
) Operation {
	o := closureTxn(ClosureTxnType_Commit, iso, ops...)
	if len(commitInBatch) > 0 {
		o.ClosureTxn.CommitInBatch = &BatchOperation{Ops: commitInBatch}
	}
	return o
}

func get(key string) Operation {
	return Operation{Get: &GetOperation{Key: []byte(key)}}
}

func getForUpdate(key string) Operation {
	return Operation{Get: &GetOperation{Key: []byte(key), ForUpdate: true}}
}

func getForUpdateGuaranteedDurability(key string) Operation {
	return Operation{Get: &GetOperation{Key: []byte(key), ForUpdate: true, GuaranteedDurability: true}}
}

func getForShare(key string) Operation {
	return Operation{Get: &GetOperation{Key: []byte(key), ForShare: true}}
}

func getForShareGuaranteedDurability(key string) Operation {
	return Operation{Get: &GetOperation{Key: []byte(key), ForShare: true, GuaranteedDurability: true}}
}

func getSkipLocked(key string) Operation {
	return Operation{Get: &GetOperation{Key: []byte(key), SkipLocked: true}}
}

func getForUpdateSkipLocked(key string) Operation {
	return Operation{Get: &GetOperation{Key: []byte(key), ForUpdate: true, SkipLocked: true}}
}

func getForUpdateSkipLockedGuaranteedDurability(key string) Operation {
	return Operation{Get: &GetOperation{Key: []byte(key), ForUpdate: true, SkipLocked: true, GuaranteedDurability: true}}
}

func getForShareSkipLocked(key string) Operation {
	return Operation{Get: &GetOperation{Key: []byte(key), ForShare: true, SkipLocked: true}}
}

func getForShareSkipLockedGuaranteedDurability(key string) Operation {
	return Operation{Get: &GetOperation{Key: []byte(key), ForShare: true, SkipLocked: true, GuaranteedDurability: true}}
}

func put(key string, seq kvnemesisutil.Seq) Operation {
	return Operation{Put: &PutOperation{Key: []byte(key), Seq: seq}}
}

func scan(key, endKey string) Operation {
	return Operation{Scan: &ScanOperation{Key: []byte(key), EndKey: []byte(endKey)}}
}

func scanForUpdate(key, endKey string) Operation {
	return Operation{Scan: &ScanOperation{Key: []byte(key), EndKey: []byte(endKey), ForUpdate: true}}
}

func scanForUpdateGuaranteedDurability(key, endKey string) Operation {
	return Operation{Scan: &ScanOperation{Key: []byte(key), EndKey: []byte(endKey), ForUpdate: true, GuaranteedDurability: true}}
}

func scanForShare(key, endKey string) Operation {
	return Operation{Scan: &ScanOperation{Key: []byte(key), EndKey: []byte(endKey), ForShare: true}}
}

func scanForShareGuaranteedDurability(key, endKey string) Operation {
	return Operation{Scan: &ScanOperation{Key: []byte(key), EndKey: []byte(endKey), ForShare: true, GuaranteedDurability: true}}
}

func scanSkipLocked(key, endKey string) Operation {
	return Operation{Scan: &ScanOperation{Key: []byte(key), EndKey: []byte(endKey), SkipLocked: true}}
}

func scanForUpdateSkipLocked(key, endKey string) Operation {
	return Operation{Scan: &ScanOperation{Key: []byte(key), EndKey: []byte(endKey), ForUpdate: true, SkipLocked: true}}
}

func scanForUpdateSkipLockedGuaranteedDurability(key, endKey string) Operation {
	return Operation{Scan: &ScanOperation{Key: []byte(key), EndKey: []byte(endKey), ForUpdate: true, SkipLocked: true, GuaranteedDurability: true}}
}

func scanForShareSkipLocked(key, endKey string) Operation {
	return Operation{Scan: &ScanOperation{Key: []byte(key), EndKey: []byte(endKey), ForShare: true, SkipLocked: true}}
}

func scanForShareSkipLockedGuaranteedDurability(key, endKey string) Operation {
	return Operation{Scan: &ScanOperation{Key: []byte(key), EndKey: []byte(endKey), ForShare: true, SkipLocked: true, GuaranteedDurability: true}}
}

func reverseScan(key, endKey string) Operation {
	return Operation{Scan: &ScanOperation{Key: []byte(key), EndKey: []byte(endKey), Reverse: true}}
}

func reverseScanForUpdate(key, endKey string) Operation {
	return Operation{Scan: &ScanOperation{Key: []byte(key), EndKey: []byte(endKey), Reverse: true, ForUpdate: true}}
}

func reverseScanForUpdateGuaranteedDurability(key, endKey string) Operation {
	return Operation{Scan: &ScanOperation{Key: []byte(key), EndKey: []byte(endKey), Reverse: true, ForUpdate: true, GuaranteedDurability: true}}
}

func reverseScanForShare(key, endKey string) Operation {
	return Operation{Scan: &ScanOperation{Key: []byte(key), EndKey: []byte(endKey), Reverse: true, ForShare: true}}
}

func reverseScanForShareGuaranteedDurability(key, endKey string) Operation {
	return Operation{Scan: &ScanOperation{Key: []byte(key), EndKey: []byte(endKey), Reverse: true, ForShare: true, GuaranteedDurability: true}}
}

func reverseScanSkipLocked(key, endKey string) Operation {
	return Operation{Scan: &ScanOperation{Key: []byte(key), EndKey: []byte(endKey), Reverse: true, SkipLocked: true}}
}

func reverseScanForUpdateSkipLocked(key, endKey string) Operation {
	return Operation{Scan: &ScanOperation{Key: []byte(key), EndKey: []byte(endKey), Reverse: true, ForUpdate: true, SkipLocked: true}}
}

func reverseScanForUpdateSkipLockedGuaranteedDurability(key, endKey string) Operation {
	return Operation{Scan: &ScanOperation{Key: []byte(key), EndKey: []byte(endKey), Reverse: true, ForUpdate: true, SkipLocked: true, GuaranteedDurability: true}}
}

func reverseScanForShareSkipLocked(key, endKey string) Operation {
	return Operation{Scan: &ScanOperation{Key: []byte(key), EndKey: []byte(endKey), Reverse: true, ForShare: true, SkipLocked: true}}
}

func reverseScanForShareSkipLockedGuaranteedDurability(key, endKey string) Operation {
	return Operation{Scan: &ScanOperation{Key: []byte(key), EndKey: []byte(endKey), Reverse: true, ForShare: true, SkipLocked: true, GuaranteedDurability: true}}
}

func del(key string, seq kvnemesisutil.Seq) Operation {
	return Operation{Delete: &DeleteOperation{
		Key: []byte(key),
		Seq: seq,
	}}
}

func delRange(key, endKey string, seq kvnemesisutil.Seq) Operation {
	return Operation{DeleteRange: &DeleteRangeOperation{Key: []byte(key), EndKey: []byte(endKey), Seq: seq}}
}

func delRangeUsingTombstone(key, endKey string, seq kvnemesisutil.Seq) Operation {
	return Operation{DeleteRangeUsingTombstone: &DeleteRangeUsingTombstoneOperation{Key: []byte(key), EndKey: []byte(endKey), Seq: seq}}
}

func split(key string) Operation {
	return Operation{Split: &SplitOperation{Key: []byte(key)}}
}

func merge(key string) Operation {
	return Operation{Merge: &MergeOperation{Key: []byte(key)}}
}

func changeReplicas(key string, changes ...kvpb.ReplicationChange) Operation {
	return Operation{ChangeReplicas: &ChangeReplicasOperation{Key: []byte(key), Changes: changes}}
}

func transferLease(key string, target roachpb.StoreID) Operation {
	return Operation{TransferLease: &TransferLeaseOperation{Key: []byte(key), Target: target}}
}

func changeSetting(changeType ChangeSettingType) Operation {
	return Operation{ChangeSetting: &ChangeSettingOperation{Type: changeType}}
}

func changeZone(changeType ChangeZoneType) Operation {
	return Operation{ChangeZone: &ChangeZoneOperation{Type: changeType}}
}

func addSSTable(
	data []byte, span roachpb.Span, sstTimestamp hlc.Timestamp, seq kvnemesisutil.Seq, asWrites bool,
) Operation {
	return Operation{AddSSTable: &AddSSTableOperation{
		Data:         data,
		Span:         span,
		SSTTimestamp: sstTimestamp,
		Seq:          seq,
		AsWrites:     asWrites,
	}}
}

func barrier(key, endKey string, withLAI bool) Operation {
	return Operation{Barrier: &BarrierOperation{
		Key:                   []byte(key),
		EndKey:                []byte(endKey),
		WithLeaseAppliedIndex: withLAI,
	}}
}

func createSavepoint(id int) Operation {
	return Operation{SavepointCreate: &SavepointCreateOperation{ID: int32(id)}}
}

func releaseSavepoint(id int) Operation {
	return Operation{SavepointRelease: &SavepointReleaseOperation{ID: int32(id)}}
}

func rollbackSavepoint(id int) Operation {
	return Operation{SavepointRollback: &SavepointRollbackOperation{ID: int32(id)}}
}
