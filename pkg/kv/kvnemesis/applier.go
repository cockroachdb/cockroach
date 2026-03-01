// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvnemesis

import (
	"context"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvnemesis/kvnemesisutil"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

// Applier executes Steps.
type Applier struct {
	env   *Env
	dbs   []*kv.DB
	nodes *nodes
	mu    struct {
		dbIdx int
		syncutil.Mutex
		txns map[string]*kv.Txn
	}
}

// MakeApplier constructs an Applier that executes against the given DBs.
func MakeApplier(env *Env, n *nodes, dbs ...*kv.DB) *Applier {
	a := &Applier{
		env:   env,
		dbs:   dbs,
		nodes: n,
	}
	a.mu.txns = make(map[string]*kv.Txn)
	return a
}

// Apply executes the given Step and mutates it with the result of execution. An
// error is only returned from Apply if there is an internal coding error within
// Applier, errors from a Step execution are saved in the Step itself.
func (a *Applier) Apply(ctx context.Context, step *Step) (trace tracingpb.Recording, retErr error) {
	var db *kv.DB
	db, step.DBID = a.getNextDBRoundRobin()

	step.Before = db.Clock().Now()
	recCtx, collectAndFinish := tracing.ContextWithRecordingSpan(ctx, db.Tracer, "txn step")
	defer func() {
		step.After = db.Clock().Now()
		if p := recover(); p != nil {
			retErr = errors.Errorf(`panic applying step %s: %v`, step, p)
		}
		trace = collectAndFinish()
	}()
	a.applyOp(recCtx, db, &step.Op)
	return collectAndFinish(), nil
}

func (a *Applier) getNextDBRoundRobin() (*kv.DB, int32) {
	a.mu.Lock()
	dbIdx := a.mu.dbIdx
	a.mu.dbIdx = (a.mu.dbIdx + 1) % len(a.dbs)
	a.mu.Unlock()
	return a.dbs[dbIdx], int32(dbIdx)
}

// Sentinel errors.
var (
	errOmitted                                      = errors.New("omitted")
	errClosureTxnRollback                           = errors.New("rollback")
	errDelRangeUsingTombstoneStraddlesRangeBoundary = errors.New("DeleteRangeUsingTombstone can not straddle range boundary")
)

func exceptOmitted(err error) bool { // true if errOmitted
	return errors.Is(err, errOmitted)
}

func exceptRollback(err error) bool { // true if intentional txn rollback
	return errors.Is(err, errClosureTxnRollback)
}

func exceptRetry(err error) bool { // true if retry error
	return errors.HasInterface(err, (*kvpb.ClientVisibleRetryError)(nil))
}

func exceptUnhandledRetry(err error) bool {
	return errors.HasType(err, (*kvpb.UnhandledRetryableError)(nil))
}

func exceptAmbiguous(err error) bool { // true if ambiguous result
	return errors.HasInterface(err, (*kvpb.ClientVisibleAmbiguousError)(nil)) ||
		strings.Contains(err.Error(), "result is ambiguous")
}

func exceptDelRangeUsingTombstoneStraddlesRangeBoundary(err error) bool {
	return errors.Is(err, errDelRangeUsingTombstoneStraddlesRangeBoundary)
}

func exceptConditionFailed(err error) bool {
	return errors.HasType(err, (*kvpb.ConditionFailedError)(nil))
}

func exceptReplicaUnavailable(err error) bool {
	return errors.HasType(err, (*kvpb.ReplicaUnavailableError)(nil)) ||
		strings.Contains(err.Error(), "replica unavailable")
}

func exceptContextCanceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		strings.Contains(err.Error(), "query execution canceled")
}

const followerReadsOffset = 2 * time.Second

func makeFollowerReadTimestamp(ts hlc.Timestamp) hlc.Timestamp {
	return ts.Add(-followerReadsOffset.Nanoseconds(), 0)
}
func configureBatchForFollowerReads(b *kv.Batch, ts hlc.Timestamp) {
	b.Header.Timestamp = makeFollowerReadTimestamp(ts)
	b.Header.RoutingPolicy = kvpb.RoutingPolicy_NEAREST
}

func (a *Applier) applyOp(ctx context.Context, db *kv.DB, op *Operation) {
	switch o := op.GetValue().(type) {
	case *GetOperation,
		*PutOperation,
		*CPutOperation,
		*ScanOperation,
		*BatchOperation,
		*DeleteOperation,
		*DeleteRangeOperation,
		*DeleteRangeUsingTombstoneOperation,
		*AddSSTableOperation:
		applyClientOp(ctx, db, op, false /* inTxn */, nil /* spIDToToken */)
	case *SplitOperation:
		err := db.AdminSplit(ctx, o.Key, hlc.MaxTimestamp)
		o.Result = resultInit(ctx, err)
	case *MergeOperation:
		err := db.AdminMerge(ctx, o.Key)
		o.Result = resultInit(ctx, err)
	case *ChangeReplicasOperation:
		desc := getRangeDesc(ctx, o.Key, db)
		_, err := db.AdminChangeReplicas(ctx, o.Key, desc, o.Changes)
		o.Result = resultInit(ctx, err)
	case *TransferLeaseOperation:
		err := db.AdminTransferLease(ctx, o.Key, o.Target)
		o.Result = resultInit(ctx, err)
	case *ChangeSettingOperation:
		err := changeClusterSettingInEnv(ctx, a.env, o)
		o.Result = resultInit(ctx, err)
	case *ChangeZoneOperation:
		err := updateZoneConfigInEnv(ctx, a.env, o.Type)
		o.Result = resultInit(ctx, err)
	case *BarrierOperation:
		var err error
		if o.WithLeaseAppliedIndex {
			_, _, err = db.BarrierWithLAI(ctx, o.Key, o.EndKey)
		} else {
			_, err = db.Barrier(ctx, o.Key, o.EndKey)
		}
		o.Result = resultInit(ctx, err)
	case *FlushLockTableOperation:
		o.Result = resultInit(ctx, db.FlushLockTable(ctx, o.Key, o.EndKey))
	case *AddNetworkPartitionOperation:
		err := a.env.Partitioner.AddPartition(roachpb.NodeID(o.FromNode), roachpb.NodeID(o.ToNode))
		o.Result = resultInit(ctx, err)
	case *RemoveNetworkPartitionOperation:
		err := a.env.Partitioner.RemovePartition(roachpb.NodeID(o.FromNode), roachpb.NodeID(o.ToNode))
		o.Result = resultInit(ctx, err)
	case *StopNodeOperation:
		serverID := int(o.NodeId) - 1
		a.env.ServerController.StopServer(serverID)
		a.nodes.setStopped(int(o.NodeId))
		o.Result = resultInit(ctx, nil)
	case *RestartNodeOperation:
		serverID := int(o.NodeId) - 1
		err := a.env.ServerController.RestartServer(serverID)
		a.nodes.setRunning(int(o.NodeId))
		o.Result = resultInit(ctx, err)
	case *CrashNodeOperation:
		serverID := int(o.NodeId) - 1
		a.env.ServerController.CrashNode(serverID)
		a.nodes.setCrashed(int(o.NodeId))
		o.Result = resultInit(ctx, nil)
	case *ClosureTxnOperation:
		// Use a backoff loop to avoid thrashing on txn aborts. Don't wait between
		// epochs of the same transaction to avoid waiting while holding locks.
		retryOnAbort := retry.StartWithCtx(ctx, retry.Options{
			InitialBackoff: 1 * time.Millisecond,
			MaxBackoff:     10 * time.Second,
		})
		var savedTxn *kv.Txn
		txnErr := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			// Attempt to set a follower reads timestamp only on the first iteration.
			// Setting it again and again on retries can lead to starvation.
			if o.FollowerReadEligible && savedTxn == nil {
				followerReadTs := makeFollowerReadTimestamp(db.Clock().Now())
				err := txn.SetFixedTimestamp(ctx, followerReadTs)
				if err != nil {
					panic(err)
				}
			}
			if err := txn.SetIsoLevel(o.IsoLevel); err != nil {
				panic(err)
			}
			if o.UserPriority > 0 {
				if err := txn.SetUserPriority(o.UserPriority); err != nil {
					panic(err)
				}
			}
			txn.SetBufferedWritesEnabled(o.BufferedWrites)
			if savedTxn != nil && txn.TestingCloneTxn().Epoch == 0 {
				// If the txn's current epoch is 0 and we've run at least one prior
				// iteration, we were just aborted.
				retryOnAbort.Next()
			}
			savedTxn = txn
			// A map of a savepoint id to the corresponding savepoint token that was
			// created after applying the savepoint op.
			spIDToToken := make(map[int]kv.SavepointToken)
			// First error. Because we need to mark everything that
			// we didn't "reach" due to a prior error with errOmitted,
			// we *don't* return eagerly on this but save it to the end.
			var err error
			{
				for i := range o.Ops {
					op := &o.Ops[i]
					op.Result().Reset() // in case we're a retry
					if err != nil {
						// If a previous op failed, mark this op as never invoked. We need
						// to do this because we want, as an invariant, to have marked all
						// operations as either failed or succeeded.
						*op.Result() = resultInit(ctx, errOmitted)
						if op.Batch != nil {
							for _, op := range op.Batch.Ops {
								*op.Result() = resultInit(ctx, errOmitted)
							}
						}

						continue
					}

					applyClientOp(ctx, txn, op, true /* inTxn */, &spIDToToken)
					// The KV api disallows use of a txn after an operation on it errors.
					if r := op.Result(); r.Type == ResultType_Error {
						err = errors.DecodeError(ctx, *r.Err)
					}
				}
			}
			if err != nil {
				if o.CommitInBatch != nil {
					// We failed before committing, so set errOmitted everywhere
					// and then return the original error.
					o.CommitInBatch.Result = resultInit(ctx, errOmitted)
					for _, op := range o.CommitInBatch.Ops {
						// NB: the `op` is definitely not a batch since we can't nest
						// batches within each other, so we don't need that second level of
						// recursion here.
						*op.Result() = resultInit(ctx, errOmitted)
					}
				}
				return err
			}
			if o.CommitInBatch != nil {
				b := txn.NewBatch()
				applyBatchOp(ctx, b, txn.CommitInBatch, o.CommitInBatch, nil /* clock */)
				// The KV api disallows use of a txn after an operation on it errors.
				if r := o.CommitInBatch.Result; r.Type == ResultType_Error {
					return errors.DecodeError(ctx, *r.Err)
				}
			}
			switch o.Type {
			case ClosureTxnType_Commit:
				return nil
			case ClosureTxnType_Rollback:
				return errClosureTxnRollback
			default:
				panic(errors.AssertionFailedf(`unknown closure txn type: %s`, o.Type))
			}
		})
		o.Result = resultInit(ctx, txnErr)
		if txnErr == nil {
			o.Txn = savedTxn.TestingCloneTxn()
			o.Result.OptionalTimestamp = o.Txn.WriteTimestamp
		}
	case *SavepointCreateOperation, *SavepointReleaseOperation, *SavepointRollbackOperation:
		panic(errors.AssertionFailedf(`can't apply a savepoint operation %v outside of a ClosureTxnOperation`, o))
	default:
		panic(errors.AssertionFailedf(`unknown operation type: %T %v`, o, o))
	}
}

type dbRunI interface {
	Run(context.Context, *kv.Batch) error
}

type clientI interface {
	dbRunI
	Get(context.Context, interface{}) (kv.KeyValue, error)
	GetForUpdate(context.Context, interface{}, kvpb.KeyLockingDurabilityType) (kv.KeyValue, error)
	GetForShare(context.Context, interface{}, kvpb.KeyLockingDurabilityType) (kv.KeyValue, error)
	Put(context.Context, interface{}, interface{}) error
	Scan(context.Context, interface{}, interface{}, int64) ([]kv.KeyValue, error)
	ScanForUpdate(context.Context, interface{}, interface{}, int64, kvpb.KeyLockingDurabilityType) ([]kv.KeyValue, error)
	ScanForShare(context.Context, interface{}, interface{}, int64, kvpb.KeyLockingDurabilityType) ([]kv.KeyValue, error)
	ReverseScan(context.Context, interface{}, interface{}, int64) ([]kv.KeyValue, error)
	ReverseScanForUpdate(context.Context, interface{}, interface{}, int64, kvpb.KeyLockingDurabilityType) ([]kv.KeyValue, error)
	ReverseScanForShare(context.Context, interface{}, interface{}, int64, kvpb.KeyLockingDurabilityType) ([]kv.KeyValue, error)
	Del(context.Context, ...interface{}) ([]roachpb.Key, error)
	DelRange(context.Context, interface{}, interface{}, bool) ([]roachpb.Key, error)
}

func dbRunWithResultAndTimestamp(
	ctx context.Context, db dbRunI, ops ...func(b *kv.Batch),
) ([]kv.Result, hlc.Timestamp, error) {
	b := &kv.Batch{}
	for _, op := range ops {
		op(b)
	}
	ts, err := batchRun(ctx, db.Run, b)
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}
	return b.Results, ts, nil
}

func batchRun(
	ctx context.Context, run func(context.Context, *kv.Batch) error, b *kv.Batch,
) (hlc.Timestamp, error) {
	if err := run(ctx, b); err != nil {
		return hlc.Timestamp{}, err
	}
	var ts hlc.Timestamp
	if rr := b.RawResponse(); rr != nil {
		ts = rr.Timestamp
	}
	return ts, nil
}

func applyClientOp(
	ctx context.Context,
	db clientI,
	op *Operation,
	inTxn bool,
	spIDToToken *map[int]kv.SavepointToken,
) {
	switch o := op.GetValue().(type) {
	case *GetOperation:
		res, ts, err := dbRunWithResultAndTimestamp(ctx, db, func(b *kv.Batch) {
			if o.SkipLocked {
				b.Header.WaitPolicy = lock.WaitPolicy_SkipLocked
			}
			dur := kvpb.BestEffort
			if o.GuaranteedDurability {
				dur = kvpb.GuaranteedDurability
			}
			if o.ForUpdate {
				b.GetForUpdate(o.Key, dur)
			} else if o.ForShare {
				b.GetForShare(o.Key, dur)
			} else {
				b.Get(o.Key)
			}
			if !inTxn && o.FollowerReadEligible {
				kvDB, ok := db.(*kv.DB)
				if !ok {
					panic(errors.AssertionFailedf("unexpected transactional interface"))
				}
				configureBatchForFollowerReads(b, kvDB.Clock().Now())
			}
		})
		o.Result = resultInit(ctx, err)
		if err != nil {
			return
		}
		o.Result.Type = ResultType_Value
		o.Result.OptionalTimestamp = ts
		kv := res[0].Rows[0]
		if kv.Value != nil {
			o.Result.Value = kv.Value.RawBytes
		} else {
			o.Result.Value = nil
		}
	case *PutOperation:
		_, ts, err := dbRunWithResultAndTimestamp(ctx, db, func(b *kv.Batch) {
			if o.MustAcquireExclusiveLock {
				b.PutMustAcquireExclusiveLock(o.Key, o.Value())
			} else {
				b.Put(o.Key, o.Value())
			}
			setLastReqSeq(b, o.Seq)
		})
		o.Result = resultInit(ctx, err)
		if err != nil {
			return
		}
		o.Result.OptionalTimestamp = ts
	case *CPutOperation:
		_, ts, err := dbRunWithResultAndTimestamp(ctx, db, func(b *kv.Batch) {
			var expBytes []byte
			if o.ExpVal != nil {
				expBytes = roachpb.MakeValueFromBytes(o.ExpVal).TagAndDataBytes()
			}
			if o.AllowIfDoesNotExist {
				b.CPutAllowingIfNotExists(o.Key, o.Value(), expBytes)
			} else {
				b.CPut(o.Key, o.Value(), expBytes)
			}
			setLastReqSeq(b, o.Seq)
		})
		o.Result = resultInit(ctx, err)
		// If the CPut failed with ConditionFailedError, we still want to record the
		// timestamp and do some validation later.
		if err != nil && !exceptConditionFailed(err) {
			return
		}
		o.Result.OptionalTimestamp = ts
	case *ScanOperation:
		res, ts, err := dbRunWithResultAndTimestamp(ctx, db, func(b *kv.Batch) {
			if o.SkipLocked {
				b.Header.WaitPolicy = lock.WaitPolicy_SkipLocked
			}
			dur := kvpb.BestEffort
			if o.GuaranteedDurability {
				dur = kvpb.GuaranteedDurability
			}
			if o.Reverse {
				if o.ForUpdate {
					b.ReverseScanForUpdate(o.Key, o.EndKey, dur)
				} else if o.ForShare {
					b.ReverseScanForShare(o.Key, o.EndKey, dur)
				} else {
					b.ReverseScan(o.Key, o.EndKey)
				}
			} else {
				if o.ForUpdate {
					b.ScanForUpdate(o.Key, o.EndKey, dur)
				} else if o.ForShare {
					b.ScanForShare(o.Key, o.EndKey, dur)
				} else {
					b.Scan(o.Key, o.EndKey)
				}
			}
			if !inTxn && o.FollowerReadEligible {
				kvDB, ok := db.(*kv.DB)
				if !ok {
					panic(errors.AssertionFailedf("unexpected transactional interface"))
				}
				configureBatchForFollowerReads(b, kvDB.Clock().Now())
			}
		})
		o.Result = resultInit(ctx, err)
		if err != nil {
			return
		}
		kvs := res[0].Rows
		o.Result.OptionalTimestamp = ts
		o.Result.Type = ResultType_Values
		o.Result.Values = make([]KeyValue, len(kvs))
		for i, kv := range kvs {
			o.Result.Values[i] = KeyValue{
				Key:   kv.Key,
				Value: kv.Value.RawBytes,
			}
		}
	case *DeleteOperation:
		res, ts, err := dbRunWithResultAndTimestamp(ctx, db, func(b *kv.Batch) {
			if o.MustAcquireExclusiveLock {
				b.DelMustAcquireExclusiveLock(o.Key)
			} else {
				b.Del(o.Key)
			}
			setLastReqSeq(b, o.Seq)
		})
		o.Result = resultInit(ctx, err)
		if err != nil {
			return
		}
		deletedKeys := res[0].Keys
		o.Result.OptionalTimestamp = ts
		o.Result.Type = ResultType_Keys
		o.Result.Keys = make([][]byte, len(deletedKeys))
		for i, deletedKey := range deletedKeys {
			o.Result.Keys[i] = deletedKey
		}
	case *DeleteRangeOperation:
		res, ts, err := dbRunWithResultAndTimestamp(ctx, db, func(b *kv.Batch) {
			b.DelRange(o.Key, o.EndKey, true /* returnKeys */)
			setLastReqSeq(b, o.Seq)
		})
		o.Result = resultInit(ctx, err)
		if err != nil {
			return
		}
		deletedKeys := res[0].Keys
		o.Result.OptionalTimestamp = ts
		o.Result.Type = ResultType_Keys
		o.Result.Keys = make([][]byte, len(deletedKeys))
		for i, deletedKey := range deletedKeys {
			o.Result.Keys[i] = deletedKey
		}
	case *DeleteRangeUsingTombstoneOperation:
		_, ts, err := dbRunWithResultAndTimestamp(ctx, db, func(b *kv.Batch) {
			b.DelRangeUsingTombstone(o.Key, o.EndKey)
			setLastReqSeq(b, o.Seq)
		})
		o.Result = resultInit(ctx, err)
		if err != nil {
			return
		}
		o.Result.OptionalTimestamp = ts
	case *AddSSTableOperation:
		if inTxn {
			panic(errors.AssertionFailedf(`AddSSTable cannot be used in transactions`))
		}
		_, ts, err := dbRunWithResultAndTimestamp(ctx, db, func(b *kv.Batch) {
			// Unlike other write operations, AddSSTable sends raw MVCC values
			// directly through to storage, including an MVCCValueHeader already
			// tagged with the sequence number. We therefore don't need to pass the
			// sequence number via the RequestHeader here.
			b.AddRawRequest(&kvpb.AddSSTableRequest{
				RequestHeader: kvpb.RequestHeader{
					Key:    o.Span.Key,
					EndKey: o.Span.EndKey,
				},
				Data:                           o.Data,
				SSTTimestampToRequestTimestamp: o.SSTTimestamp,
				DisallowConflicts:              true,
				IngestAsWrites:                 o.AsWrites,
			})
		})
		o.Result = resultInit(ctx, err)
		if err != nil {
			return
		}
		o.Result.OptionalTimestamp = ts
	case *BarrierOperation:
		_, _, err := dbRunWithResultAndTimestamp(ctx, db, func(b *kv.Batch) {
			b.AddRawRequest(&kvpb.BarrierRequest{
				RequestHeader: kvpb.RequestHeader{
					Key:    o.Key,
					EndKey: o.EndKey,
				},
				WithLeaseAppliedIndex: o.WithLeaseAppliedIndex,
			})
		})
		o.Result = resultInit(ctx, err)
	case *FlushLockTableOperation:
		_, _, err := dbRunWithResultAndTimestamp(ctx, db, func(b *kv.Batch) {
			b.AddRawRequest(&kvpb.FlushLockTableRequest{
				RequestHeader: kvpb.RequestHeader{
					Key:    o.Key,
					EndKey: o.EndKey,
				},
			})
		})
		o.Result = resultInit(ctx, err)
	case *BatchOperation:
		b := &kv.Batch{}
		if inTxn {
			applyBatchOp(ctx, b, db.Run, o, nil /* clock */)
		} else {
			kvDB, ok := db.(*kv.DB)
			if !ok {
				panic(errors.AssertionFailedf("unexpected transactional interface"))
			}
			applyBatchOp(ctx, b, db.Run, o, kvDB.Clock())
		}
	case *SavepointCreateOperation:
		txn, ok := db.(*kv.Txn) // savepoints are only allowed with transactions
		if !ok {
			panic(errors.AssertionFailedf(`non-txn interface attempted to create a savepoint %v`, o))
		}
		spt, err := txn.CreateSavepoint(ctx)
		o.Result = resultInit(ctx, err)
		if err != nil {
			return
		}
		// Map the savepoint id to the newly created savepoint token.
		if _, ok := (*spIDToToken)[int(o.ID)]; ok {
			panic(errors.AssertionFailedf("applying a savepoint create op: ID %d already exists", o.ID))
		}
		(*spIDToToken)[int(o.ID)] = spt
	case *SavepointReleaseOperation:
		txn, ok := db.(*kv.Txn) // savepoints are only allowed with transactions
		if !ok {
			panic(errors.AssertionFailedf(`non-txn interface attempted to release a savepoint %v`, o))
		}
		spt, ok := (*spIDToToken)[int(o.ID)]
		if !ok {
			panic(errors.AssertionFailedf("applying a savepoint release op: ID %d does not exist", o.ID))
		}
		err := txn.ReleaseSavepoint(ctx, spt)
		o.Result = resultInit(ctx, err)
		if err != nil {
			return
		}
	case *SavepointRollbackOperation:
		txn, ok := db.(*kv.Txn) // savepoints are only allowed with transactions
		if !ok {
			panic(errors.AssertionFailedf(`non-txn interface attempted to rollback a savepoint %v`, o))
		}
		spt, ok := (*spIDToToken)[int(o.ID)]
		if !ok {
			panic(errors.AssertionFailedf("applying a savepoint rollback op: ID %d does not exist", o.ID))
		}
		err := txn.RollbackToSavepoint(ctx, spt)
		o.Result = resultInit(ctx, err)
		if err != nil {
			return
		}
	default:
		panic(errors.AssertionFailedf(`unknown batch operation type: %T %v`, o, o))
	}
}

func setLastReqSeq(b *kv.Batch, seq kvnemesisutil.Seq) {
	sl := b.Requests()
	req := sl[len(sl)-1].GetInner()
	h := req.Header()
	h.KVNemesisSeq.Set(seq)
	req.SetHeader(h)
}

func applyBatchOp(
	ctx context.Context,
	b *kv.Batch,
	run func(context.Context, *kv.Batch) error,
	o *BatchOperation,
	clock *hlc.Clock,
) {
	for i := range o.Ops {
		switch subO := o.Ops[i].GetValue().(type) {
		case *GetOperation:
			if subO.SkipLocked {
				panic(errors.AssertionFailedf(`SkipLocked cannot be used in batches`))
			}
			dur := kvpb.BestEffort
			if subO.GuaranteedDurability {
				dur = kvpb.GuaranteedDurability
			}
			if subO.ForUpdate {
				b.GetForUpdate(subO.Key, dur)
			} else if subO.ForShare {
				b.GetForShare(subO.Key, dur)
			} else {
				b.Get(subO.Key)
			}
		case *PutOperation:
			if subO.MustAcquireExclusiveLock {
				b.PutMustAcquireExclusiveLock(subO.Key, subO.Value())
			} else {
				b.Put(subO.Key, subO.Value())
			}
			setLastReqSeq(b, subO.Seq)
		case *CPutOperation:
			expVal := roachpb.MakeValueFromBytes(subO.ExpVal)
			if subO.AllowIfDoesNotExist {
				b.CPutAllowingIfNotExists(subO.Key, subO.Value(), expVal.TagAndDataBytes())
			} else {
				b.CPut(subO.Key, subO.Value(), expVal.TagAndDataBytes())
			}
			setLastReqSeq(b, subO.Seq)
		case *ScanOperation:
			if subO.SkipLocked {
				panic(errors.AssertionFailedf(`SkipLocked cannot be used in batches`))
			}
			dur := kvpb.BestEffort
			if subO.GuaranteedDurability {
				dur = kvpb.GuaranteedDurability
			}
			if subO.Reverse {
				if subO.ForUpdate {
					b.ReverseScanForUpdate(subO.Key, subO.EndKey, dur)
				} else if subO.ForShare {
					b.ReverseScanForShare(subO.Key, subO.EndKey, dur)
				} else {
					b.ReverseScan(subO.Key, subO.EndKey)
				}
			} else {
				if subO.ForUpdate {
					b.ScanForUpdate(subO.Key, subO.EndKey, dur)
				} else if subO.ForShare {
					b.ScanForShare(subO.Key, subO.EndKey, dur)
				} else {
					b.Scan(subO.Key, subO.EndKey)
				}
			}
		case *DeleteOperation:
			if subO.MustAcquireExclusiveLock {
				b.DelMustAcquireExclusiveLock(subO.Key)
			} else {
				b.Del(subO.Key)
			}
			setLastReqSeq(b, subO.Seq)
		case *DeleteRangeOperation:
			b.DelRange(subO.Key, subO.EndKey, true /* returnKeys */)
			setLastReqSeq(b, subO.Seq)
		case *DeleteRangeUsingTombstoneOperation:
			b.DelRangeUsingTombstone(subO.Key, subO.EndKey)
			setLastReqSeq(b, subO.Seq)
		case *AddSSTableOperation:
			panic(errors.AssertionFailedf(`AddSSTable cannot be used in batches`))
		case *BarrierOperation:
			panic(errors.AssertionFailedf(`Barrier cannot be used in batches`))
		case *FlushLockTableOperation:
			panic(errors.AssertionFailedf(`FlushLockOperation cannot be used in batches`))
		case *MutateBatchHeaderOperation:
			b.Header.MaxSpanRequestKeys = subO.MaxSpanRequestKeys
			b.Header.TargetBytes = subO.TargetBytes
		default:
			panic(errors.AssertionFailedf(`unknown batch operation type: %T %v`, subO, subO))
		}
	}
	if clock != nil && o.FollowerReadEligible {
		configureBatchForFollowerReads(b, clock.Now())
	}
	ts, err := batchRun(ctx, run, b)
	o.Result = resultInit(ctx, err)
	// NB: we intentionally fall through; the batch propagates the error
	// to each result.
	err = nil
	o.Result.OptionalTimestamp = ts
	resultIdx := 0
	for i := range o.Ops {
		switch subO := o.Ops[i].GetValue().(type) {
		case *GetOperation:
			res := b.Results[resultIdx]
			if res.Err != nil {
				subO.Result = resultInit(ctx, res.Err)
			} else {
				if res.ResumeSpan != nil {
					subO.Result.Type = ResultType_NoError
				} else {
					subO.Result.Type = ResultType_Value
					result := res.Rows[0]
					if result.Value != nil {
						subO.Result.Value = result.Value.RawBytes
					} else {
						subO.Result.Value = nil
					}
				}
			}
			subO.Result.ResumeSpan = res.ResumeSpan
		case *PutOperation:
			res := b.Results[resultIdx]
			subO.Result = resultInit(ctx, res.Err)
			subO.Result.ResumeSpan = res.ResumeSpan
		case *CPutOperation:
			res := b.Results[resultIdx]
			subO.Result = resultInit(ctx, res.Err)
			subO.Result.ResumeSpan = res.ResumeSpan
		case *ScanOperation:
			res := b.Results[resultIdx]
			if res.Err != nil {
				subO.Result = resultInit(ctx, res.Err)
			} else {
				subO.Result.Type = ResultType_Values
				subO.Result.Values = make([]KeyValue, len(res.Rows))
				for j, kv := range res.Rows {
					subO.Result.Values[j] = KeyValue{
						Key:   []byte(kv.Key),
						Value: kv.Value.RawBytes,
					}
				}
			}
			subO.Result.ResumeSpan = res.ResumeSpan
		case *DeleteOperation:
			res := b.Results[resultIdx]
			subO.Result = resultInit(ctx, res.Err)
			subO.Result.ResumeSpan = res.ResumeSpan
		case *DeleteRangeOperation:
			res := b.Results[resultIdx]
			if res.Err != nil {
				subO.Result = resultInit(ctx, res.Err)
			} else {
				subO.Result.Type = ResultType_Keys
				subO.Result.Keys = make([][]byte, len(res.Keys))
				for j, key := range res.Keys {
					subO.Result.Keys[j] = key
				}
			}
			subO.Result.ResumeSpan = res.ResumeSpan
		case *DeleteRangeUsingTombstoneOperation:
			res := b.Results[resultIdx]
			subO.Result = resultInit(ctx, res.Err)
			subO.Result.ResumeSpan = res.ResumeSpan
		case *MutateBatchHeaderOperation:
			// NB: MutateBatchHeaderOperation cannot fail.
			subO.Result = resultInit(ctx, nil)
		case *AddSSTableOperation:
			panic(errors.AssertionFailedf(`AddSSTable cannot be used in batches`))
		default:
			panic(errors.AssertionFailedf(`unknown batch operation type: %T %v`, subO, subO))
		}
		if o.Ops[i].OperationHasResultInBatch() {
			resultIdx++
		}
	}
}

func resultInit(ctx context.Context, err error) Result {
	if err == nil {
		return Result{Type: ResultType_NoError}
	}
	ee := errors.EncodeError(ctx, err)
	return Result{
		Type: ResultType_Error,
		Err:  &ee,
	}
}

func getRangeDesc(ctx context.Context, key roachpb.Key, dbs ...*kv.DB) roachpb.RangeDescriptor {
	var dbIdx int
	var opts = retry.Options{}
	for r := retry.StartWithCtx(ctx, opts); r.Next(); dbIdx = (dbIdx + 1) % len(dbs) {
		sender := dbs[dbIdx].NonTransactionalSender()
		// Use kvpb.INCONSISTENT because kv.CONSISTENT requires a transactional
		// sender. In the generator, range lookups are usually used for finding
		// replica/lease change targets, so it's ok if these are not consistent.
		// Using kv.CONSISTENT with a non-transactional sender and in the presence
		// of network partitions can lead to infinitely stuck lookups.
		descs, _, err := kv.RangeLookup(ctx, sender, key, kvpb.INCONSISTENT, 0, false)
		if err != nil {
			log.Dev.Infof(ctx, "looking up descriptor for %s: %+v", key, err)
			continue
		}
		if len(descs) != 1 {
			log.Dev.Infof(ctx, "unexpected number of descriptors for %s: %d", key, len(descs))
			continue
		}
		return descs[0]
	}
	return roachpb.RangeDescriptor{}
}

func newGetReplicasFn(dbs ...*kv.DB) GetReplicasFn {
	return func(ctx context.Context, key roachpb.Key) ([]roachpb.ReplicationTarget, []roachpb.ReplicationTarget) {
		desc := getRangeDesc(ctx, key, dbs...)
		replicas := desc.Replicas().Descriptors()
		var voters []roachpb.ReplicationTarget
		var nonVoters []roachpb.ReplicationTarget
		for _, replica := range replicas {
			target := roachpb.ReplicationTarget{
				NodeID:  replica.NodeID,
				StoreID: replica.StoreID,
			}
			if replica.Type == roachpb.NON_VOTER {
				nonVoters = append(nonVoters, target)
			} else {
				voters = append(voters, target)
			}
		}
		return voters, nonVoters
	}
}

func changeClusterSettingInEnv(ctx context.Context, env *Env, op *ChangeSettingOperation) error {
	var settings map[string]string
	switch op.Type {
	case ChangeSettingType_SetLeaseType:
		switch op.LeaseType {
		case roachpb.LeaseExpiration:
			settings = map[string]string{
				"kv.lease.expiration_leases_only.enabled": "true",
			}
		case roachpb.LeaseEpoch:
			settings = map[string]string{
				"kv.lease.expiration_leases_only.enabled":       "false",
				"kv.raft.leader_fortification.fraction_enabled": "0.0",
			}
		case roachpb.LeaseLeader:
			settings = map[string]string{
				"kv.lease.expiration_leases_only.enabled":       "false",
				"kv.raft.leader_fortification.fraction_enabled": "1.0",
			}
		default:
			panic(errors.AssertionFailedf(`unknown LeaseType: %v`, op.LeaseType))
		}
	case ChangeSettingType_ToggleVirtualIntentResolution:
		val := "false"
		if op.VirEnabled {
			val = "true"
		}
		settings = map[string]string{
			"kv.concurrency.virtual_intent_resolution.enabled": val,
		}
	default:
		panic(errors.AssertionFailedf(`unknown ChangeSettingType: %v`, op.Type))
	}
	for name, val := range settings {
		if err := env.SetClusterSetting(ctx, name, val); err != nil {
			return err
		}
	}
	return nil
}

func updateZoneConfig(zone *zonepb.ZoneConfig, change ChangeZoneType) {
	switch change {
	case ChangeZoneType_ToggleGlobalReads:
		cur := zone.GlobalReads != nil && *zone.GlobalReads
		zone.GlobalReads = proto.Bool(!cur)
	default:
		panic(errors.AssertionFailedf(`unknown ChangeZoneType: %v`, change))
	}
}

func updateZoneConfigInEnv(ctx context.Context, env *Env, change ChangeZoneType) error {
	return env.UpdateZoneConfig(ctx, int(GeneratorDataTableID), func(zone *zonepb.ZoneConfig) {
		updateZoneConfig(zone, change)
	})
}
