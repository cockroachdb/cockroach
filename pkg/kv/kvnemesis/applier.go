// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvnemesis

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

// Applier executes Steps.
type Applier struct {
	env *Env
	dbs []*kv.DB
	mu  struct {
		dbIdx int
		syncutil.Mutex
		txns map[string]*kv.Txn
	}
}

// MakeApplier constructs an Applier that executes against the given DBs.
func MakeApplier(env *Env, dbs ...*kv.DB) *Applier {
	a := &Applier{
		env: env,
		dbs: dbs,
	}
	a.mu.txns = make(map[string]*kv.Txn)
	return a
}

// Apply executes the given Step and mutates it with the result of execution. An
// error is only returned from Apply if there is an internal coding error within
// Applier, errors from a Step execution are saved in the Step itself.
func (a *Applier) Apply(ctx context.Context, step *Step) (trace tracing.Recording, retErr error) {
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
	applyOp(recCtx, a.env, db, &step.Op)
	return collectAndFinish(), nil
}

func (a *Applier) getNextDBRoundRobin() (*kv.DB, int32) {
	a.mu.Lock()
	dbIdx := a.mu.dbIdx
	a.mu.dbIdx = (a.mu.dbIdx + 1) % len(a.dbs)
	a.mu.Unlock()
	return a.dbs[dbIdx], int32(dbIdx)
}

func applyOp(ctx context.Context, env *Env, db *kv.DB, op *Operation) {
	switch o := op.GetValue().(type) {
	case *GetOperation,
		*PutOperation,
		*ScanOperation,
		*BatchOperation,
		*DeleteOperation,
		*DeleteRangeOperation:
		applyClientOp(ctx, db, op, false /* inTxn */)
	case *SplitOperation:
		err := db.AdminSplit(ctx, o.Key, hlc.MaxTimestamp)
		o.Result = resultError(ctx, err)
	case *MergeOperation:
		err := db.AdminMerge(ctx, o.Key)
		o.Result = resultError(ctx, err)
	case *ChangeReplicasOperation:
		desc := getRangeDesc(ctx, o.Key, db)
		_, err := db.AdminChangeReplicas(ctx, o.Key, desc, o.Changes)
		// TODO(dan): Save returned desc?
		o.Result = resultError(ctx, err)
	case *TransferLeaseOperation:
		err := db.AdminTransferLease(ctx, o.Key, o.Target)
		o.Result = resultError(ctx, err)
	case *ChangeZoneOperation:
		err := updateZoneConfigInEnv(ctx, env, o.Type)
		o.Result = resultError(ctx, err)
	case *ClosureTxnOperation:
		// Use a backoff loop to avoid thrashing on txn aborts. Don't wait between
		// epochs of the same transaction to avoid waiting while holding locks.
		retryOnAbort := retry.StartWithCtx(ctx, retry.Options{
			InitialBackoff: 1 * time.Millisecond,
			MaxBackoff:     250 * time.Millisecond,
		})
		var savedTxn *kv.Txn
		txnErr := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			if savedTxn != nil && txn.TestingCloneTxn().Epoch == 0 {
				// If the txn's current epoch is 0 and we've run at least one prior
				// iteration, we were just aborted.
				retryOnAbort.Next()
			}
			savedTxn = txn
			for i := range o.Ops {
				op := &o.Ops[i]
				applyClientOp(ctx, txn, op, true /* inTxn */)
				// The KV api disallows use of a txn after an operation on it errors.
				if r := op.Result(); r.Type == ResultType_Error {
					return errors.DecodeError(ctx, *r.Err)
				}
			}
			if o.CommitInBatch != nil {
				b := txn.NewBatch()
				applyBatchOp(ctx, b, txn.CommitInBatch, o.CommitInBatch, true)
				// The KV api disallows use of a txn after an operation on it errors.
				if r := o.CommitInBatch.Result; r.Type == ResultType_Error {
					return errors.DecodeError(ctx, *r.Err)
				}
			}
			switch o.Type {
			case ClosureTxnType_Commit:
				return nil
			case ClosureTxnType_Rollback:
				return errors.New("rollback")
			default:
				panic(errors.AssertionFailedf(`unknown closure txn type: %s`, o.Type))
			}
		})
		o.Result = resultError(ctx, txnErr)
		if txnErr == nil {
			o.Txn = savedTxn.TestingCloneTxn()
		}
	default:
		panic(errors.AssertionFailedf(`unknown operation type: %T %v`, o, o))
	}
}

type clientI interface {
	Get(context.Context, interface{}) (kv.KeyValue, error)
	GetForUpdate(context.Context, interface{}) (kv.KeyValue, error)
	Put(context.Context, interface{}, interface{}) error
	Scan(context.Context, interface{}, interface{}, int64) ([]kv.KeyValue, error)
	ScanForUpdate(context.Context, interface{}, interface{}, int64) ([]kv.KeyValue, error)
	ReverseScan(context.Context, interface{}, interface{}, int64) ([]kv.KeyValue, error)
	ReverseScanForUpdate(context.Context, interface{}, interface{}, int64) ([]kv.KeyValue, error)
	Del(context.Context, ...interface{}) error
	DelRange(context.Context, interface{}, interface{}, bool) ([]roachpb.Key, error)
	Run(context.Context, *kv.Batch) error
}

func applyClientOp(ctx context.Context, db clientI, op *Operation, inTxn bool) {
	switch o := op.GetValue().(type) {
	case *GetOperation:
		fn := db.Get
		if o.ForUpdate {
			fn = db.GetForUpdate
		}
		kv, err := fn(ctx, o.Key)
		if err != nil {
			o.Result = resultError(ctx, err)
		} else {
			o.Result.Type = ResultType_Value
			if kv.Value != nil {
				o.Result.Value = kv.Value.RawBytes
			} else {
				o.Result.Value = nil
			}
		}
	case *PutOperation:
		err := db.Put(ctx, o.Key, o.Value)
		o.Result = resultError(ctx, err)
	case *ScanOperation:
		fn := db.Scan
		if o.Reverse && o.ForUpdate {
			fn = db.ReverseScanForUpdate
		} else if o.Reverse {
			fn = db.ReverseScan
		} else if o.ForUpdate {
			fn = db.ScanForUpdate
		}
		kvs, err := fn(ctx, o.Key, o.EndKey, 0 /* maxRows */)
		if err != nil {
			o.Result = resultError(ctx, err)
		} else {
			o.Result.Type = ResultType_Values
			o.Result.Values = make([]KeyValue, len(kvs))
			for i, kv := range kvs {
				o.Result.Values[i] = KeyValue{
					Key:   []byte(kv.Key),
					Value: kv.Value.RawBytes,
				}
			}
		}
	case *DeleteOperation:
		err := db.Del(ctx, o.Key)
		o.Result = resultError(ctx, err)
	case *DeleteRangeOperation:
		if !inTxn {
			panic(errors.AssertionFailedf(`non-transactional DelRange operations currently unsupported`))
		}
		deletedKeys, err := db.DelRange(ctx, o.Key, o.EndKey, true /* returnKeys */)
		if err != nil {
			o.Result = resultError(ctx, err)
		} else {
			o.Result.Type = ResultType_Keys
			o.Result.Keys = make([][]byte, len(deletedKeys))
			for i, deletedKey := range deletedKeys {
				o.Result.Keys[i] = deletedKey
			}
		}
	case *BatchOperation:
		b := &kv.Batch{}
		applyBatchOp(ctx, b, db.Run, o, inTxn)
	default:
		panic(errors.AssertionFailedf(`unknown batch operation type: %T %v`, o, o))
	}
}

func applyBatchOp(
	ctx context.Context,
	b *kv.Batch,
	runFn func(context.Context, *kv.Batch) error,
	o *BatchOperation,
	inTxn bool,
) {
	for i := range o.Ops {
		switch subO := o.Ops[i].GetValue().(type) {
		case *GetOperation:
			if subO.ForUpdate {
				b.GetForUpdate(subO.Key)
			} else {
				b.Get(subO.Key)
			}
		case *PutOperation:
			b.Put(subO.Key, subO.Value)
		case *ScanOperation:
			if subO.Reverse && subO.ForUpdate {
				b.ReverseScanForUpdate(subO.Key, subO.EndKey)
			} else if subO.Reverse {
				b.ReverseScan(subO.Key, subO.EndKey)
			} else if subO.ForUpdate {
				b.ScanForUpdate(subO.Key, subO.EndKey)
			} else {
				b.Scan(subO.Key, subO.EndKey)
			}
		case *DeleteOperation:
			b.Del(subO.Key)
		case *DeleteRangeOperation:
			if !inTxn {
				panic(errors.AssertionFailedf(`non-transactional batch DelRange operations currently unsupported`))
			}
			b.DelRange(subO.Key, subO.EndKey, true /* returnKeys */)
		default:
			panic(errors.AssertionFailedf(`unknown batch operation type: %T %v`, subO, subO))
		}
	}
	runErr := runFn(ctx, b)
	o.Result = resultError(ctx, runErr)
	for i := range o.Ops {
		switch subO := o.Ops[i].GetValue().(type) {
		case *GetOperation:
			if b.Results[i].Err != nil {
				subO.Result = resultError(ctx, b.Results[i].Err)
			} else {
				subO.Result.Type = ResultType_Value
				result := b.Results[i].Rows[0]
				if result.Value != nil {
					subO.Result.Value = result.Value.RawBytes
				} else {
					subO.Result.Value = nil
				}
			}
		case *PutOperation:
			err := b.Results[i].Err
			subO.Result = resultError(ctx, err)
		case *ScanOperation:
			kvs, err := b.Results[i].Rows, b.Results[i].Err
			if err != nil {
				subO.Result = resultError(ctx, err)
			} else {
				subO.Result.Type = ResultType_Values
				subO.Result.Values = make([]KeyValue, len(kvs))
				for j, kv := range kvs {
					subO.Result.Values[j] = KeyValue{
						Key:   []byte(kv.Key),
						Value: kv.Value.RawBytes,
					}
				}
			}
		case *DeleteOperation:
			err := b.Results[i].Err
			subO.Result = resultError(ctx, err)
		case *DeleteRangeOperation:
			keys, err := b.Results[i].Keys, b.Results[i].Err
			if err != nil {
				subO.Result = resultError(ctx, err)
			} else {
				subO.Result.Type = ResultType_Keys
				subO.Result.Keys = make([][]byte, len(keys))
				for j, key := range keys {
					subO.Result.Keys[j] = key
				}
			}
		default:
			panic(errors.AssertionFailedf(`unknown batch operation type: %T %v`, subO, subO))
		}
	}
}

func resultError(ctx context.Context, err error) Result {
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
		descs, _, err := kv.RangeLookup(ctx, sender, key, roachpb.CONSISTENT, 0, false)
		if err != nil {
			log.Infof(ctx, "looking up descriptor for %s: %+v", key, err)
			continue
		}
		if len(descs) != 1 {
			log.Infof(ctx, "unexpected number of descriptors for %s: %d", key, len(descs))
			continue
		}
		return descs[0]
	}
	panic(`unreachable`)
}

func newGetReplicasFn(dbs ...*kv.DB) GetReplicasFn {
	ctx := context.Background()
	return func(key roachpb.Key) []roachpb.ReplicationTarget {
		desc := getRangeDesc(ctx, key, dbs...)
		replicas := desc.Replicas().Descriptors()
		targets := make([]roachpb.ReplicationTarget, len(replicas))
		for i, replica := range replicas {
			targets[i] = roachpb.ReplicationTarget{
				NodeID:  replica.NodeID,
				StoreID: replica.StoreID,
			}
		}
		return targets
	}
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
