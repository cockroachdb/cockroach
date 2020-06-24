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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Applier executes Steps.
type Applier struct {
	dbs []*kv.DB
	mu  struct {
		dbIdx int
		syncutil.Mutex
		txns map[string]*kv.Txn
	}
}

// MakeApplier constructs an Applier that executes against the given DB.
func MakeApplier(dbs ...*kv.DB) *Applier {
	a := &Applier{
		dbs: dbs,
	}
	a.mu.txns = make(map[string]*kv.Txn)
	return a
}

// Apply executes the given Step and mutates it with the result of execution. An
// error is only returned from Apply if there is an internal coding error within
// Applier, errors from a Step execution are saved in the Step itself.
func (a *Applier) Apply(ctx context.Context, step *Step) (retErr error) {
	var db *kv.DB
	db, step.DBID = a.getNextDBRoundRobin()

	step.Before = db.Clock().Now()
	defer func() {
		step.After = db.Clock().Now()
		if p := recover(); p != nil {
			retErr = errors.Errorf(`panic applying step %s: %v`, step, p)
		}
	}()
	applyOp(ctx, db, &step.Op)
	return nil
}

func (a *Applier) getNextDBRoundRobin() (*kv.DB, int32) {
	a.mu.Lock()
	dbIdx := a.mu.dbIdx
	a.mu.dbIdx = (a.mu.dbIdx + 1) % len(a.dbs)
	a.mu.Unlock()
	return a.dbs[dbIdx], int32(dbIdx)
}

func applyOp(ctx context.Context, db *kv.DB, op *Operation) {
	switch o := op.GetValue().(type) {
	case *GetOperation, *PutOperation, *ScanOperation, *BatchOperation:
		applyClientOp(ctx, db, op)
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
	case *ClosureTxnOperation:
		var savedTxn *kv.Txn
		txnErr := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			savedTxn = txn
			for i := range o.Ops {
				op := &o.Ops[i]
				applyClientOp(ctx, txn, op)
				// The KV api disallows use of a txn after an operation on it errors.
				if r := op.Result(); r.Type == ResultType_Error {
					return errors.DecodeError(ctx, *r.Err)
				}
			}
			if o.CommitInBatch != nil {
				b := txn.NewBatch()
				applyBatchOp(ctx, b, txn.CommitInBatch, o.CommitInBatch)
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
			o.Txn = savedTxn.Sender().TestingCloneTxn()
		}
	default:
		panic(errors.AssertionFailedf(`unknown operation type: %T %v`, o, o))
	}
}

type clientI interface {
	Get(context.Context, interface{}) (kv.KeyValue, error)
	Put(context.Context, interface{}, interface{}) error
	Scan(context.Context, interface{}, interface{}, int64) ([]kv.KeyValue, error)
	ScanForUpdate(context.Context, interface{}, interface{}, int64) ([]kv.KeyValue, error)
	Run(context.Context, *kv.Batch) error
}

func applyClientOp(ctx context.Context, db clientI, op *Operation) {
	switch o := op.GetValue().(type) {
	case *GetOperation:
		kv, err := db.Get(ctx, o.Key)
		if err != nil {
			o.Result = resultError(ctx, err)
		} else {
			o.Result.Type = ResultType_Value
			if kv.Value != nil {
				o.Result.Value = kv.Value.RawBytes
			}
		}
	case *PutOperation:
		err := db.Put(ctx, o.Key, o.Value)
		o.Result = resultError(ctx, err)
	case *ScanOperation:
		fn := db.Scan
		if o.ForUpdate {
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
	case *BatchOperation:
		b := &kv.Batch{}
		applyBatchOp(ctx, b, db.Run, o)
	default:
		panic(errors.AssertionFailedf(`unknown batch operation type: %T %v`, o, o))
	}
}

func applyBatchOp(
	ctx context.Context, b *kv.Batch, runFn func(context.Context, *kv.Batch) error, o *BatchOperation,
) {
	for i := range o.Ops {
		switch subO := o.Ops[i].GetValue().(type) {
		case *GetOperation:
			b.Get(subO.Key)
		case *PutOperation:
			b.Put(subO.Key, subO.Value)
		case *ScanOperation:
			if subO.ForUpdate {
				b.ScanForUpdate(subO.Key, subO.EndKey)
			} else {
				b.Scan(subO.Key, subO.EndKey)
			}
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
		replicas := desc.Replicas().All()
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
