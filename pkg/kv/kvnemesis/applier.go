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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Applier executes Steps.
type Applier struct {
	db *client.DB
	mu struct {
		syncutil.Mutex
		txns map[string]*client.Txn
	}
}

// MakeApplier constructs an Applier that executes against the given DB.
func MakeApplier(db *client.DB) *Applier {
	a := &Applier{
		db: db,
	}
	a.mu.txns = make(map[string]*client.Txn)
	return a
}

// Apply executes the given Step and mutates it with the result of execution. An
// error is only returned from Apply if there is an internal coding error within
// Applier, errors from a Step execution are saved in the Step itself.
func (a *Applier) Apply(ctx context.Context, step *Step) (retErr error) {
	step.Before = a.db.Clock().Now()
	defer func() {
		step.After = a.db.Clock().Now()
		if p := recover(); p != nil {
			retErr = errors.Errorf(`panic applying step %s: %v`, step, p)
		}
	}()
	a.applyOp(ctx, &step.Op)
	return nil
}

func (a *Applier) applyOp(ctx context.Context, op *Operation) {
	switch o := op.GetValue().(type) {
	case *GetOperation, *PutOperation, *BatchOperation:
		applyClientOp(ctx, a.db, op)
	case *SplitOperation:
		err := a.db.AdminSplit(ctx, o.Key, o.Key, hlc.MaxTimestamp)
		o.Result = resultError(ctx, err)
	case *MergeOperation:
		err := a.db.AdminMerge(ctx, o.Key)
		o.Result = resultError(ctx, err)
	case *ClosureTxnOperation:
		txnErr := a.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
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
	default:
		panic(errors.AssertionFailedf(`unknown operation type: %T %v`, o, o))
	}
}

type clientI interface {
	Get(context.Context, interface{}) (client.KeyValue, error)
	Put(context.Context, interface{}, interface{}) error
	Run(context.Context, *client.Batch) error
}

func applyClientOp(ctx context.Context, db clientI, op *Operation) {
	switch o := op.GetValue().(type) {
	case *GetOperation:
		result, err := db.Get(ctx, o.Key)
		if err != nil {
			o.Result = resultError(ctx, err)
		} else {
			o.Result.Type = ResultType_Value
			if result.Value != nil {
				if value, err := result.Value.GetBytes(); err != nil {
					panic(errors.Wrapf(err, "decoding %x", result.Value.RawBytes))
				} else {
					o.Result.Value = value
				}
			}
		}
	case *PutOperation:
		err := db.Put(ctx, o.Key, o.Value)
		o.Result = resultError(ctx, err)
	case *BatchOperation:
		b := &client.Batch{}
		applyBatchOp(ctx, b, db.Run, o)
	default:
		panic(errors.AssertionFailedf(`unknown batch operation type: %T %v`, o, o))
	}
}

func applyBatchOp(
	ctx context.Context,
	b *client.Batch,
	runFn func(context.Context, *client.Batch) error,
	o *BatchOperation,
) {
	for i := range o.Ops {
		switch subO := o.Ops[i].GetValue().(type) {
		case *GetOperation:
			b.Get(subO.Key)
		case *PutOperation:
			b.Put(subO.Key, subO.Value)
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
					if value, err := result.Value.GetBytes(); err != nil {
						panic(errors.Wrapf(err, "decoding %x", result.Value.RawBytes))
					} else {
						subO.Result.Value = value
					}
				}
			}
		case *PutOperation:
			err := b.Results[i].Err
			subO.Result = resultError(ctx, err)
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
