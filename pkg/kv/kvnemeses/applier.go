// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvnemeses

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
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

// Apply executes a Step and fills in the Result field of the Operations within
// it.
func (a *Applier) Apply(ctx context.Context, step *Step) error {
	g := ctxgroup.WithContext(ctx)
	for i := range step.Ops {
		i := i
		g.GoCtx(func(ctx context.Context) (retErr error) {
			defer func() {
				if p := recover(); p != nil {
					retErr = errors.Errorf(`panic applying op %d %s: %v`, i, step.Ops[i], p)
				}
			}()
			a.applyOp(ctx, &step.Ops[i])
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	step.AfterTimestamp = a.db.Clock().Now()
	return nil
}

func (a *Applier) applyOp(ctx context.Context, op *Operation) {
	switch o := op.GetValue().(type) {
	case *GetOperation, *PutOperation, *BatchOperation:
		applyBatchOp(ctx, a.db, op)
	case *SplitOperation:
		expiration := a.db.Clock().Now().Add(time.Hour.Nanoseconds(), 0)
		err := a.db.AdminSplit(ctx, o.Key, o.Key, expiration)
		o.Result = resultError(ctx, err)
	case *MergeOperation:
		err := a.db.AdminMerge(ctx, o.Key)
		o.Result = resultError(ctx, err)
	case *BeginTxnOperation:
		txn := a.db.NewTxn(ctx, o.TxnID)
		a.mu.Lock()
		a.mu.txns[o.TxnID] = txn
		a.mu.Unlock()
		o.Result.Type = ResultType_NoError
	case *UseTxnOperation:
		a.mu.Lock()
		txn := a.mu.txns[o.TxnID]
		a.mu.Unlock()
		for i := range o.Ops {
			applyBatchOp(ctx, txn, &o.Ops[i])
		}
		o.Result.Type = ResultType_NoError
	case *CommitTxnOperation:
		a.mu.Lock()
		txn := a.mu.txns[o.TxnID]
		delete(a.mu.txns, o.TxnID)
		a.mu.Unlock()
		err := txn.CommitOrCleanup(ctx)
		o.Result = resultError(ctx, err)
	case *RollbackTxnOperation:
		a.mu.Lock()
		txn := a.mu.txns[o.TxnID]
		delete(a.mu.txns, o.TxnID)
		a.mu.Unlock()
		err := txn.Rollback(ctx)
		o.Result = resultError(ctx, err)
	default:
		panic(errors.AssertionFailedf(`unknown operation type: %T %v`, o, o))
	}
}

type batchI interface {
	Get(context.Context, interface{}) (client.KeyValue, error)
	Put(context.Context, interface{}, interface{}) error
	Run(context.Context, *client.Batch) error
}

func applyBatchOp(ctx context.Context, db batchI, op *Operation) {
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
		runErr := db.Run(ctx, b)
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
	default:
		panic(errors.AssertionFailedf(`unknown batch operation type: %T %v`, o, o))
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
