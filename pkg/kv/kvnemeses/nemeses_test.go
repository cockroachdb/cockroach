// Copyright 2019 The Cockroach Authors.
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
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"

	// errorspb "github.com/cockroachdb/errors/errorspb"
	"github.com/stretchr/testify/require"
)

func TestKVNemeses(t *testing.T) {
	defer leaktest.AfterTest(t)()
	scope := log.Scope(t)
	defer scope.Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	rng, _ := randutil.NewPseudoRand()
	log, err := RunNemeses(ctx, rng, tc.Server(0).DB())
	require.NoError(t, err)
	// TODO(dan): Validate the log.
	if t.Failed() {
		for _, step := range log {
			t.Logf(`step: %s`, step)
		}
	}
}

// RunNemeses generates and applies a series of Operations to exercise the KV
// api. It returns the resulting log of inputs and outputs.
func RunNemeses(ctx context.Context, rng *rand.Rand, db *client.DB) ([]Step, error) {
	s := MakeStepper()
	a := MakeApplier(db)
	var stepLog []Step
	for i := 0; i < 30; i++ {
		step := s.RandStep(rng)
		stepLog = append(stepLog, step)

		var buf strings.Builder
		step.format(&buf, `  REQUEST `)
		log.Info(ctx, buf.String())
		for _, op := range step.Ops {
			log.Info(ctx, op.GetValue())
		}

		if err := a.Apply(ctx, step); err != nil {
			return nil, err
		}
	}
	// TODO(dan): Use RangeFeed to annotate the log with the actual KV history.
	return stepLog, nil
}

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

// Apply executes a Step.
func (a *Applier) Apply(ctx context.Context, step Step) error {
	g := ctxgroup.WithContext(ctx)
	for i := range step.Ops {
		op := step.Ops[i]
		g.GoCtx(func(ctx context.Context) (retErr error) {
			defer func() {
				if p := recover(); p != nil {
					retErr = errors.Errorf(`panic applying op %s: %v`, op, p)
				}
			}()
			a.applyOp(ctx, op)
			return nil
		})
	}
	return g.Wait()
}

func (a *Applier) applyOp(ctx context.Context, op Operation) {
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
		for _, op := range o.Ops {
			applyBatchOp(ctx, txn, op)
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

func applyBatchOp(ctx context.Context, db batchI, op Operation) {
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
		for _, subOp := range o.Ops {
			switch subO := subOp.GetValue().(type) {
			case *GetOperation:
				b.Get(subO.Key)
			case *PutOperation:
				b.Put(subO.Key, subO.Value)
			default:
				panic(errors.AssertionFailedf(`unknown batch operation type: %T %v`, subO, subO))
			}
		}
		err := db.Run(ctx, b)
		// TODO(dan): Fill in results from batch.
		o.Result = resultError(ctx, err)
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
