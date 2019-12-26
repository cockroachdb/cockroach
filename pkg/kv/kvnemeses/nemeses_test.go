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
	"bytes"
	"context"
	"math/rand"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestKVNemesesSingleNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	scope := log.Scope(t)
	defer scope.Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	db := tc.Server(0).DB()
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1s'`)

	rng, _ := randutil.NewPseudoRand()
	steps, kvs, err := RunNemeses(ctx, rng, db)
	require.NoError(t, err)

	v := MakeValidator()
	for _, step := range steps {
		v.Step(step)
	}
	for _, failure := range v.Failures() {
		t.Errorf("failure:\n%+v", failure)
	}

	if t.Failed() {
		t.Logf("kvs:\n%s", kvs.DebugPrint())
	}
}

// RunNemeses generates and applies a series of Operations to exercise the KV
// api. It returns the resulting log of inputs and outputs.
func RunNemeses(ctx context.Context, rng *rand.Rand, db *client.DB) ([]Step, *Engine, error) {
	s := MakeStepper()
	a := MakeApplier(db)
	r, err := MakeResulter(ctx, db, s.DataSpan())
	if err != nil {
		return nil, nil, err
	}
	defer r.Close()

	var steps []Step
	var buf strings.Builder
	for i := 0; i < 5; i++ {
		steps = append(steps, s.RandStep(rng))
		step := &steps[i]

		buf.Reset()
		step.format(&buf, `  REQUEST  `)
		log.Info(ctx, buf.String())
		if err := a.Apply(ctx, step); err != nil {
			return nil, nil, err
		}
		buf.Reset()
		step.format(&buf, `  RESPONSE `)
		log.Info(ctx, buf.String())
	}
	if err := r.Annotate(ctx, steps); err != nil {
		return nil, nil, err
	}

	kvs := r.dataWatcher.Finish()
	return steps, kvs, nil
}

// Validator checks for violations of our kv api guarantees.
type Validator struct {
	failures []error
}

// MakeValidator returns a new Validator.
func MakeValidator() *Validator {
	return &Validator{}
}

// Step processes a single set of concurrently run Operations. This Step must
// already have been Annotated by a Resulter.
func (v *Validator) Step(s Step) {
	// TODO(dan): Validatate our transaction guarantees.

	var validateOps func(*string, []Operation)
	validateOps = func(txnID *string, ops []Operation) {
		for _, op := range ops {
			switch t := op.GetValue().(type) {
			case *GetOperation:
				v.failIfError(op, t.Result)
			case *PutOperation:
				v.failIfError(op, t.Result)
			case *BatchOperation:
				// TODO(dan): This gets retryable errors, but the client package doesn't
				// currently expose a way to sniff them out.
				//
				// v.failIfError(op, t.Result)
				// validateOps(txnID, t.Ops)
			case *SplitOperation:
				v.failIfError(op, t.Result)
			case *MergeOperation:
				// TODO(dan): "merge failed: unexpected value". Nemeses's first bug find?
				// v.failIfError(op, t.Result)
			case *BeginTxnOperation:
				v.failIfError(op, t.Result)
			case *UseTxnOperation:
				v.failIfError(op, t.Result)
				// TODO(dan): This gets retryable errors, but the client package doesn't
				// currently expose a way to sniff them out.
				//
				// validateOps(&t.TxnID, t.Ops)
			}
		}
	}
	validateOps(nil, s.Ops)
}

func (v *Validator) failIfError(op Operation, r Result) {
	switch r.Type {
	case ResultType_Unknown:
		err := errors.AssertionFailedf(`unknown result %s`, op)
		v.failures = append(v.failures, err)
	case ResultType_Error:
		ctx := context.Background()
		err := errors.DecodeError(ctx, *r.Err)
		err = errors.Wrapf(err, `error applying %s`, op)
		v.failures = append(v.failures, err)
	}
}

// Failures returns any violations seen so far.
func (v *Validator) Failures() []error {
	return v.failures
}

// Resulter tracks the kv changes that happen over a span.
type Resulter struct {
	db          *client.DB
	dataWatcher *Watcher
}

// MakeResulter returns a Resulter for the given span. All changes that happen
// between when this method returns and when Finish is first called are
// guaranteed to be captured.
func MakeResulter(ctx context.Context, db *client.DB, dataSpan roachpb.Span) (*Resulter, error) {
	r := &Resulter{db: db}
	var err error
	if r.dataWatcher, err = Watch(ctx, db, dataSpan); err != nil {
		return nil, err
	}
	return r, nil
}

// Close ensures the the Resulter is torn down. It may be called multiple times.
func (r *Resulter) Close() {
	_ = r.dataWatcher.Finish()
}

// Annotate fills the AfterValue fields of the given Steps.
func (r *Resulter) Annotate(ctx context.Context, steps []Step) error {
	if len(steps) == 0 {
		return nil
	}
	lastStep := steps[len(steps)-1]

	// TODO(dan): Also slurp the splits. The meta ranges use expiration based
	// leases, so we can't use RangeFeed to do it. Maybe ExportRequest?

	if err := r.dataWatcher.WaitForFrontier(ctx, lastStep.AfterTimestamp); err != nil {
		return errors.Wrap(err, `waiting for data watcher`)
	}
	kvs := r.dataWatcher.Finish()
	annotateAfter(steps, kvs, r.db)

	return nil
}

func annotateAfter(steps []Step, kvs *Engine, db *client.DB) {
	var beforeTimestamp hlc.Timestamp
	for i := range steps {
		step := &steps[i]
		for j := range step.Ops {
			annotateAfterOperation(&step.Ops[j], kvs, beforeTimestamp, step.AfterTimestamp, db)
		}
		beforeTimestamp = step.AfterTimestamp
	}
}

func annotateAfterOperation(
	op *Operation, kvs *Engine, before, after hlc.Timestamp, db *client.DB,
) {
	switch t := op.GetValue().(type) {
	case *GetOperation:
		if a := kvs.Get(roachpb.Key(t.Key), after); a.IsPresent() {
			var err error
			if t.AfterValue, err = a.GetBytes(); err != nil {
				panic(err)
			}
		}
	case *PutOperation:
		if a := kvs.Get(roachpb.Key(t.Key), after); a.IsPresent() {
			var err error
			if t.AfterValue, err = a.GetBytes(); err != nil {
				panic(err)
			}
		}
		// Sanity check the RangeFeed.
		if err := expectValue(db, t.Key, after, t.AfterValue); err != nil {
			panic(err)
		}
	case *BatchOperation:
		for i := range t.Ops {
			annotateAfterOperation(&t.Ops[i], kvs, before, after, db)
		}
	case *UseTxnOperation:
		for i := range t.Ops {
			annotateAfterOperation(&t.Ops[i], kvs, before, after, db)
		}
	}
}

func expectValue(db *client.DB, key []byte, ts hlc.Timestamp, expected []byte) error {
	var actual client.KeyValue
	fn := func(ctx context.Context, txn *client.Txn) error {
		txn.SetFixedTimestamp(ctx, ts)
		var err error
		actual, err = txn.Get(ctx, key)
		return err
	}
	if err := db.Txn(context.Background(), fn); err != nil {
		return err
	}
	var actualVal []byte
	if actual.Value.IsPresent() {
		var err error
		if actualVal, err = actual.Value.GetBytes(); err != nil {
			panic(err)
		}
	}
	if !bytes.Equal(expected, actualVal) {
		return errors.Errorf(`expected %s %s to be %s got: %s`, key, ts, expected, actualVal)
	}
	return nil
}
