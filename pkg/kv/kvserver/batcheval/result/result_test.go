// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package result

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestEvalResultIsZero(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var p Result
	if !p.IsZero() {
		t.Fatalf("%v unexpectedly non-zero", p)
	}

	v := reflect.ValueOf(&p).Elem()
	for i := 0; i < v.NumField(); i++ {
		func() {
			vf := v.Field(i)
			if vf.CanAddr() {
				vf = vf.Addr()
			}
			switch f := vf.Interface().(type) {
			case *LocalResult:
				f.GossipFirstRange = true
				defer func() { f.GossipFirstRange = false }()
			case *kvserverpb.ReplicatedEvalResult:
				f.IsLeaseRequest = true
				defer func() { f.IsLeaseRequest = false }()
			case **kvserverpb.WriteBatch:
				*f = new(kvserverpb.WriteBatch)
				defer func() { *f = nil }()
			case **kvserverpb.LogicalOpLog:
				*f = new(kvserverpb.LogicalOpLog)
				defer func() { *f = nil }()
			default:
				tf := v.Type().Field(i)
				t.Fatalf("unknown field %s of type %s on %T", tf.Name, tf.Type, p)
			}

			if p.IsZero() {
				t.Fatalf("%#v unexpectedly zero", p)
			}
		}()

		if !p.IsZero() {
			t.Fatalf("%v unexpectedly non-zero", p)
		}
	}
}

func TestMergeAndDestroy(t *testing.T) {
	var r0, r1, r2 Result
	r1.Local.Metrics = new(Metrics)
	r2.Local.Metrics = new(Metrics)

	r1.Local.Metrics.LeaseRequestSuccess = 7

	r2.Local.Metrics.ResolveAbort = 13
	r2.Local.Metrics.LeaseRequestSuccess = 2

	if err := r0.MergeAndDestroy(r1); err != nil {
		t.Fatal(err)
	}

	if err := r0.MergeAndDestroy(r2); err != nil {
		t.Fatal(err)
	}

	if f, exp := *r1.Local.Metrics, (Metrics{LeaseRequestSuccess: 9, ResolveAbort: 13}); f != exp {
		t.Fatalf("expected %d, got %d", exp, f)
	}

	var r3 Result
	r3.Replicated.State = &kvserverpb.ReplicaState{
		ForceFlushIndex: roachpb.ForceFlushIndex{Index: 3},
	}
	require.ErrorContains(t, r0.MergeAndDestroy(r3), "must not specify ForceFlushIndex")

	var r4 Result
	r4.Replicated.DoTimelyApplicationToAllReplicas = true
	require.False(t, r0.Replicated.DoTimelyApplicationToAllReplicas)
	require.NoError(t, r0.MergeAndDestroy(r4))
	require.True(t, r0.Replicated.DoTimelyApplicationToAllReplicas)
}
