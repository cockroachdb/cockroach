// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package result

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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
}
