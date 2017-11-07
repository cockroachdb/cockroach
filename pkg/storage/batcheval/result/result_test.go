// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package result

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
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
			case *storagebase.ReplicatedEvalResult:
				f.IsLeaseRequest = true
				defer func() { f.IsLeaseRequest = false }()
			case **storagebase.WriteBatch:
				*f = new(storagebase.WriteBatch)
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
