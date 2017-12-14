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

package compactor

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

type wrappedEngine struct {
	engine.Engine
	capacityFn func() roachpb.StoreCapacity
	getSizeFn  func(engine.MVCCKey, engine.MVCCKey) int64
}

func newWrappedEngine() *wrappedEngine {
	return &wrappedEngine{
		Engine: engine.NewInMem(roachpb.Attributes{}, 1<<20),
	}
}

func (we *wrappedEngine) Capacity() roachpb.StoreCapacity {
	if we.capacityFn == nil {
		return we.Engine.Capacity()
	}
	return we.capacityFn()
}

func (we *wrappedEngine) GetApproximateSize(start, end engine.MVCCKey) int64 {
	return we.getSizeFn(start, end)
}

func testSetup() (*Compactor, func()) {
	stopper := stop.NewStopper()
	eng := newWrappedEngine()
	stopper.AddCloser(eng)
	compactor := NewCompactor(eng)
	compactor.Start(context.Background(), tracing.NewTracer(), stopper)
	return compactor, func() {
		stopper.Stop(context.Background())
	}
}

//
func TestCompactor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	compactor, cleanup := testSetup()
	defer cleanup()

	compactor.capacityFn = func() roachpb.StoreCapacity {
		return roachpb.StoreCapacity{
			LogicalBytes: 10,
		}
	}
	compactor.getSizeFn = func(start, end engine.MVCCKey) int64 {
		return 0
	}
}
