// Copyright 2014 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Andrew Bonventre (andybons@gmail.com)
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package engine

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/cockroachdb/cockroach/proto"
)

func TestInMemCapacity(t *testing.T) {
	// TODO(Tobias): Test for correct update during put()
	engine := NewInMem(proto.Attributes{}, 1<<20)
	c, err := engine.Capacity()
	if err != nil {
		t.Errorf("unexpected error fetching capacity: %v", err)
	}
	if c.Capacity != 1<<20 {
		t.Errorf("expected capacity to be %d, got %d", 1<<20, c.Capacity)
	}
	if c.Available != 1<<20 {
		t.Errorf("expected available to be %d, got %d", 1<<20, c.Available)
	}

	bytes := []byte("0123456789")

	// Add a key.
	err = engine.Put(Key(bytes), bytes)
	if err != nil {
		t.Errorf("put: expected no error, but got %s", err)
	}
	if c, err = engine.Capacity(); err != nil {
		t.Errorf("unexpected error fetching capacity: %v", err)
	}
	if c.Capacity != 1<<20 {
		t.Errorf("expected capacity to be %d, got %d", 1<<20, c.Capacity)
	}
	if !(c.Available < 1<<20) {
		t.Errorf("expected available to be < %d, got %d", 1<<20, c.Available)
	}

	// Remove key.
	err = engine.Clear(Key(bytes))
	if err != nil {
		t.Errorf("delete: expected no error, but got %s", err)
	}
	if c, err = engine.Capacity(); err != nil {
		t.Errorf("unexpected error fetching capacity: %v", err)
	}
	if c.Available != 1<<20 {
		t.Errorf("expected available to be %d, got %d", 1<<20, c.Available)
	}
}

func TestInMemOverCapacity(t *testing.T) {
	value := []byte("0123456789")
	// Create an engine with enough space for one, but not two, nodes.
	engine := NewInMem(proto.Attributes{},
		int64(float64(computeSize(RawKeyValue{Key: Key("X"), Value: value}))*1.5))
	var err error
	if err = engine.Put(Key("1"), value); err != nil {
		t.Errorf("put: expected no error, but got %s", err)
	}
	if err = engine.Put(Key("2"), value); err == nil {
		t.Error("put: expected error, but got none")
	}
}

func BenchmarkCapacity(b *testing.B) {
	engine := NewInMem(proto.Attributes{}, 1<<30)
	bytes := []byte("0123456789")
	for i := 0; i < b.N; i++ {
		if err := engine.Put(Key(fmt.Sprintf("%d", i)), bytes); err != nil {
			b.Fatalf("put: expected no error, but got %s", err)
		}
		if i%10000 == 0 {
			_, err := engine.Capacity()
			if err != nil {
				b.Errorf("unexpected error fetching capacity: %v", err)
			}
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
		}
	}
}
