// Copyright 2016 The Cockroach Authors.
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
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package distsql

import (
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/util/uuid"
)

func TestFlowRegistry(t *testing.T) {
	reg := makeFlowRegistry()

	id1 := FlowID{uuid.MakeV4()}
	f1 := &Flow{}

	id2 := FlowID{uuid.MakeV4()}
	f2 := &Flow{}

	id3 := FlowID{uuid.MakeV4()}
	f3 := &Flow{}

	// A basic duration; needs to be significantly larger than possible delays
	// in scheduling goroutines.
	jiffy := 10 * time.Millisecond

	// -- Lookup, register, lookup, unregister, lookup. --

	if f := reg.LookupFlow(id1, 0); f != nil {
		t.Error("looked up unregistered flow")
	}

	reg.RegisterFlow(id1, f1)

	if f := reg.LookupFlow(id1, 0); f != f1 {
		t.Error("couldn't lookup previously registered flow")
	}

	reg.UnregisterFlow(id1)

	if f := reg.LookupFlow(id1, 0); f != nil {
		t.Error("looked up unregistered flow")
	}

	// -- Lookup with timeout, register in the meantime. --

	go func() {
		time.Sleep(jiffy)
		reg.RegisterFlow(id1, f1)
	}()

	if f := reg.LookupFlow(id1, 10*jiffy); f != f1 {
		t.Error("couldn't lookup registered flow (with wait)")
	}

	if f := reg.LookupFlow(id1, 0); f != f1 {
		t.Error("couldn't lookup registered flow")
	}

	// -- Multiple lookups before register. --

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		if f := reg.LookupFlow(id2, 10*jiffy); f != f2 {
			t.Error("couldn't lookup registered flow (with wait)")
		}
		wg.Done()
	}()

	go func() {
		if f := reg.LookupFlow(id2, 10*jiffy); f != f2 {
			t.Error("couldn't lookup registered flow (with wait)")
		}
		wg.Done()
	}()

	time.Sleep(jiffy)
	reg.RegisterFlow(id2, f2)
	wg.Wait()

	// -- Multiple lookups, with the first one failing. --

	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup

	wg1.Add(1)
	wg2.Add(1)
	go func() {
		if f := reg.LookupFlow(id3, jiffy); f != nil {
			t.Error("expected lookup to fail")
		}
		wg1.Done()
	}()

	go func() {
		if f := reg.LookupFlow(id3, 10*jiffy); f != f3 {
			t.Error("couldn't lookup registered flow (with wait)")
		}
		wg2.Done()
	}()

	wg1.Wait()
	reg.RegisterFlow(id3, f3)
	wg2.Wait()
}
