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

package distsqlrun

import (
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

// lookupFlow returns the registered flow with the given ID. If no such flow is
// registered, waits until it gets registered - up to the given timeout. If the
// timeout elapses and the flow is not registered, the bool return value will be
// false.
func lookupFlow(fr *flowRegistry, fid FlowID, timeout time.Duration) *Flow {
	fr.Lock()
	defer fr.Unlock()
	entry := fr.waitForFlowLocked(fid, timeout)
	if entry == nil {
		return nil
	}
	return entry.flow
}

// lookupStreamInfo returns a stream entry from a flowRegistry. If either the
// flow or the streams are missing, an error is returned.
//
// A copy of the registry's inboundStreamInfo is returned so it can be accessed
// without locking.
func lookupStreamInfo(fr *flowRegistry, fid FlowID, sid StreamID) (inboundStreamInfo, error) {
	fr.Lock()
	defer fr.Unlock()
	entry := fr.getEntryLocked(fid)
	if entry.flow == nil {
		return inboundStreamInfo{}, errors.Errorf("missing flow entry: %s", fid)
	}
	si, ok := entry.inboundStreams[sid]
	if !ok {
		return inboundStreamInfo{}, errors.Errorf("missing stream entry: %d", sid)
	}
	return *si, nil
}

func TestFlowRegistry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	reg := makeFlowRegistry()

	id1 := FlowID{uuid.MakeV4()}
	f1 := &Flow{}

	id2 := FlowID{uuid.MakeV4()}
	f2 := &Flow{}

	id3 := FlowID{uuid.MakeV4()}
	f3 := &Flow{}

	id4 := FlowID{uuid.MakeV4()}
	f4 := &Flow{}

	// A basic duration; needs to be significantly larger than possible delays
	// in scheduling goroutines.
	jiffy := 10 * time.Millisecond

	// -- Lookup, register, lookup, unregister, lookup. --

	if f := lookupFlow(reg, id1, 0); f != nil {
		t.Error("looked up unregistered flow")
	}

	ctx := context.Background()
	reg.RegisterFlow(ctx, id1, f1, nil /* inboundStreams */, flowStreamDefaultTimeout)

	if f := lookupFlow(reg, id1, 0); f != f1 {
		t.Error("couldn't lookup previously registered flow")
	}

	reg.UnregisterFlow(id1)

	if f := lookupFlow(reg, id1, 0); f != nil {
		t.Error("looked up unregistered flow")
	}

	// -- Lookup with timeout, register in the meantime. --

	go func() {
		time.Sleep(jiffy)
		reg.RegisterFlow(ctx, id1, f1, nil /* inboundStreams */, flowStreamDefaultTimeout)
	}()

	if f := lookupFlow(reg, id1, 10*jiffy); f != f1 {
		t.Error("couldn't lookup registered flow (with wait)")
	}

	if f := lookupFlow(reg, id1, 0); f != f1 {
		t.Error("couldn't lookup registered flow")
	}

	// -- Multiple lookups before register. --

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		if f := lookupFlow(reg, id2, 10*jiffy); f != f2 {
			t.Error("couldn't lookup registered flow (with wait)")
		}
		wg.Done()
	}()

	go func() {
		if f := lookupFlow(reg, id2, 10*jiffy); f != f2 {
			t.Error("couldn't lookup registered flow (with wait)")
		}
		wg.Done()
	}()

	time.Sleep(jiffy)
	reg.RegisterFlow(ctx, id2, f2, nil /* inboundStreams */, flowStreamDefaultTimeout)
	wg.Wait()

	// -- Multiple lookups, with the first one failing. --

	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup

	wg1.Add(1)
	wg2.Add(1)
	go func() {
		if f := lookupFlow(reg, id3, jiffy); f != nil {
			t.Error("expected lookup to fail")
		}
		wg1.Done()
	}()

	go func() {
		if f := lookupFlow(reg, id3, 10*jiffy); f != f3 {
			t.Error("couldn't lookup registered flow (with wait)")
		}
		wg2.Done()
	}()

	wg1.Wait()
	reg.RegisterFlow(ctx, id3, f3, nil /* inboundStreams */, flowStreamDefaultTimeout)
	wg2.Wait()

	// -- Lookup with huge timeout, register in the meantime. --

	go func() {
		time.Sleep(jiffy)
		reg.RegisterFlow(ctx, id4, f4, nil /* inboundStreams */, flowStreamDefaultTimeout)
	}()

	// This should return in a jiffy.
	if f := lookupFlow(reg, id4, time.Hour); f != f4 {
		t.Error("couldn't lookup registered flow (with wait)")
	}
}

// Test that, if inbound streams are not connected within the timeout, errors
// are propagated to their consumers and future attempts to connect them fail.
func TestStreamConnectionTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	reg := makeFlowRegistry()

	jiffy := time.Nanosecond

	// Register a flow with a very low timeout. After it times out, we'll attempt
	// to connect a stream, but it'll be too late.
	id1 := FlowID{uuid.MakeV4()}
	f1 := &Flow{}
	streamID1 := StreamID(1)
	consumer := &RowBuffer{}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	inboundStreams := map[StreamID]*inboundStreamInfo{
		streamID1: {receiver: consumer, waitGroup: wg},
	}
	reg.RegisterFlow(context.TODO(), id1, f1, inboundStreams, jiffy)

	testutils.SucceedsSoon(t, func() error {
		si, err := lookupStreamInfo(reg, id1, streamID1)
		if err != nil {
			t.Fatal(err)
		}
		if !si.timedOut {
			return errors.Errorf("not timed out yet")
		}
		return nil
	})

	if !consumer.ProducerClosed {
		t.Fatalf("expected consumer to have been closed when the flow timed out")
	}

	if _, _, _, err := reg.ConnectInboundStream(id1, streamID1, jiffy); !testutils.IsError(
		err, "came too late") {
		t.Fatalf("expected %q, got: %v", "came too late", err)
	}

	// Unregister the flow. Subsequent attempts to connect a stream should result
	// in a different error than before.
	reg.UnregisterFlow(id1)
	_, _, _, err := reg.ConnectInboundStream(id1, streamID1, jiffy)
	if !testutils.IsError(err, "not found") {
		t.Fatalf("expected %q, got: %v", "not found", err)
	}
}
