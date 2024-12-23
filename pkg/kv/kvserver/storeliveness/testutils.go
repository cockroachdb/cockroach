// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storeliveness

import (
	"context"
	"sync"

	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// TestEngine is a wrapper around storage.Engine that helps simulate failed and
// stalled writes.
type TestEngine struct {
	storage.Engine
	storeID      slpb.StoreIdent
	mu           syncutil.Mutex
	cond         sync.Cond
	blockOnWrite bool
	errorOnWrite bool
}

func NewTestEngine(storeID slpb.StoreIdent) *TestEngine {
	te := &TestEngine{
		Engine:  storage.NewDefaultInMemForTesting(),
		storeID: storeID,
	}
	te.cond.L = &te.mu
	return te
}

func (te *TestEngine) NewBatch() storage.Batch {
	return &TestBatch{
		Batch: te.Engine.NewBatch(),
		te:    te,
	}
}

func (te *TestEngine) SetBlockOnWrite(bow bool) {
	te.mu.Lock()
	defer te.mu.Unlock()
	te.blockOnWrite = bow
	te.cond.Broadcast()
}

func (te *TestEngine) SetErrorOnWrite(eow bool) {
	te.mu.Lock()
	defer te.mu.Unlock()
	te.errorOnWrite = eow
}

func (te *TestEngine) blockOrErrorOnWrite() error {
	te.mu.Lock()
	defer te.mu.Unlock()
	for te.blockOnWrite {
		te.cond.Wait()
	}
	if te.errorOnWrite {
		return errors.New("error on write")
	}
	return nil
}

func (te *TestEngine) PutUnversioned(key roachpb.Key, value []byte) error {
	if err := te.blockOrErrorOnWrite(); err != nil {
		return err
	}
	return te.Engine.PutUnversioned(key, value)
}

type TestBatch struct {
	storage.Batch
	te *TestEngine
}

func (tb *TestBatch) Commit(sync bool) error {
	if err := tb.te.blockOrErrorOnWrite(); err != nil {
		return err
	}
	return tb.Batch.Commit(sync)
}

// testMessageSender implements the MessageSender interface and stores all sent
// messages in a slice.
type testMessageSender struct {
	mu       syncutil.Mutex
	messages []slpb.Message
}

func (tms *testMessageSender) SendAsync(_ context.Context, msg slpb.Message) (sent bool) {
	tms.mu.Lock()
	defer tms.mu.Unlock()
	tms.messages = append(tms.messages, msg)
	return true
}

func (tms *testMessageSender) drainSentMessages() []slpb.Message {
	tms.mu.Lock()
	defer tms.mu.Unlock()
	msgs := tms.messages
	tms.messages = nil
	return msgs
}

func (tms *testMessageSender) getNumSentMessages() int {
	tms.mu.Lock()
	defer tms.mu.Unlock()
	return len(tms.messages)
}

var _ MessageSender = (*testMessageSender)(nil)

// UnreliableHandler allows users to selectively drop StoreLiveness messages.
type UnreliableHandler struct {
	Name string
	MessageHandler
	UnreliableHandlerFuncs
}

var _ MessageHandler = &UnreliableHandler{}

type UnreliableHandlerFuncs struct {
	DropStoreLivenessMsg func(*slpb.Message) bool
}

// HandleMessage implements the MessageHandler interface.
func (h *UnreliableHandler) HandleMessage(msg *slpb.Message) error {
	if h.DropStoreLivenessMsg != nil && h.DropStoreLivenessMsg(msg) {
		return nil
	}

	return h.MessageHandler.HandleMessage(msg)
}
