// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdctest

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"gocloud.dev/pubsub"
)

type messageStruct struct {
	Message string
	Err     error
}

// MockPubsubSink is the Webhook sink used in tests.
type MockPubsubSink struct {
	sub         *pubsub.Subscription
	topic       *pubsub.Topic
	ctx         context.Context
	groupCtx    ctxgroup.Group
	MessageChan chan messageStruct
	url         string
	shutdown    func()
}

// MakeMockPubsubSink returns a MockPubsubSink object initialized with the given url and context
func MakeMockPubsubSink(url string) (*MockPubsubSink, error) {
	ctx := context.Background()
	ctx, shutdown := context.WithCancel(ctx)
	groupCtx := ctxgroup.WithContext(ctx)
	p := &MockPubsubSink{
		ctx: ctx, MessageChan: make(chan messageStruct, 1), url: url, shutdown: shutdown,
		groupCtx: groupCtx,
	}
	return p, nil
}

// Close shuts down the subscriber object and closes the channels used
func (p *MockPubsubSink) Close() {
	if p.sub != nil {
		_ = p.sub.Shutdown(p.ctx)
	}
	p.shutdown()
	_ = p.groupCtx.Wait()
	close(p.MessageChan)
}

// Dial opens a subscriber using the url of the MockPubsubSink
func (p *MockPubsubSink) Dial() error {
	topic, err := pubsub.OpenTopic(p.ctx, p.url)
	if err != nil {
		return err
	}
	p.topic = topic
	sub, err := pubsub.OpenSubscription(p.ctx, p.url)
	if err != nil {
		return err
	}
	p.sub = sub
	p.groupCtx.GoCtx(func(ctx context.Context) error {
		p.receive()
		return nil
	})
	return nil
}

// receive loops to read in messages
func (p *MockPubsubSink) receive() {
	for {
		msg, err := p.sub.Receive(p.ctx)
		m := messageStruct{}
		if err != nil {
			m.Err = err

		} else {
			msg.Ack()
			m.Message = string(msg.Body)
		}
		select {
		case <-p.ctx.Done():
			return
		case p.MessageChan <- m:
		}
	}
}
