// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"sync"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/pkg/errors"
)

func TestKafkaSink(t *testing.T) {
	defer leaktest.AfterTest(t)()
	assertNotFinished := func(flushFinished chan error) {
		select {
		case <-flushFinished:
			t.Fatal(`flush should not have finished`)
		default:
		}
	}

	ctx := context.Background()
	inputCh := make(chan *sarama.ProducerMessage, 100)
	successesCh := make(chan *sarama.ProducerMessage, 100)
	errorsCh := make(chan *sarama.ProducerError, 100)
	sink := &kafkaSink{
		inputCh:     inputCh,
		successesCh: successesCh,
		errorsCh:    errorsCh,
		topicsSeen:  make(map[string]struct{}),
	}
	sink.mu.flushes = make(map[int64]*sync.WaitGroup)

	// Check two flushes at once and out of order message acknowledgement.
	if err := sink.EmitRow(ctx, `t`, []byte(`1`), nil); err != nil {
		t.Fatal(err)
	}
	m1 := <-inputCh
	if err := sink.EmitRow(ctx, `t`, []byte(`2`), nil); err != nil {
		t.Fatal(err)
	}
	m2 := <-inputCh
	flush1 := make(chan error)
	go func() {
		flush1 <- sink.Flush(ctx)
	}()
	flush2 := make(chan error)
	go func() {
		flush2 <- sink.Flush(ctx)
	}()
	assertNotFinished(flush1)
	assertNotFinished(flush2)
	successesCh <- m2
	assertNotFinished(flush1)
	assertNotFinished(flush2)
	successesCh <- m1
	if err := <-flush1; err != nil {
		t.Fatal(err)
	}
	if err := <-flush2; err != nil {
		t.Fatal(err)
	}

	// Check that errors also decrement a flush.
	if err := sink.EmitRow(ctx, `t`, []byte(`4`), nil); err != nil {
		t.Fatal(err)
	}
	m4 := <-inputCh
	if err := sink.EmitRow(ctx, `t`, []byte(`5`), nil); err != nil {
		t.Fatal(err)
	}
	m5 := <-inputCh
	flush3 := make(chan error)
	go func() {
		flush3 <- sink.Flush(ctx)
	}()
	flush4 := make(chan error)
	go func() {
		flush4 <- sink.Flush(ctx)
	}()
	errorsCh <- &sarama.ProducerError{
		Msg: m4,
		Err: errors.New(`m4`),
	}
	successesCh <- m5
	if err1, err2 := <-flush3, <-flush4; err1 == nil && err2 == nil || err1 != nil && err2 != nil {
		t.Errorf(`expected exactly one err to be non-nil: %v and %v`, err1, err2)
	}
}
