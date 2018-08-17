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
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/pkg/errors"
)

type asyncProducerMock struct {
	inputCh     chan *sarama.ProducerMessage
	successesCh chan *sarama.ProducerMessage
	errorsCh    chan *sarama.ProducerError
}

func (p asyncProducerMock) Input() chan<- *sarama.ProducerMessage     { return p.inputCh }
func (p asyncProducerMock) Successes() <-chan *sarama.ProducerMessage { return p.successesCh }
func (p asyncProducerMock) Errors() <-chan *sarama.ProducerError      { return p.errorsCh }
func (p asyncProducerMock) AsyncClose()                               { panic(`unimplemented`) }
func (p asyncProducerMock) Close() error {
	close(p.inputCh)
	close(p.successesCh)
	close(p.errorsCh)
	return nil
}

func TestKafkaSink(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	p := asyncProducerMock{
		inputCh:     make(chan *sarama.ProducerMessage, 1),
		successesCh: make(chan *sarama.ProducerMessage, 1),
		errorsCh:    make(chan *sarama.ProducerError, 1),
	}
	sink := &kafkaSink{
		producer:   p,
		topicsSeen: make(map[string]struct{}),
	}
	sink.start()
	defer func() {
		if err := sink.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// No inflight
	if err := sink.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Timeout
	if err := sink.EmitRow(ctx, `t`, []byte(`1`), nil); err != nil {
		t.Fatal(err)
	}
	m1 := <-p.inputCh
	for i := 0; i < 2; i++ {
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Millisecond)
		defer cancel()
		if err := sink.Flush(timeoutCtx); !testutils.IsError(err, `context deadline exceeded`) {
			t.Fatalf(`expected "context deadline exceeded" error got: %+v`, err)
		}
	}
	go func() { p.successesCh <- m1 }()
	if err := sink.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Check no inflight again now that we've sent something
	if err := sink.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Mixed success and error.
	if err := sink.EmitRow(ctx, `t`, []byte(`2`), nil); err != nil {
		t.Fatal(err)
	}
	m2 := <-p.inputCh
	if err := sink.EmitRow(ctx, `t`, []byte(`3`), nil); err != nil {
		t.Fatal(err)
	}
	m3 := <-p.inputCh
	if err := sink.EmitRow(ctx, `t`, []byte(`4`), nil); err != nil {
		t.Fatal(err)
	}
	m4 := <-p.inputCh
	go func() { p.successesCh <- m2 }()
	go func() {
		p.errorsCh <- &sarama.ProducerError{
			Msg: m3,
			Err: errors.New("m3"),
		}
	}()
	go func() { p.successesCh <- m4 }()
	if err := sink.Flush(ctx); !testutils.IsError(err, `m3`) {
		t.Fatalf(`expected "m3" error got: %+v`, err)
	}

	// Check simple success again after error
	if err := sink.EmitRow(ctx, `t`, []byte(`5`), nil); err != nil {
		t.Fatal(err)
	}
	m5 := <-p.inputCh
	go func() { p.successesCh <- m5 }()
	if err := sink.Flush(ctx); err != nil {
		t.Fatal(err)
	}
}
