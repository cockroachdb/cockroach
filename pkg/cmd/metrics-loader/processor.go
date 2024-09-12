// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/prometheus/prompb"
)

const processorBatchSize = 4096
const processorChannelSize = 8192

type Processor interface {
	// Process reads the data from the reader and sends it to the sender.
	Process(reader io.Reader) error
	AddMutator(mutator LabelMutatorFn)
}

type LabelMutatorFn func(labels []prompb.Label) []prompb.Label

type processor struct {
	sender   Sender
	reader   Reader
	mutators []LabelMutatorFn
}

// NewProcessor creates a new Processor with a given Reader and Sender.
func NewProcessor(reader Reader, sender Sender) Processor {
	return &processor{
		sender: sender,
		reader: reader,
	}
}

// Process implements the Processor interface.
func (p *processor) Process(reader io.Reader) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan *prompb.TimeSeries, processorChannelSize)
	var readErr error
	go func() {
		defer close(ch)
		if err := p.reader.Read(reader, func(ts *prompb.TimeSeries) {
			select {
			case <-ctx.Done():
				return
			case ch <- ts:
			}
		}); err != nil {
			readErr = err
			cancel()
		}
	}()

	batch := make([]prompb.TimeSeries, 0, processorBatchSize)
	for ts := range ch {
		for _, mutator := range p.mutators {
			ts.Labels = mutator(ts.Labels)
		}
		batch = append(batch, *ts)
		if len(batch) == processorBatchSize {
			if err := p.sender.Send(batch); err != nil {
				cancel()
				return err
			}
			batch = make([]prompb.TimeSeries, 0, processorBatchSize)
		}
	}
	if len(batch) > 0 {
		if err := p.sender.Send(batch); err != nil {
			cancel()
			return errors.CombineErrors(err, readErr)
		}
	}
	return readErr
}

func (p *processor) AddMutator(mutator LabelMutatorFn) {
	p.mutators = append(p.mutators, mutator)
}
