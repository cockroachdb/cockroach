// Copyright 2023 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package produce

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/transform"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/validate"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// EventProducer defines how to produce an event into an event-specific
// queue to await aggregation by the obsservice.
type EventProducer interface {
	Produce(interface{}) error
}

// ProducerGroup is a modular, event-type-specific group of steps
// designed to take a routed obspb.Event through the necessary procedure
// to transform it into its event-specific type, validate that event,
// and enqueue it to await aggregation by the obsservice. It can be
// provided with a Transformer, multiple Validators (called sequentially),
// and multiple Producers (called sequentially) at initialization.
type ProducerGroup struct {
	alias       string
	transformer transform.EventTransformer
	validators  []validate.Validator
	producers   []EventProducer
}

var _ obslib.EventConsumer = (*ProducerGroup)(nil)

func NewProducerGroup(
	alias string,
	transformer transform.EventTransformer,
	validators []validate.Validator,
	producers []EventProducer,
) (*ProducerGroup, error) {
	if transformer == nil {
		return nil, errors.New("nil transformer provided")
	}
	if validators == nil {
		return nil, errors.New("nil validators provided")
	}
	if producers == nil {
		return nil, errors.New("nil producers provided")
	}
	if len(producers) == 0 {
		return nil, errors.New("must provide at least one EventProducer")
	}
	return &ProducerGroup{
		alias:       fmt.Sprintf("%s-producer", alias),
		transformer: transformer,
		validators:  validators,
		producers:   producers,
	}, nil
}

// TODO(abarganier): histogram for ProducerGroup latency.
func (p *ProducerGroup) Consume(ctx context.Context, event *obspb.Event) error {
	transformed, err := p.transformer.Transform(event)
	if err != nil {
		// TODO(abarganier): Counter metrics for transformation errors.
		log.Errorf(ctx, "%s failed to transform event %v, err = %v",
			p.alias, event, err)
		return errors.Wrap(err, "transforming event")
	}
	var validationErrors error
	for _, validator := range p.validators {
		if err := validator.Validate(transformed); err != nil {
			// TODO(abarganier): Counter metrics for validation errors.
			validationErrors = errors.CombineErrors(err, validationErrors)
		}
	}
	if validationErrors != nil {
		log.Errorf(ctx, "%s validation failed for event %v, err = %v",
			p.alias, transformed, err)
		return validationErrors
	}
	var produceErrors error
	for _, producer := range p.producers {
		if err := producer.Produce(transformed); err != nil {
			// TODO(abarganier): Counter metrics for producer errors.
			produceErrors = errors.CombineErrors(err, produceErrors)
		}
	}
	if produceErrors != nil {
		log.Errorf(ctx, "%s failed to produce event %v, err = %v",
			p.alias, transformed, err)
		return produceErrors
	}
	return nil
}
