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
// queue to await processing by the Obs. Platform.
//
// EventProducer implementations are expected to be thread-safe.
type EventProducer[T any] interface {
	Produce(T) error
}

// ProducerGroup is a modular, event-type-specific group of steps
// designed to take a routed obspb.Event through the necessary procedure
// to transform it into its event-specific type, validate that event,
// and enqueue it to await processing by the Obs. Platform. It can be
// provided with a Transformer, multiple Validators (called sequentially),
// and multiple Producers (called sequentially) at initialization.
//
// ProducerGroup is thread-safe.
//
// TODO(abarganier): T is okay as `any` for now, but eventually we might require that it can provide some basic
// metadata about the event. When such a need arises, change to an interface type that ensures the ability to
// fetch such metadata. Such a change would need to apply to transformers/validators/producers as well.
type ProducerGroup[T any] struct {
	alias       string
	ownerTeam   obslib.OwnerTeam
	logPrefix   string
	transformer transform.EventTransformer[T]
	validators  []validate.Validator[T]
	producers   []EventProducer[T]
}

var _ obslib.EventConsumer = (*ProducerGroup[any])(nil)

func NewProducerGroup[T any](
	alias string,
	ownerTeam obslib.OwnerTeam,
	transformer transform.EventTransformer[T],
	validators []validate.Validator[T],
	producers []EventProducer[T],
) (*ProducerGroup[T], error) {
	producerAlias := fmt.Sprintf("%s-producer", alias)
	if transformer == nil {
		return nil, errors.Newf("nil transformer provided to %s", producerAlias)
	}
	if len(validators) == 0 {
		return nil, errors.Newf("must provide at least one Validator to %s", producerAlias)
	}
	if len(producers) == 0 {
		return nil, errors.Newf("must provide at least one EventProducer to %s", producerAlias)
	}
	return &ProducerGroup[T]{
		alias:       producerAlias,
		ownerTeam:   ownerTeam,
		logPrefix:   fmt.Sprintf("[%s,%s]", ownerTeam, producerAlias),
		transformer: transformer,
		validators:  validators,
		producers:   producers,
	}, nil
}

// TODO(abarganier): histogram for ProducerGroup latency.
// TODO(abarganier): smarter logging strategy for errors, to avoid log spam.
func (p *ProducerGroup[T]) Consume(ctx context.Context, event *obspb.Event) error {
	// TODO(abarganier): event-agnostic metadata validation.
	transformed, err := p.transformer.Transform(event)
	if err != nil {
		// TODO(abarganier): Counter metrics for transformation errors.
		log.Errorf(ctx, "%s failed to transform, err = %v", p.logPrefix, err)
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
		log.Errorf(ctx, "%s event validation failed, err = %v", p.logPrefix, err)
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
		log.Errorf(ctx, "%s failed to produce event, err = %v", p.logPrefix, err)
		return produceErrors
	}
	return nil
}
