// Copyright 2023 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package process

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/validate"
	"github.com/cockroachdb/errors"
)

// EventProcessor is the interface that defines a component capable of
// processing individual events of type T. In an event processing
// pipeline, once events have been ingested, transformed, and enqueued,
// they are ready for processing by a EventProcessor. A processor might be
// responsible for aggregating events, writing events to storage, etc.
//
// Processors are expected to be thread-safe.
type EventProcessor[T any] interface {
	Process(ctx context.Context, event T) error
}

// ProcessorGroup is a modular, event-type-specific group of steps
// designed to process type-specific events. Processing could involve
// aggregation, writing events to storage, etc. The ProcessorGroup is
// not concerned with the individual functions of each Processor, but
// instead groups together common functionality involved with processing
// enqueued events.
//
// It can be provided with multiple Validators (called sequentially) and
// multiple EventProcessor's (called sequentially) at initialization.
//
// ProcessorGroup is thread-safe.
//
// TODO(abarganier): T is okay as `any` for now, but eventually we might require that it can provide some basic
// metadata about the event. When such a need arises, change to an interface type that ensures the ability to
// fetch such metadata. Such a change would need to apply to transformers/validators/producers as well.
type ProcessorGroup[T any] struct {
	alias      string
	ownerTeam  obslib.OwnerTeam
	logPrefix  string
	validators []validate.Validator[T]
	processors []EventProcessor[T]
}

func NewProcessorGroup[T any](
	alias string,
	ownerTeam obslib.OwnerTeam,
	validators []validate.Validator[T],
	processors []EventProcessor[T],
) (*ProcessorGroup[T], error) {
	if len(validators) == 0 {
		return nil, errors.New("must provide at least one Validator")
	}
	if len(processors) == 0 {
		return nil, errors.New("must provide at least one EventProcessor")
	}
	processorAlias := fmt.Sprintf("%s-processor", alias)
	return &ProcessorGroup[T]{
		alias:      processorAlias,
		ownerTeam:  ownerTeam,
		logPrefix:  fmt.Sprintf("[%s,%s]", ownerTeam, processorAlias),
		validators: validators,
		processors: processors,
	}, nil
}

// TODO(abarganier): histogram for ProcessorGroup latency.
// TODO(abarganier): smarter logging strategy for errors, to avoid log spam.
func (p ProcessorGroup[T]) Process(ctx context.Context, event T) error {
	var validationErrors error
	for _, validator := range p.validators {
		if err := validator.Validate(event); err != nil {
			// TODO(abarganier): Counter metrics for validation errors.
			validationErrors = errors.CombineErrors(err, validationErrors)
		}
	}
	if validationErrors != nil {
		return errors.Wrapf(validationErrors, "%s event validation failed", p.logPrefix)
	}
	var processorErrors error
	for _, processor := range p.processors {
		if err := processor.Process(ctx, event); err != nil {
			// TODO(abarganier): Counter metrics for processor errors.
			processorErrors = errors.CombineErrors(err, processorErrors)
		}
	}
	if processorErrors != nil {
		return errors.Wrapf(processorErrors, "%s failed to process event", p.logPrefix)
	}
	return nil
}

var _ EventProcessor[any] = (*ProcessorGroup[any])(nil)
