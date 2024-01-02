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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/transform"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/validate"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestProducerGroup_Consume(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tests := []struct {
		name        string
		transformer *TestTransformer
		validators  []validate.Validator[string]
		producers   []EventProducer[string]
		wantErr     bool
		errMsg      string
	}{
		{
			name: "consumes event",
			transformer: &TestTransformer{
				toReturn: "data",
			},
			validators: []validate.Validator[string]{&TestValidator{}, &TestValidator{}}, // Empty TestValidators will succeed
			producers:  []EventProducer[string]{&TestProducer{}, &TestProducer{}},        // Empty TestProducers will succeed
		},
		{
			name: "returns transform error",
			transformer: &TestTransformer{
				retErr: errors.New("transform error"),
			},
			validators: []validate.Validator[string]{&TestValidator{}},
			producers:  []EventProducer[string]{&TestProducer{}},
			wantErr:    true,
			errMsg:     "transform error",
		},
		{
			name: "returns validation errors",
			transformer: &TestTransformer{
				toReturn: "data",
			},
			validators: []validate.Validator[string]{
				&TestValidator{},
				&TestValidator{retErr: errors.New("validation error")},
			},
			producers: []EventProducer[string]{&TestProducer{}},
			wantErr:   true,
			errMsg:    "validation error",
		},
		{
			name: "returns producer errors",
			transformer: &TestTransformer{
				toReturn: "data",
			},
			validators: []validate.Validator[string]{
				&TestValidator{},
			},
			producers: []EventProducer[string]{
				&TestProducer{},
				&TestProducer{
					retErr: errors.New("producer error"),
				},
			},
			wantErr: true,
			errMsg:  "producer error",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pg, err := NewProducerGroup[string](
				"test-alias", // alias
				"test-team",  // ownerTeam
				tc.transformer,
				tc.validators,
				tc.producers)
			require.NoError(t, err)
			testEvent := &obspb.Event{}
			err = pg.Consume(ctx, testEvent)
			if tc.wantErr {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.errMsg)
				return
			}
			require.NoError(t, err)
			require.Truef(t, tc.transformer.called, "transformer not called")
			for _, validator := range tc.validators {
				require.Truef(t, validator.(*TestValidator).called, "validator not called")
			}
			for _, producer := range tc.producers {
				require.Truef(t, producer.(*TestProducer).called, "producer not called")
			}
		})
	}
}

type TestProducer struct {
	retErr error
	called bool
}

func (t *TestProducer) Produce(_ string) error {
	t.called = true
	return t.retErr
}

var _ EventProducer[string] = (*TestProducer)(nil)

type TestValidator struct {
	retErr error
	called bool
}

var _ validate.Validator[string] = (*TestValidator)(nil)

func (t *TestValidator) Validate(_ string) error {
	t.called = true
	return t.retErr
}

type TestTransformer struct {
	toReturn string
	retErr   error
	called   bool
}

var _ transform.EventTransformer[string] = (*TestTransformer)(nil)

func (t *TestTransformer) Transform(_ *obspb.Event) (string, error) {
	t.called = true
	if t.toReturn != "" {
		return t.toReturn, nil
	}
	return "", t.retErr
}
