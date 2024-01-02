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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/validate"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestProcessorGroup_Process(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tests := []struct {
		name       string
		validators []validate.Validator[string]
		processors []EventProcessor[string]
		wantErr    bool
		errMsg     string
	}{
		{
			name:       "processes event",
			validators: []validate.Validator[string]{&TestValidator{}, &TestValidator{}}, // Empty TestValidators will succeed
			processors: []EventProcessor[string]{&TestProcessor{}, &TestProcessor{}},     // Empty TestProcessor will succeed
		},
		{
			name: "returns validation error",
			validators: []validate.Validator[string]{
				&TestValidator{},
				&TestValidator{retErr: errors.New("validation error")},
			},
			processors: []EventProcessor[string]{&TestProcessor{}, &TestProcessor{}},
			wantErr:    true,
			errMsg:     "validation error",
		},
		{
			name:       "returns processor error",
			validators: []validate.Validator[string]{&TestValidator{}, &TestValidator{}},
			processors: []EventProcessor[string]{&TestProcessor{}, &TestProcessor{retErr: errors.New("processor error")}},
			wantErr:    true,
			errMsg:     "processor error",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pg, err := NewProcessorGroup[string](
				"test-alias",
				"test-team",
				tc.validators,
				tc.processors)
			require.NoError(t, err)
			testEvent := "some string"
			err = pg.Process(ctx, testEvent)
			if tc.wantErr {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.errMsg)
				return
			}
			require.NoError(t, err)
			for _, v := range tc.validators {
				require.Truef(t, v.(*TestValidator).called, "validator not called")
			}
			for _, p := range tc.processors {
				require.Truef(t, p.(*TestProcessor).called, "processor not called")
			}
		})
	}
}

type TestValidator struct {
	retErr error
	called bool
}

var _ validate.Validator[string] = (*TestValidator)(nil)

func (t *TestValidator) Validate(_ string) error {
	t.called = true
	return t.retErr
}

type TestProcessor struct {
	retErr error
	called bool
}

func (t *TestProcessor) Process(_ context.Context, _ string) error {
	t.called = true
	return t.retErr
}

var _ EventProcessor[string] = (*TestProcessor)(nil)
