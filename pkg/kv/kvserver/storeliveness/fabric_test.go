// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storeliveness

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestSupportStateCombine(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		receiver SupportState
		arg      SupportState
		expected SupportState
	}{
		// StateUnknown as receiver
		{StateUnknown, StateUnknown, StateUnknown},
		{StateUnknown, StateSupporting, StateSupporting},
		{StateUnknown, StateNotSupporting, StateNotSupporting},
		// StateSupporting as receiver
		{StateSupporting, StateUnknown, StateSupporting},
		{StateSupporting, StateSupporting, StateSupporting},
		{StateSupporting, StateNotSupporting, StateNotSupporting},
		// StateNotSupporting as receiver
		{StateNotSupporting, StateUnknown, StateNotSupporting},
		{StateNotSupporting, StateSupporting, StateNotSupporting},
		{StateNotSupporting, StateNotSupporting, StateNotSupporting},
	}

	for _, tc := range testCases {
		tc.receiver.combine(tc.arg)
		require.Equal(t, tc.expected, tc.receiver)
	}
}
