// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeAddressMode(t *testing.T) {
	for _, tc := range []struct {
		input AddressMode
		want  AddressMode
		err   bool
	}{
		{input: "", want: AddressModePublic},
		{input: "AUTO", want: AddressModeAuto},
		{input: AddressModePublic, want: AddressModePublic},
		{input: AddressModePrivate, want: AddressModePrivate},
		{input: "other", err: true},
	} {
		got, err := NormalizeAddressMode(tc.input)
		if tc.err {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
	}
}
