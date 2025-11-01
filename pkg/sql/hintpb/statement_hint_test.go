// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hintpb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFromToBytes(t *testing.T) {
	// Test writing empty hint.
	_, err := ToBytes(StatementHintUnion{})
	require.EqualError(t, err, "cannot convert empty hint to bytes")

	// Test reading empty bytes.
	_, err = FromBytes(nil)
	require.EqualError(t, err, "invalid hint bytes: no value set")
	_, err = FromBytes([]byte{})
	require.EqualError(t, err, "invalid hint bytes: no value set")

	// Test reading invalid bytes.
	_, err = FromBytes([]byte{0xFF, 0xFF, 0xFF})
	require.Error(t, err)

	// Test that a valid hint round trips.
	testRT := func(hint interface{}) {
		var hintUnion StatementHintUnion
		hintUnion.SetValue(hint)
		bytes, err := ToBytes(hintUnion)
		require.NoError(t, err)
		require.NotEmpty(t, bytes)
		decodedHintUnion, err := FromBytes(bytes)
		require.NoError(t, err)
		require.Equal(t, hint, decodedHintUnion.GetValue())
	}
	testRT(&InjectHints{})
	testRT(&InjectHints{DonorSQL: "SELECT * FROM t"})
}
