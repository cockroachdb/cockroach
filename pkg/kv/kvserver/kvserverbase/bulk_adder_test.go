// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserverbase

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestDuplicateKeyErrorEncodeDecode verifies that DuplicateKeyError survives
// an encode/decode round-trip through the cockroachdb/errors library. This is
// critical because errors flowing through DistSQL are serialized via
// errors.EncodeError and deserialized via errors.DecodeError; without
// registered encoders/decoders the error becomes an opaque leaf and
// errors.As can no longer match the original type.
func TestDuplicateKeyErrorEncodeDecode(t *testing.T) {
	ctx := context.Background()
	key := roachpb.Key("/Table/106/4/1/0")
	value := []byte("some-value-bytes")

	origErr := NewDuplicateKeyError(key, value)

	// Verify the original error works with errors.As.
	var extracted *DuplicateKeyError
	require.True(t, errors.As(origErr, &extracted))
	require.Equal(t, key, extracted.Key)
	require.Equal(t, value, extracted.Value)

	// Round-trip through encode/decode, simulating DistSQL serialization.
	encoded := errors.EncodeError(ctx, origErr)
	decoded := errors.DecodeError(ctx, encoded)

	// The decoded error must preserve the message.
	require.Equal(t, origErr.Error(), decoded.Error())

	// errors.As must still match DuplicateKeyError after decoding.
	var decodedDup *DuplicateKeyError
	require.True(t, errors.As(decoded, &decodedDup),
		"expected DuplicateKeyError after decode, got %T: %v", decoded, decoded)

	// The key and value must survive the round-trip.
	require.Equal(t, key, decodedDup.Key)
	require.Equal(t, value, decodedDup.Value)

	// errors.HasType must also work.
	require.True(t, errors.HasType(decoded, (*DuplicateKeyError)(nil)))
}
