// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package embedding

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/stretchr/testify/require"
)

func TestGetEngineNotInitialized(t *testing.T) {
	resetForTesting()
	defer resetForTesting()

	eng, err := GetEngine()
	require.Nil(t, eng)
	require.Error(t, err)
	require.Equal(t, pgcode.ConfigFile, pgerror.GetPGCode(err))
	require.Contains(t, err.Error(), "not initialized")
}

func TestInitMissingPaths(t *testing.T) {
	resetForTesting()
	defer resetForTesting()

	err := Init("", "", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not available")

	// GetEngine should also return a user-facing error.
	eng, err := GetEngine()
	require.Nil(t, eng)
	require.Error(t, err)
	require.Equal(t, pgcode.ConfigFile, pgerror.GetPGCode(err))
}

func TestInitIdempotent(t *testing.T) {
	resetForTesting()
	defer resetForTesting()

	// First call sets the error.
	err1 := Init("", "", "")
	require.Error(t, err1)

	// Second call is a no-op (sync.Once). Returns nil because the
	// once.Do body doesn't run again.
	err2 := Init("/different", "/model.onnx", "/vocab.txt")
	require.NoError(t, err2)

	// The original error is still stored.
	eng, err := GetEngine()
	require.Nil(t, eng)
	require.Error(t, err)
}
