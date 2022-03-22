// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package envutil

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnvOrDefault(t *testing.T) {
	const def = 123
	os.Clearenv()
	// These tests are mostly an excuse to exercise otherwise unused code.
	// TODO(knz): Test everything.
	if act := EnvOrDefaultBytes("COCKROACH_X", def); act != def {
		t.Errorf("expected %d, got %d", def, act)
	}
	if act := EnvOrDefaultInt("COCKROACH_X", def); act != def {
		t.Errorf("expected %d, got %d", def, act)
	}
}

func TestTestSetEnvExists(t *testing.T) {
	key := "COCKROACH_ENVUTIL_TESTSETTING"
	require.NoError(t, os.Setenv(key, "before"))

	ClearEnvCache()
	value, ok := EnvString(key, 0)
	require.True(t, ok)
	require.Equal(t, value, "before")

	cleanup := TestSetEnv(t, key, "testvalue")
	value, ok = EnvString(key, 0)
	require.True(t, ok)
	require.Equal(t, value, "testvalue")

	cleanup()

	value, ok = EnvString(key, 0)
	require.True(t, ok)
	require.Equal(t, value, "before")
}

func TestTestSetEnvDoesNotExist(t *testing.T) {
	key := "COCKROACH_ENVUTIL_TESTSETTING"
	require.NoError(t, os.Unsetenv(key))

	ClearEnvCache()
	_, ok := EnvString(key, 0)
	require.False(t, ok)

	cleanup := TestSetEnv(t, key, "foo")
	value, ok := EnvString(key, 0)
	require.True(t, ok)
	require.Equal(t, value, "foo")

	cleanup()

	_, ok = EnvString(key, 0)
	require.False(t, ok)
}
