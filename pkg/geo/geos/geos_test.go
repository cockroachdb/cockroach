// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geos

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestInitGEOS(t *testing.T) {
	t.Run("test no initGEOS paths", func(t *testing.T) {
		_, _, err := initGEOS([]string{})
		require.Error(t, err)
	})

	t.Run("test invalid initGEOS paths", func(t *testing.T) {
		_, _, err := initGEOS([]string{"/invalid/path"})
		require.Error(t, err)
	})

	t.Run("test valid initGEOS paths", func(t *testing.T) {
		ret, loc, err := initGEOS(findGEOSLocations(""))
		require.NoError(t, err)
		require.NotEmpty(t, loc)
		require.NotNil(t, ret)
	})
}

func TestEnsureInit(t *testing.T) {
	// Fetch at least once.
	_, err := ensureInit(EnsureInitErrorDisplayPublic, "")
	require.NoError(t, err)

	fakeErr := errors.Newf("contain path info do not display me")
	defer func() { geosOnce.err = nil }()

	geosOnce.err = fakeErr
	_, err = ensureInit(EnsureInitErrorDisplayPrivate, "")
	require.Contains(t, err.Error(), fakeErr.Error())

	_, err = ensureInit(EnsureInitErrorDisplayPublic, "")
	require.Equal(t, errors.Newf("geos: this operation is not available").Error(), err.Error())
}
