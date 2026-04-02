// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package geos

import (
	"math"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestInitGEOS(t *testing.T) {
	t.Run("test no initGEOS paths", func(t *testing.T) {
		_, _, err := initGEOS([]string{})
		require.Error(t, err)
		require.Regexp(t, "Ensure you have the spatial libraries installed as per the instructions in .*install-cockroachdb-", strings.Join(errors.GetAllHints(err), "\n"))
	})

	t.Run("test invalid initGEOS paths", func(t *testing.T) {
		_, _, err := initGEOS([]string{"/invalid/path"})
		require.Error(t, err)
		require.Regexp(t, "Ensure you have the spatial libraries installed as per the instructions in .*install-cockroachdb-", strings.Join(errors.GetAllHints(err), "\n"))
	})

	t.Run("test valid initGEOS paths", func(t *testing.T) {
		ret, loc, err := initGEOS(findLibraryDirectories("", ""))
		require.NoError(t, err)
		require.NotEmpty(t, loc)
		require.NotNil(t, ret)
	})
}

func TestEnsureInit(t *testing.T) {
	// Fetch at least once.
	_, err := ensureInit(EnsureInitErrorDisplayPublic, "", "")
	require.NoError(t, err)

	fakeErr := errors.Newf("contain path info do not display me")
	defer func() { geosOnce.err = nil }()

	geosOnce.err = fakeErr
	_, err = ensureInit(EnsureInitErrorDisplayPrivate, "", "")
	require.Contains(t, err.Error(), fakeErr.Error())

	_, err = ensureInit(EnsureInitErrorDisplayPublic, "", "")
	require.Equal(t, errors.Newf("geos: this operation is not available").Error(), err.Error())
}

// TestClipByRectNaNInf is a regression test for issue #166976.
// GEOS hangs when ClipByRect is called with NaN or Inf bounding box
// coordinates.
func TestClipByRectNaNInf(t *testing.T) {
	ewkb := []byte("\x01\x02\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00$@")

	t.Run("rejects NaN coordinates", func(t *testing.T) {
		_, err := ClipByRect(ewkb, math.NaN(), 0, 5, 5)
		require.Error(t, err)
		require.Contains(t, err.Error(), "value out of range: overflow")

		_, err = ClipByRect(ewkb, 0, math.NaN(), 5, 5)
		require.Error(t, err)
		require.Contains(t, err.Error(), "value out of range: overflow")

		_, err = ClipByRect(ewkb, 0, 0, math.NaN(), 5)
		require.Error(t, err)
		require.Contains(t, err.Error(), "value out of range: overflow")

		_, err = ClipByRect(ewkb, 0, 0, 5, math.NaN())
		require.Error(t, err)
		require.Contains(t, err.Error(), "value out of range: overflow")
	})

	t.Run("rejects Inf coordinates", func(t *testing.T) {
		_, err := ClipByRect(ewkb, math.Inf(1), 0, 5, 5)
		require.Error(t, err)
		require.Contains(t, err.Error(), "value out of range: overflow")

		_, err = ClipByRect(ewkb, 0, math.Inf(-1), 5, 5)
		require.Error(t, err)
		require.Contains(t, err.Error(), "value out of range: overflow")

		_, err = ClipByRect(ewkb, 0, 0, math.Inf(1), 5)
		require.Error(t, err)
		require.Contains(t, err.Error(), "value out of range: overflow")

		_, err = ClipByRect(ewkb, 0, 0, 5, math.Inf(-1))
		require.Error(t, err)
		require.Contains(t, err.Error(), "value out of range: overflow")
	})
}
