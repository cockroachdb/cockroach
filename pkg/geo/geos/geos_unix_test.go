// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !windows

package geos

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInitCockroachGEOSLib(t *testing.T) {
	t.Run("test invalid initCRGEOS paths", func(t *testing.T) {
		_, err := initCRGEOS([]string{"/invalid/path"})
		require.Error(t, err)
	})

	t.Run("test valid initCRGEOS paths", func(t *testing.T) {
		ret, err := initCRGEOS(defaultGEOSLocations)
		require.NoError(t, err)
		require.NotNil(t, ret)
	})
}

func TestFetchGEOSOrError(t *testing.T) {
	// Fetch at least once.
	_, err := FetchGEOSOrError(FetchGEOSOrErrorDisplayPublic)
	require.NoError(t, err)

	fakeErr := fmt.Errorf("contain path info do not display me")
	defer func() { crGEOS.err = nil }()

	crGEOS.err = fakeErr
	_, err = FetchGEOSOrError(FetchGEOSOrErrorDisplayPrivate)
	require.Equal(t, fakeErr, err)

	_, err = FetchGEOSOrError(FetchGEOSOrErrorDisplayPublic)
	require.Equal(t, fmt.Errorf("geos: this operation is not available"), err)
}
