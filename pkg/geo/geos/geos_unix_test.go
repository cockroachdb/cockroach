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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInitCockroachGEOSLib(t *testing.T) {
	t.Run("test invalid initCRGEOS paths", func(t *testing.T) {
		ret := initCRGEOS([]string{"/invalid/path"})
		assert.Error(t, validOrError(ret))
	})

	t.Run("test valid initCRGEOS paths", func(t *testing.T) {
		ret := initCRGEOS(defaultGEOSLocations)
		assert.NoError(t, validOrError(ret))
	})
}
