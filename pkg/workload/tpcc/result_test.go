// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tpcc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewResult(t *testing.T) {
	// Ensure you don't get panics when calling common methods
	// on a trivial Result that doesn't have any data attached.
	res := NewResult(1000, 0, 0, nil)
	require.Error(t, res.FailureError())
	require.Zero(t, res.Efficiency())
	require.Zero(t, res.TpmC())
}
