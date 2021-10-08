// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
