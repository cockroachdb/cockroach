// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sysutil_test

import (
	"io/ioutil"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcFDUsage(t *testing.T) {
	fdUsage := &sysutil.ProcFDUsage{}
	require.NoError(t, fdUsage.Get())

	beforeOpen := fdUsage.Open
	f, err := ioutil.TempFile(t.TempDir(), "test-open-1")
	require.NoError(t, err)
	defer f.Close()

	require.NoError(t, fdUsage.Get())
	assert.Equal(t, beforeOpen+1, fdUsage.Open, "opening file increase Open count")

	require.NoError(t, f.Close())
	require.NoError(t, fdUsage.Get())
	assert.Equal(t, beforeOpen, fdUsage.Open, "closing file decreases Open count")
}
