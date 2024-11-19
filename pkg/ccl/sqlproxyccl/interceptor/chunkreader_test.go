// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package interceptor

import (
	"io"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/testutilsccl"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestChunkReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	cr := newChunkReader([]byte("foo bar baz hello world"))

	buf, err := cr.Next(11)
	require.NoError(t, err)
	require.Equal(t, "foo bar baz", string(buf))

	buf, err = cr.Next(1)
	require.NoError(t, err)
	require.Equal(t, " ", string(buf))

	buf, err = cr.Next(12)
	require.EqualError(t, err, errInvalidRead.Error())
	require.Nil(t, buf)

	// Attempt n = 0 before EOF.
	buf, err = cr.Next(0)
	require.NoError(t, err)
	require.Len(t, buf, 0)

	buf, err = cr.Next(11)
	require.NoError(t, err)
	require.Equal(t, "hello world", string(buf))

	buf, err = cr.Next(1)
	require.EqualError(t, err, io.EOF.Error())
	require.Nil(t, buf)

	// Attempting n = 0 after EOF returns nothing instead of an error.
	buf, err = cr.Next(0)
	require.NoError(t, err)
	require.Len(t, buf, 0)
}
