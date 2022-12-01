// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package testingint

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestTestingInt64(t *testing.T) {
	m := RealTestingInt64(123)
	require.Equal(t, 1, m.Size())

	buf1 := make([]byte, m.Size())
	{
		n, err := protoutil.MarshalTo(&m, buf1)
		require.NoError(t, err)
		require.Equal(t, m.Size(), n)
	}

	buf2 := make([]byte, m.Size())
	{
		n, err := protoutil.MarshalToSizedBuffer(&m, buf2)
		require.NoError(t, err)
		require.Equal(t, m.Size(), n)
	}

	var r1 RealTestingInt64
	require.NoError(t, protoutil.Unmarshal(buf1, &r1))
	require.EqualValues(t, r1, 123)

	var r2 RealTestingInt64

	require.NoError(t, protoutil.Unmarshal(buf2, &r2))
	require.EqualValues(t, r2, 123)
}
