// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package strutil

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppendInt(t *testing.T) {
	for _, tc := range []struct {
		n     int
		width int
		fmt   string
	}{
		{0, 3, "%03d"},
		{5, 2, "%02d"},
		{10, 1, "%01d"},
		{-11, 4, "%04d"},
		{1234, 6, "%06d"},
		{-321, 2, "%02d"},
	} {
		require.Equal(t, fmt.Sprintf(tc.fmt, tc.n), string(AppendInt(nil, tc.n, tc.width)))
	}
}
