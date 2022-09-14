// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
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
