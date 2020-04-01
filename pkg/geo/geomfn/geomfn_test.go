// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geomfn

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/stretchr/testify/assert"
)

func TestParseGeometry(t *testing.T) {
	testCases := []struct {
		wkt         geo.WKT
		expected    *geo.Geometry
		expectedErr bool
	}{
		{
			"POINT(1.0 1.0)",
			geo.NewGeometry(geo.EWKB([]byte{0x1, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0x3f, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0x3f})),
			false,
		},
		{
			"invalid",
			nil,
			true,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s", tc.wkt), func(t *testing.T) {
			g, err := ParseGeometry(tc.wkt)
			if tc.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, g)
			}
		})
	}
}
