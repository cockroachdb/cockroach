// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package vm

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestZonePlacement(t *testing.T) {
	for i, c := range []struct {
		numZones, numNodes int
		expected           []int
	}{
		{1, 1, []int{0}},
		{1, 2, []int{0, 0}},
		{2, 4, []int{0, 0, 1, 1}},
		{2, 5, []int{0, 0, 1, 1, 0}},
		{3, 11, []int{0, 0, 0, 1, 1, 1, 2, 2, 2, 0, 1}},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			assert.EqualValues(t, c.expected, ZonePlacement(c.numZones, c.numNodes))
		})
	}
}
