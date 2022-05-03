// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geoprojbase_test

import (
	"strconv"
	"testing"

	_ "github.com/cockroachdb/cockroach/pkg/geo/geographiclib"
	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
	"github.com/stretchr/testify/require"
)

func TestProjections(t *testing.T) {
	for _, proj := range geoprojbase.AllProjections() {
		t.Run(strconv.Itoa(int(proj.SRID)), func(t *testing.T) {
			require.NotEqual(t, geoprojbase.Bounds{}, proj.Bounds)
			require.GreaterOrEqual(t, proj.Bounds.MaxX, proj.Bounds.MinX)
			require.GreaterOrEqual(t, proj.Bounds.MaxY, proj.Bounds.MinY)
		})
	}
}
