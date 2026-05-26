// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
