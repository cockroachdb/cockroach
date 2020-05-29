// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geoproj

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewProjPJFromText(t *testing.T) {
	pjProj, err := NewProjPJFromText("+proj=longlat +datum=WGS84 +no_defs")
	require.NoError(t, err)
	require.True(t, pjProj.IsLatLng())
}
