// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package state

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestReplicaPlacement tests the ReplicaPlacement parser.
func TestReplicaPlacement(t *testing.T) {
	datadriven.Walk(t, datapathutils.TestDataPath(t, t.Name()), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			require.Equal(t, "parse", d.Cmd)
			rp := ParseReplicaPlacement(d.Input)
			rp.findReplicaPlacementForEveryStoreSet(1000)
			return rp.String()
		})
	})
}
