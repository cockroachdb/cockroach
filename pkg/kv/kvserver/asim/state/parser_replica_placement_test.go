// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package state

import (
	"strings"
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
			// Make sure rp is immutable by calling
			// findReplicaPlacementForEveryStoreSet twice and comparing the results.
			result1 := rp.findReplicaPlacementForEveryStoreSet(1000)
			result2 := rp.findReplicaPlacementForEveryStoreSet(1000)
			require.Equal(t, result1, result2)
			var result []string
			for _, r := range result1 {
				result = append(result, r.String())
			}
			return strings.Join(result, "\n")
		})
	})
}
