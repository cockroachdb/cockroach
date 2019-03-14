// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// Note that zonesAreEqual only tests equality on fields used by the optimizer.
func TestZonesAreEqual(t *testing.T) {
	defer leaktest.AfterTest(t)()

	zone1 := &config.ZoneConfig{}
	require.Equal(t, zone1, zone1)

	zone2 := &config.ZoneConfig{}
	require.Equal(t, zone1, zone2)

	constraints1 := []config.Constraints{
		{NumReplicas: 0, Constraints: []config.Constraint{
			{Type: config.Constraint_REQUIRED, Key: "k1", Value: "v1"},
		}},
	}
	zone3 := &config.ZoneConfig{Constraints: constraints1}
	zone4 := &config.ZoneConfig{Constraints: constraints1}
	require.Equal(t, zone3, zone4)

	zone5 := &config.ZoneConfig{Constraints: []config.Constraints{
		{NumReplicas: 3, Constraints: []config.Constraint{
			{Type: config.Constraint_REQUIRED, Key: "k1", Value: "v1"},
		}},
	}}
	require.NotEqual(t, zone4, zone5)

	constraints2 := []config.Constraints{
		{NumReplicas: 3, Constraints: []config.Constraint{
			{Type: config.Constraint_REQUIRED, Key: "k1", Value: "v1"},
		}},
		{NumReplicas: 1, Constraints: []config.Constraint{
			{Type: config.Constraint_REQUIRED, Key: "k2", Value: "v2"},
		}},
	}
	zone6 := &config.ZoneConfig{Constraints: constraints2}
	require.NotEqual(t, zone5, zone6)

	zone7 := &config.ZoneConfig{Constraints: []config.Constraints{
		{NumReplicas: 3, Constraints: []config.Constraint{
			{Type: config.Constraint_REQUIRED, Key: "k1", Value: "v1"},
		}},
		{NumReplicas: 1, Constraints: []config.Constraint{
			{Type: config.Constraint_REQUIRED, Key: "k3", Value: "v3"},
		}},
	}}
	require.NotEqual(t, zone6, zone7)

	zone8 := &config.ZoneConfig{Constraints: []config.Constraints{
		{NumReplicas: 3, Constraints: []config.Constraint{
			{Type: config.Constraint_REQUIRED, Key: "k1", Value: "v1"},
		}},
		{NumReplicas: 1, Constraints: []config.Constraint{
			{Type: config.Constraint_PROHIBITED, Key: "k2", Value: "v2"},
		}},
	}}
	require.NotEqual(t, zone6, zone8)

	subzones1 := []config.Subzone{{IndexID: 0, Config: *zone3}}
	zone9 := &config.ZoneConfig{Constraints: constraints1, Subzones: subzones1}
	require.NotEqual(t, zone3, zone9)

	zone10 := &config.ZoneConfig{Constraints: constraints1, Subzones: []config.Subzone{
		{IndexID: 1, Config: *zone3},
	}}
	require.NotEqual(t, zone9, zone10)

	zone11 := &config.ZoneConfig{Constraints: constraints1, Subzones: []config.Subzone{
		{IndexID: 1, PartitionName: "p1", Config: *zone3},
	}}
	require.NotEqual(t, zone10, zone11)

	zone12 := &config.ZoneConfig{Constraints: constraints1, Subzones: []config.Subzone{
		{IndexID: 1, Config: *zone3},
		{IndexID: 2, PartitionName: "p1", Config: *zone3},
	}}
	require.NotEqual(t, zone10, zone12)
}
