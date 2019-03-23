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

	// Test replica constraint equality.
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
		{NumReplicas: 0, Constraints: []config.Constraint{
			{Type: config.Constraint_REQUIRED, Key: "k1", Value: "v1"},
			{Type: config.Constraint_REQUIRED, Key: "k2", Value: "v2"},
		}},
	}}
	require.NotEqual(t, zone3, zone8)

	zone9 := &config.ZoneConfig{Constraints: []config.Constraints{
		{NumReplicas: 0, Constraints: []config.Constraint{
			{Type: config.Constraint_REQUIRED, Key: "k1", Value: "v1"},
			{Type: config.Constraint_REQUIRED, Key: "k2", Value: "v3"},
		}},
	}}
	require.NotEqual(t, zone8, zone9)

	// Test subzone equality.
	subzones1 := []config.Subzone{{IndexID: 0, Config: *zone3}}
	zone20 := &config.ZoneConfig{Constraints: constraints1, Subzones: subzones1}
	require.NotEqual(t, zone3, zone20)

	zone21 := &config.ZoneConfig{Constraints: constraints1, Subzones: []config.Subzone{
		{IndexID: 1, Config: *zone3},
	}}
	require.NotEqual(t, zone20, zone21)

	zone22 := &config.ZoneConfig{Constraints: constraints1, Subzones: []config.Subzone{
		{IndexID: 1, PartitionName: "p1", Config: *zone3},
	}}
	require.NotEqual(t, zone21, zone22)

	zone23 := &config.ZoneConfig{Constraints: constraints1, Subzones: []config.Subzone{
		{IndexID: 1, Config: *zone3},
		{IndexID: 2, PartitionName: "p1", Config: *zone3},
	}}
	require.NotEqual(t, zone21, zone23)

	// Test leaseholder preference equality.
	leasePrefs1 := []config.LeasePreference{{Constraints: []config.Constraint{
		{Type: config.Constraint_REQUIRED, Key: "k1", Value: "v1"},
	}}}
	zone43 := &config.ZoneConfig{Constraints: constraints1, LeasePreferences: leasePrefs1}
	zone44 := &config.ZoneConfig{Constraints: constraints1, LeasePreferences: leasePrefs1}
	require.Equal(t, zone43, zone44)

	leasePrefs2 := []config.LeasePreference{{Constraints: []config.Constraint{
		{Type: config.Constraint_REQUIRED, Key: "k1", Value: "v1"},
		{Type: config.Constraint_REQUIRED, Key: "k2", Value: "v2"},
	}}}
	zone45 := &config.ZoneConfig{Constraints: constraints1, LeasePreferences: leasePrefs2}
	require.NotEqual(t, zone43, zone45)

	zone46 := &config.ZoneConfig{LeasePreferences: leasePrefs1}
	require.NotEqual(t, zone43, zone46)

	zone47 := &config.ZoneConfig{
		Constraints: constraints1,
		LeasePreferences: []config.LeasePreference{{Constraints: []config.Constraint{
			{Type: config.Constraint_PROHIBITED, Key: "k1", Value: "v1"},
		}}},
	}
	require.NotEqual(t, zone43, zone47)
}
