// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ibm

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProviderDefaultZones(t *testing.T) {
	t.Run("geo-distributed", func(t *testing.T) {
		// Call DefaultZones with geoDistributed=true
		zones := DefaultZones(true)

		// Verify that all zones are returned
		expectedZones := []string{}
		for _, regionZones := range defaultZones {
			expectedZones = append(expectedZones, regionZones...)
		}

		// Verify that we got the right number of zones
		require.Equal(t, len(expectedZones), len(zones), "Expected all zones to be returned")

		// Create maps to check zone presence easily
		expectedZonesMap := make(map[string]bool)
		for _, zone := range expectedZones {
			expectedZonesMap[zone] = true
		}

		actualZonesMap := make(map[string]bool)
		for _, zone := range zones {
			actualZonesMap[zone] = true
		}

		// Verify each expected zone is present
		for zone := range expectedZonesMap {
			assert.True(t, actualZonesMap[zone], "Expected zone %s to be present", zone)
		}

		// Verify no unexpected zones
		for zone := range actualZonesMap {
			assert.True(t, expectedZonesMap[zone], "Unexpected zone %s returned", zone)
		}
	})

	t.Run("non-geo-distributed", func(t *testing.T) {
		// Call DefaultZones with geoDistributed=false multiple times
		// to verify randomness behavior
		seenZones := make(map[string]bool)

		for i := 0; i < 100; i++ {
			zones := DefaultZones(false)

			// Verify that only one zone is returned
			require.Equal(t, 1, len(zones), "Expected exactly one zone to be returned")

			// Track which zones we've seen
			seenZones[zones[0]] = true

			// Verify the returned zone is valid
			found := false
			for _, regionZones := range defaultZones {
				for _, validZone := range regionZones {
					if zones[0] == validZone {
						found = true
						break
					}
				}
				if found {
					break
				}
			}
			assert.True(t, found, "Returned zone %s is not valid", zones[0])
		}

		// We should have seen multiple zones after 100 runs
		// This is a probabilistic test, but the chance of getting the same zone
		// 100 times in a row is extremely small
		assert.Greater(t, len(seenZones), 1, "Expected to see multiple zones due to randomness")
	})

	t.Run("empty-zones", func(t *testing.T) {
		// Save original defaultZones
		originalDefaultZones := defaultZones

		// Temporarily replace defaultZones with empty map
		defaultZones = map[string][]string{}
		defer func() {
			// Restore original defaultZones
			defaultZones = originalDefaultZones
		}()

		// For geo-distributed case
		zones := DefaultZones(true)
		assert.Empty(t, zones, "Expected empty zones list for empty defaultZones with geoDistributed=true")

		// For non-geo-distributed case - should not panic
		assert.NotPanics(t, func() {
			zones = DefaultZones(false)
		}, "DefaultZones with empty defaultZones should not panic")
		assert.Empty(t, zones, "Expected empty zones list for empty defaultZones with geoDistributed=false")
	})
}
