// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package operations

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTimeBasedValueGenerator(t *testing.T) {
	// Define a time supplier that steps by 10 minutes
	curTime := time.Date(2022, 1, 1, 0, 20, 0, 0, time.UTC)
	timeSupplier := func() time.Time {
		defer func() {
			curTime = curTime.Add(10 * time.Minute)
		}()
		return curTime
	}

	// Define the values and frequency for the test
	values := []string{"value1", "value2", "value3"}
	frequency := 15 * time.Minute

	// Create the value generator
	valueGenerator := timeBasedValues(timeSupplier, values, frequency)
	// Call the value generator and check the result, one value should be generated
	// twice and the other once, since the frequency is 15 minutes and the time
	// supplier steps by 10 minutes.
	counts := map[string]int{}
	for i := 0; i < 3; i++ {
		value := valueGenerator()
		counts[value]++
	}
	expectedCounts := map[string]int{
		"value2": 1,
		"value3": 2,
	}
	require.Equal(t, expectedCounts, counts)
}

func TestTimeBasedRandomValueGenerator(t *testing.T) {
	// Define a time supplier that steps by 10 minutes
	curTime := time.Date(2022, 1, 1, 0, 20, 0, 0, time.UTC)
	timeSupplier := func() time.Time {
		defer func() {
			curTime = curTime.Add(10 * time.Minute)
		}()
		return curTime
	}

	// Create the value generator
	valueGenerator := timeBasedRandomValue(timeSupplier, 30*time.Minute, func(rng *rand.Rand) string {
		return fmt.Sprintf("%d", rng.Intn(246)+5)
	})
	// Call the value generator and check the result, we expect the same value to
	// be generated a few times since the call frequency is higher than the random
	// value generation frequency.
	counts := map[string]int{}
	for i := 0; i < 10; i++ {
		value := valueGenerator()
		counts[value]++
	}
	expectedCounts := map[string]int{
		"108": 3, "178": 3, "43": 1, "58": 3,
	}
	require.Equal(t, expectedCounts, counts)
}
