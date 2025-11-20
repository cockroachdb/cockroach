// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package env

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/config/types"
	"github.com/iancoleman/strcase"
)

// DiscoverAllSliceIndicesFromEnv discovers all slice indices being used in environment variables
func DiscoverAllSliceIndicesFromEnv() map[string][]int {
	indicesBySlice := make(map[string][]int)

	// Get all slice field names
	sliceFields := types.GetSliceFieldNames()

	for _, sliceEnvName := range sliceFields {
		// Convert back to field name
		sliceFieldName := strcase.ToCamel(strings.ToLower(sliceEnvName))

		// Discover indices for this slice
		indices := DiscoverSliceIndicesFromEnv(sliceFieldName)
		if len(indices) > 0 {
			indicesBySlice[sliceFieldName] = indices
		}
	}

	return indicesBySlice
}

// CollectSliceOverridesFromEnv extracts slice overrides from environment variables
// This function dynamically discovers slice fields from the Config struct
func CollectSliceOverridesFromEnv() ([]types.SliceOverride, error) {
	var overrides []types.SliceOverride

	// Get all slice field names from Config struct using reflection
	sliceFields := types.GetSliceFieldNames()

	// Build regex pattern dynamically based on discovered slice fields
	if len(sliceFields) == 0 {
		return overrides, nil
	}

	// Create pattern: ROACHPROD_(CLOUDPROVIDERS|DNSPROVIDERS|...)_(\d+)_(.+)
	slicePattern := strings.Join(sliceFields, "|")
	envRegex := regexp.MustCompile(fmt.Sprintf(`^%s_(%s)_(\d+)_(.+)$`, types.EnvPrefix, slicePattern))

	for _, env := range os.Environ() {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key, value := parts[0], parts[1]
		matches := envRegex.FindStringSubmatch(key)
		if len(matches) == 4 {
			sliceField := types.EnvNameToSliceField(matches[1])
			index, err := strconv.Atoi(matches[2])
			if err != nil {
				continue // Skip invalid index
			}
			fieldPath := types.EnvNameToFieldPath(matches[3])

			override := types.SliceOverride{
				SliceField: sliceField,
				Index:      index,
				FieldPath:  fieldPath,
				Value:      value,
				Source:     "env",
			}
			overrides = append(overrides, override)
		}
	}

	return overrides, nil
}

// DiscoverSliceIndicesFromEnv scans environment variables to find slice indices
func DiscoverSliceIndicesFromEnv(sliceName string) []int {
	var indices []int
	envPrefix := fmt.Sprintf("%s_%s_", types.EnvPrefix, strings.ToUpper(strcase.ToSnake(sliceName)))

	for _, env := range os.Environ() {
		if strings.HasPrefix(env, envPrefix) {
			parts := strings.Split(env, "=")
			if len(parts) >= 1 {
				key := parts[0]
				// Extract index from ROACHPROD_CLOUDPROVIDERS_2_TYPE
				afterPrefix := strings.TrimPrefix(key, envPrefix)
				indexStr := strings.Split(afterPrefix, "_")[0]
				if index, err := strconv.Atoi(indexStr); err == nil {
					indices = append(indices, index)
				}
			}
		}
	}

	// Remove duplicates and sort
	uniqueIndices := make(map[int]bool)
	for _, index := range indices {
		uniqueIndices[index] = true
	}

	result := make([]int, 0, len(uniqueIndices))
	for index := range uniqueIndices {
		result = append(result, index)
	}
	sort.Ints(result)

	return result
}
