// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package flags

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/config/env"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/config/types"
	"github.com/iancoleman/strcase"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// createMissingSliceFlags creates flags for slice indices that are being used via environment variables
// but don't have corresponding flags yet (this enables unlimited slice indices)
func CreateMissingSliceFlags(flagSet *pflag.FlagSet) error {
	// Discover all slice indices that are being used via environment variables
	envIndices := env.DiscoverAllSliceIndicesFromEnv()

	// Get Config struct for reflection
	configType := reflect.TypeOf(types.Config{})

	// For each slice field, create missing flags for discovered indices
	for i := range configType.NumField() {
		field := configType.Field(i)

		// Skip non-slice fields
		if field.Type.Kind() != reflect.Slice {
			continue
		}

		sliceName := field.Name
		elementType := field.Type.Elem()

		// Get indices that need flags for this slice
		sliceIndices := envIndices[sliceName]
		if len(sliceIndices) == 0 {
			continue
		}

		// Create flags for each missing index
		for _, index := range sliceIndices {
			// Build flag name pattern for this slice and index
			sliceKebab := strcase.ToKebab(sliceName)
			baseFlag := fmt.Sprintf("%s-%d", sliceKebab, index)

			// Check if any flags for this index already exist
			flagExists := false
			flagSet.VisitAll(func(flag *pflag.Flag) {
				if strings.HasPrefix(flag.Name, baseFlag+"-") {
					flagExists = true
				}
			})

			// If no flags exist for this index, create them
			if !flagExists {
				err := CreateSliceFlagsForIndexGeneric(flagSet, sliceName, index, elementType, []string{}, []string{})
				if err != nil {
					return errors.Wrapf(err, "creating dynamic flags for %s[%d]", sliceName, index)
				}
			}
		}
	}

	return nil
}

// collectSliceOverridesFromFlags extracts slice overrides from command line flags
// This function dynamically discovers slice fields from the Config struct
func CollectSliceOverridesFromFlags(flagSet *pflag.FlagSet) ([]types.SliceOverride, error) {
	var overrides []types.SliceOverride

	// Get all slice field names and create kebab-case versions
	sliceFields := types.GetSliceFieldNames()
	if len(sliceFields) == 0 {
		return overrides, nil
	}

	// Convert to kebab-case for flag matching using original field names
	envToField := types.GetSliceFieldMapping()
	var kebabSliceFields []string
	for _, envName := range sliceFields {
		if fieldName, exists := envToField[envName]; exists {
			kebabSliceFields = append(kebabSliceFields, strcase.ToKebab(fieldName))
		}
	}

	// Build regex pattern dynamically: (cloud-providers|dns-providers|...)-(\d+)-(.+)
	slicePattern := strings.Join(kebabSliceFields, "|")
	sliceFlagRegex := regexp.MustCompile(fmt.Sprintf(`^(%s)-(\d+)-(.+)$`, slicePattern))

	flagSet.VisitAll(func(flag *pflag.Flag) {
		if flag.Changed {
			matches := sliceFlagRegex.FindStringSubmatch(flag.Name)
			if len(matches) == 4 {
				sliceField := types.FlagNameToSliceField(matches[1])
				index, err := strconv.Atoi(matches[2])
				if err != nil {
					return // Skip invalid index
				}
				fieldPath := types.FlagNameToFieldPath(matches[3])

				override := types.SliceOverride{
					SliceField: sliceField,
					Index:      index,
					FieldPath:  fieldPath,
					Value:      flag.Value.String(),
					Source:     "flag",
				}
				overrides = append(overrides, override)
			}
		}
	})

	return overrides, nil
}

// createSliceFlags creates flags for slice fields based on discovered indices - fully generic using reflection
func CreateSliceFlags(
	fs *pflag.FlagSet, field reflect.StructField, fieldPrefixes, flagEnvPrefixes []string,
) error {
	// Skip non-slice fields
	if field.Type.Kind() != reflect.Slice {
		return nil
	}

	sliceName := field.Name

	// Get the element type of the slice
	elementType := field.Type.Elem()

	// Create indices 0-3 for CLI usage (index 0 visible in help, 1-3 hidden)
	// Additional indices will be created dynamically based on environment variables
	indices := []int{0, 1, 2, 3}

	// Create flags for each discovered index using reflection
	for _, index := range indices {
		err := CreateSliceFlagsForIndexGeneric(fs, sliceName, index, elementType, fieldPrefixes, flagEnvPrefixes)
		if err != nil {
			return errors.Wrapf(err, "creating flags for %s[%d]", sliceName, index)
		}
	}

	return nil
}

// createSliceFlagsForIndexGeneric creates all flags for a specific slice index using reflection
func CreateSliceFlagsForIndexGeneric(
	fs *pflag.FlagSet,
	sliceName string,
	index int,
	elementType reflect.Type,
	fieldPrefixes, flagEnvPrefixes []string,
) error {
	// Convert slice name to kebab-case for flags
	sliceKebab := strcase.ToKebab(sliceName)
	baseFlag := fmt.Sprintf("%s-%d", sliceKebab, index)

	// Build prefixes for recursive flag creation
	slicePrefixes := append(fieldPrefixes, sliceName, fmt.Sprintf("%d", index))
	sliceFlagEnvPrefixes := append(flagEnvPrefixes, strcase.ToKebab(sliceName), fmt.Sprintf("%d", index))

	// Create a zero value of the element type for reflection
	elementValue := reflect.New(elementType).Elem()

	// Recursively create flags for all fields in the slice element using the same logic as normal structs
	return ProcessFieldsRecursiveForSliceElement(fs, nil, elementValue, slicePrefixes, sliceFlagEnvPrefixes, true, baseFlag)
}

// processFieldsRecursiveForSliceElement processes fields in a slice element, similar to processFieldsRecursive but for slice elements
func ProcessFieldsRecursiveForSliceElement(
	fs *pflag.FlagSet,
	e *viper.Viper,
	v reflect.Value,
	fieldPrefixes, flagEnvPrefixes []string,
	createFlags bool,
	baseFlag string,
) error {
	t := v.Type()

	for i := range t.NumField() {
		field := t.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		// Get the env tag and description
		envTag := field.Tag.Get("env")
		if envTag == "" {
			envTag = strcase.ToSnake(field.Name)
		}
		desc := field.Tag.Get("description")
		defaultVal := field.Tag.Get("default")

		// If this is a struct, recursively process its fields
		if field.Type.Kind() == reflect.Struct {
			// Create a nested baseFlag: cloud-providers-0-gce, cloud-providers-0-aws, etc.
			nestedBaseFlag := fmt.Sprintf("%s-%s", baseFlag, strcase.ToKebab(envTag))

			// Create a new zero value for the nested struct to ensure all fields are accessible
			nestedValue := reflect.New(field.Type).Elem()

			err := ProcessFieldsRecursiveForSliceElement(fs, e, nestedValue,
				append(fieldPrefixes, field.Name),
				append(flagEnvPrefixes, strcase.ToKebab(envTag)),
				createFlags,
				nestedBaseFlag,
			)
			if err != nil {
				return errors.Wrapf(err, "creating flags for struct field %s", field.Name)
			}
			continue
		}

		// Skip nested slices for now (could be extended later if needed)
		if field.Type.Kind() == reflect.Slice {
			continue
		}

		// Create the flag name: cloud-providers-0-type, cloud-providers-0-gce-project, etc.
		flagName := fmt.Sprintf("%s-%s", baseFlag, strcase.ToKebab(envTag))

		if createFlags {
			// Create flag based on the field type
			switch field.Type.Kind() {
			case reflect.String:
				defaultStr := defaultVal
				var dummyVar string
				fs.StringVar(&dummyVar, flagName, defaultStr, desc)
			case reflect.Int:
				defaultInt := 0
				if defaultVal != "" {
					if i, err := strconv.Atoi(defaultVal); err == nil {
						defaultInt = i
					}
				}
				var dummyVar int
				fs.IntVar(&dummyVar, flagName, defaultInt, desc)
			case reflect.Bool:
				defaultBool := false
				if defaultVal != "" {
					if b, err := strconv.ParseBool(defaultVal); err == nil {
						defaultBool = b
					}
				}
				var dummyVar bool
				fs.BoolVar(&dummyVar, flagName, defaultBool, desc)
			default:
				// For unsupported types, skip rather than error - allows for extensibility
				continue
			}

			// Hide flags for indices > 0 from help output while keeping them functional
			// Extract index from baseFlag (e.g., "cloud-providers-1-aws" -> index 1)
			parts := strings.Split(baseFlag, "-")
			if len(parts) >= 3 {
				// For nested flags: cloud-providers-0-aws, dns-providers-1-gce, etc.
				// The index is always the 3rd part (index 2 in 0-based array)
				if indexStr := parts[2]; indexStr != "0" {
					if flag := fs.Lookup(flagName); flag != nil {
						flag.Hidden = true
					}
				}
			}
		}
	}

	return nil
}
