// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package processing

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/config/env"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/config/flags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/config/recursive"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/config/types"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// collectSliceOverrides scans flags and environment variables for slice overrides
func CollectSliceOverrides(flagSet *pflag.FlagSet) ([]types.SliceOverride, error) {
	var overrides []types.SliceOverride

	// Collect from command line flags
	flagOverrides, err := flags.CollectSliceOverridesFromFlags(flagSet)
	if err != nil {
		return nil, errors.Wrap(err, "failed to collect flag overrides")
	}
	overrides = append(overrides, flagOverrides...)

	// Collect from environment variables
	envOverrides, err := env.CollectSliceOverridesFromEnv()
	if err != nil {
		return nil, errors.Wrap(err, "failed to collect environment overrides")
	}
	overrides = append(overrides, envOverrides...)

	return overrides, nil
}

// applySliceOverrides applies overrides to any config slice using reflection
func ApplySliceOverrides(cfg *types.Config, overrides []types.SliceOverride) error {
	// Group overrides by slice field
	overridesBySlice := make(map[string][]types.SliceOverride)
	for _, override := range overrides {
		overridesBySlice[override.SliceField] = append(overridesBySlice[override.SliceField], override)
	}

	// Apply overrides for each slice field
	for sliceFieldName, sliceOverrides := range overridesBySlice {
		err := ApplySliceFieldOverrides(cfg, sliceFieldName, sliceOverrides)
		if err != nil {
			return errors.Wrapf(err, "failed to apply overrides to %s", sliceFieldName)
		}
	}

	return nil
}

// applySliceFieldOverrides applies overrides to a specific slice field using reflection
func ApplySliceFieldOverrides(
	cfg *types.Config, sliceFieldName string, overrides []types.SliceOverride,
) error {
	// Get the slice field from config using reflection
	configValue := reflect.ValueOf(cfg).Elem()
	sliceField := configValue.FieldByName(sliceFieldName)
	if !sliceField.IsValid() {
		return fmt.Errorf("invalid slice field: %s", sliceFieldName)
	}

	if sliceField.Kind() != reflect.Slice {
		return fmt.Errorf("field %s is not a slice", sliceFieldName)
	}

	// Find the maximum index needed
	maxIndex := sliceField.Len() - 1
	for _, override := range overrides {
		if override.Index > maxIndex {
			maxIndex = override.Index
		}
	}

	// Extend slice if needed
	if maxIndex >= 0 {
		// Create new slice with required length
		sliceType := sliceField.Type()
		elementType := sliceType.Elem()
		newSlice := reflect.MakeSlice(sliceType, maxIndex+1, maxIndex+1)

		// Copy existing elements
		reflect.Copy(newSlice, sliceField)

		// Initialize new elements with zero values
		for i := sliceField.Len(); i <= maxIndex; i++ {
			newSlice.Index(i).Set(reflect.Zero(elementType))
		}

		// Replace the slice in config
		sliceField.Set(newSlice)
	}

	// Group overrides by index and source for precedence handling
	overridesByIndex := make(map[int]map[string][]types.SliceOverride)
	for _, override := range overrides {
		if overridesByIndex[override.Index] == nil {
			overridesByIndex[override.Index] = make(map[string][]types.SliceOverride)
		}
		overridesByIndex[override.Index][override.Source] = append(overridesByIndex[override.Index][override.Source], override)
	}

	// Apply overrides with precedence: flags > env
	for index, sourceMap := range overridesByIndex {
		// Get the slice element
		sliceElement := sliceField.Index(index)

		// Apply environment overrides first
		if envOverrides, exists := sourceMap["env"]; exists {
			for _, override := range envOverrides {
				err := setSliceElementField(sliceElement.Addr().Interface(), override.FieldPath, override.Value)
				if err != nil {
					return errors.Wrapf(err, "failed to set %s[%d].%s", sliceFieldName, index, override.FieldPath)
				}
			}
		}

		// Apply flag overrides (higher precedence)
		if flagOverrides, exists := sourceMap["flag"]; exists {
			for _, override := range flagOverrides {
				err := setSliceElementField(sliceElement.Addr().Interface(), override.FieldPath, override.Value)
				if err != nil {
					return errors.Wrapf(err, "failed to set %s[%d].%s", sliceFieldName, index, override.FieldPath)
				}
			}
		}
	}

	return nil
}

// setSliceElementField sets a field in any slice element using reflection
func setSliceElementField(sliceElement interface{}, fieldPath string, value interface{}) error {
	// Get the reflect.Value of the slice element
	elementValue := reflect.ValueOf(sliceElement)
	if elementValue.Kind() == reflect.Ptr {
		elementValue = elementValue.Elem()
	}

	// Handle simple fields (no dot)
	if !strings.Contains(fieldPath, ".") {
		return setReflectValue(elementValue.FieldByName(fieldPath), value, fieldPath)
	}

	// Handle nested fields (e.g., "AWS.Region", "GCE.Project")
	parts := strings.Split(fieldPath, ".")
	if len(parts) != 2 {
		return fmt.Errorf("unsupported nested field path: %s", fieldPath)
	}

	parentFieldName, childFieldName := parts[0], parts[1]

	// Navigate to the parent field (e.g., AWS, GCE, Azure, IBM)
	parentField := elementValue.FieldByName(parentFieldName)
	if !parentField.IsValid() {
		return fmt.Errorf("invalid parent field: %s", parentFieldName)
	}

	// Navigate to the child field within the parent
	childField := parentField.FieldByName(childFieldName)
	if !childField.IsValid() {
		return fmt.Errorf("invalid field %s in parent %s", childFieldName, parentFieldName)
	}

	return setReflectValue(childField, value, fieldPath)
}

// setReflectValue sets a reflect.Value with type conversion from string
func setReflectValue(field reflect.Value, value interface{}, fieldPath string) error {
	if !field.IsValid() {
		return fmt.Errorf("invalid field: %s", fieldPath)
	}

	if !field.CanSet() {
		return fmt.Errorf("cannot set field: %s", fieldPath)
	}

	// Convert string value to appropriate type
	strVal, ok := value.(string)
	if !ok {
		return fmt.Errorf("unsupported value type %T for field %s", value, fieldPath)
	}

	switch field.Kind() {
	case reflect.String:
		field.SetString(strVal)
	case reflect.Int, reflect.Int32, reflect.Int64:
		if intVal, err := strconv.ParseInt(strVal, 10, 64); err == nil {
			field.SetInt(intVal)
		} else {
			return fmt.Errorf("cannot convert %s to int for field %s", strVal, fieldPath)
		}
	case reflect.Bool:
		if boolVal, err := strconv.ParseBool(strVal); err == nil {
			field.SetBool(boolVal)
		} else {
			return fmt.Errorf("cannot convert %s to bool for field %s", strVal, fieldPath)
		}
	default:
		return fmt.Errorf("unsupported field type %s for field %s", field.Kind(), fieldPath)
	}

	return nil
}

// bindNonSliceFlagsToViper binds only non-slice flags to viper
func BindNonSliceFlagsToViper(flagSet *pflag.FlagSet, e *viper.Viper, cfg *types.Config) error {
	return recursive.ProcessFieldsRecursiveNonSlice(flagSet, e, reflect.ValueOf(cfg).Elem(), []string{}, []string{}, false)
}
