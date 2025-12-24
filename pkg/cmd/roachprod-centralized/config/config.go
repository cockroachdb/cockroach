// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/config/flags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/config/processing"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/config/recursive"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/config/types"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// AddFlagsToFlagSet adds all configuration flags to the provided FlagSet
// This should be called during command initialization to make flags visible in help
func AddFlagsToFlagSet(flagSet *pflag.FlagSet) error {
	config := &types.Config{}
	dummyViper := viper.New()
	return recursive.CreateFlags(flagSet, dummyViper, config)
}

// InitConfigWithFlagSet initializes configuration using an external FlagSet (for Cobra integration)
// Precedence order: Flags > Environment Variables > Config files > Defaults
func InitConfigWithFlagSet(flagSet *pflag.FlagSet) (*types.Config, error) {
	// Init Viper instance
	v := viper.New()
	config := &types.Config{}

	// Check if a config file is specified via the flagset
	configFile := ""
	if flag := flagSet.Lookup("config"); flag != nil && flag.Value.String() != "" {
		configFile = flag.Value.String()
	}

	// If a configuration file is specified, read it first as base configuration
	if configFile != "" {
		file, err := os.Open(configFile)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to read config file %s", configFile)
		}
		defer file.Close()

		v.SetConfigType(strings.TrimPrefix(filepath.Ext(configFile), "."))
		if err := v.ReadConfig(file); err != nil {
			return nil, errors.Wrap(err, "unable to parse config file")
		}

		// Unmarshal config file into base config
		if err := v.Unmarshal(config); err != nil { // nolint:protounmarshal
			return nil, errors.Wrap(err, "unable to unmarshal config file")
		}
	}

	// Before collecting overrides, dynamically create any missing flags for slice indices
	err := flags.CreateMissingSliceFlags(flagSet)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create missing slice flags")
	}

	// Collect slice overrides from flags and environment variables
	sliceOverrides, err := processing.CollectSliceOverrides(flagSet)
	if err != nil {
		return nil, errors.Wrap(err, "unable to collect slice overrides")
	}

	// Apply slice overrides to the base configuration
	err = processing.ApplySliceOverrides(config, sliceOverrides)
	if err != nil {
		return nil, errors.Wrap(err, "unable to apply slice overrides")
	}

	// Bind non-slice flags and environment variables to viper for remaining fields
	err = processing.BindNonSliceFlagsToViper(flagSet, v, config)
	if err != nil {
		return nil, errors.Wrap(err, "unable to bind non-slice flags to viper")
	}

	// Unmarshal final config from viper (for non-slice fields)
	tempConfig := &types.Config{}
	if err := v.Unmarshal(tempConfig); err != nil { // nolint:protounmarshal
		return nil, errors.Wrap(err, "unable to unmarshal final config")
	}

	// Merge non-slice fields from viper into our slice-processed config
	// Use reflection to copy all non-slice fields to avoid manual maintenance
	mergeNonSliceFields(config, tempConfig)

	return config, nil
}

// mergeNonSliceFields copies all non-slice fields from src to dst using reflection.
// This ensures that new fields are automatically handled without manual updates.
func mergeNonSliceFields(dst, src *types.Config) {
	dstVal := reflect.ValueOf(dst).Elem()
	srcVal := reflect.ValueOf(src).Elem()

	for i := 0; i < srcVal.NumField(); i++ {
		field := srcVal.Type().Field(i)
		srcField := srcVal.Field(i)
		dstField := dstVal.Field(i)

		// Skip slice fields - they're handled separately via slice override mechanism
		if field.Type.Kind() == reflect.Slice {
			continue
		}

		// Copy non-slice fields from src to dst
		if dstField.CanSet() {
			dstField.Set(srcField)
		}
	}
}
