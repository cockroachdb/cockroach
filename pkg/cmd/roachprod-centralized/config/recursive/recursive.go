// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package recursive

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/config/flags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/config/types"
	"github.com/iancoleman/strcase"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// bindFlagsToViper binds existing flags to viper without recreating them
func BindFlagsToViper(flagSet *pflag.FlagSet, e *viper.Viper, cfg *types.Config) error {
	return ProcessFieldsRecursive(flagSet, e, reflect.ValueOf(cfg).Elem(), []string{}, []string{}, false)
}

// createFlags creates command line flags for the NewConfig struct.
// The flags are created using a FlagSet, and the flag names are built
// from the field names, using kebab-case and prefixes based on the nested structure.
// For example, NewConfig.Api.Metrics.Enabled becomes --api-metrics-enabled.
func CreateFlags(fs *pflag.FlagSet, e *viper.Viper, c interface{}) error {
	return ProcessFieldsRecursive(fs, e, reflect.ValueOf(c).Elem(), []string{}, []string{}, true)
}

// processFieldsRecursive recursively traverses the struct and either creates flags or binds existing ones.
// If createFlags is true, it creates new flags; if false, it only binds existing flags to viper.
func ProcessFieldsRecursive(
	fs *pflag.FlagSet,
	e *viper.Viper,
	v reflect.Value,
	fieldPrefixes, flagEnvPrefixes []string,
	createFlags bool,
) error {
	t := v.Type()

	for i := range t.NumField() {
		field := t.Field(i)
		fieldValue := v.Field(i)

		// Skip unexported fields
		if !fieldValue.CanSet() {
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
			err := ProcessFieldsRecursive(fs, e, fieldValue,
				append(fieldPrefixes, field.Name),
				append(flagEnvPrefixes, strcase.ToKebab(envTag)),
				createFlags,
			)
			if err != nil {
				action := "binding"
				if createFlags {
					action = "creating"
				}
				return errors.Wrapf(err, "%s flags for struct field %s", action, field.Name)
			}
			continue
		}
		if field.Type.Kind() == reflect.Slice {
			elemKind := field.Type.Elem().Kind()
			if elemKind == reflect.Struct {
				// Handle slice of structs by creating indexed flags
				if createFlags {
					err := flags.CreateSliceFlags(fs, field, fieldPrefixes, flagEnvPrefixes)
					if err != nil {
						return errors.Wrapf(err, "creating slice flags for field %s", field.Name)
					}
				}
				continue
			}
			// Handle slice of primitives (e.g., []string, []int, []bool)
			// with pflag's native slice support
			fieldName := strings.Join(
				append(append([]string{}, fieldPrefixes...), field.Name), ".",
			)
			flagName := strings.Join(
				append(append([]string{}, flagEnvPrefixes...), strcase.ToKebab(envTag)), "-",
			)
			envName := strings.ToUpper(strings.ReplaceAll(flagName, "-", "_"))
			if createFlags {
				switch elemKind {
				case reflect.String:
					fs.StringSlice(flagName, nil, desc)
				case reflect.Int:
					fs.IntSlice(flagName, nil, desc)
				case reflect.Bool:
					fs.BoolSlice(flagName, nil, desc)
				default:
					continue
				}
				if err := e.BindPFlag(fieldName, fs.Lookup(flagName)); err != nil {
					return errors.Wrapf(err, "binding flag for %s", fieldName)
				}
			} else {
				if flag := fs.Lookup(flagName); flag != nil {
					if err := e.BindPFlag(fieldName, flag); err != nil {
						return errors.Wrapf(err, "binding flag for %s", fieldName)
					}
				}
			}
			if err := e.BindEnv(fieldName, fmt.Sprintf("%s_%s", types.EnvPrefix, envName)); err != nil {
				return errors.Wrapf(err, "binding env var for %s", fieldName)
			}
			continue
		}

		fieldName := strings.Join(append(fieldPrefixes, field.Name), ".")
		flagName := strings.Join(append(flagEnvPrefixes, strcase.ToKebab(envTag)), "-")
		envName := strings.ToUpper(strings.ReplaceAll(flagName, "-", "_"))

		if createFlags {
			// Create flag based on the field type
			switch field.Type.Kind() {
			case reflect.String:
				defaultStr := defaultVal
				fs.StringVar(fieldValue.Addr().Interface().(*string), flagName, defaultStr, desc)
			case reflect.Int:
				defaultInt := 0
				if defaultVal != "" {
					if i, err := strconv.Atoi(defaultVal); err == nil {
						defaultInt = i
					}
				}
				fs.IntVar(fieldValue.Addr().Interface().(*int), flagName, defaultInt, desc)
			case reflect.Bool:
				defaultBool := false
				if defaultVal != "" {
					if b, err := strconv.ParseBool(defaultVal); err == nil {
						defaultBool = b
					}
				}
				fs.BoolVar(fieldValue.Addr().Interface().(*bool), flagName, defaultBool, desc)
			default:
				return fmt.Errorf("unsupported field type %s for field %s", field.Type.Kind(), fieldName)
			}

			// Bind the newly created flag to viper
			err := e.BindPFlag(fieldName, fs.Lookup(flagName))
			if err != nil {
				return errors.Wrapf(err, "binding flag for %s", fieldName)
			}
		} else {
			// Only bind existing flag to viper (for the InitConfigWithFlagSet case)
			if flag := fs.Lookup(flagName); flag != nil {
				err := e.BindPFlag(fieldName, flag)
				if err != nil {
					return errors.Wrapf(err, "binding flag for %s", fieldName)
				}
			}
		}

		// Always bind environment variables
		err := e.BindEnv(fieldName, fmt.Sprintf("%s_%s", types.EnvPrefix, envName))
		if err != nil {
			return errors.Wrapf(err, "binding env var for %s", fieldName)
		}
	}

	return nil
}

// processFieldsRecursiveNonSlice is like processFieldsRecursive but skips slice fields
func ProcessFieldsRecursiveNonSlice(
	fs *pflag.FlagSet,
	e *viper.Viper,
	v reflect.Value,
	fieldPrefixes, flagEnvPrefixes []string,
	createFlags bool,
) error {
	t := v.Type()

	for i := range t.NumField() {
		field := t.Field(i)
		fieldValue := v.Field(i)

		// Skip unexported fields
		if !fieldValue.CanSet() {
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
			err := ProcessFieldsRecursiveNonSlice(fs, e, fieldValue,
				append(fieldPrefixes, field.Name),
				append(flagEnvPrefixes, strcase.ToKebab(envTag)),
				createFlags,
			)
			if err != nil {
				action := "binding"
				if createFlags {
					action = "creating"
				}
				return errors.Wrapf(err, "%s flags for struct field %s", action, field.Name)
			}
			continue
		}

		// Skip slice fields - they are handled separately
		if field.Type.Kind() == reflect.Slice {
			continue
		}

		fieldName := strings.Join(append(fieldPrefixes, field.Name), ".")
		flagName := strings.Join(append(flagEnvPrefixes, strcase.ToKebab(envTag)), "-")
		envName := strings.ToUpper(strings.ReplaceAll(flagName, "-", "_"))

		if createFlags {
			// Create flag based on the field type
			switch field.Type.Kind() {
			case reflect.String:
				defaultStr := defaultVal
				fs.StringVar(fieldValue.Addr().Interface().(*string), flagName, defaultStr, desc)
			case reflect.Int:
				defaultInt := 0
				if defaultVal != "" {
					if i, err := strconv.Atoi(defaultVal); err == nil {
						defaultInt = i
					}
				}
				fs.IntVar(fieldValue.Addr().Interface().(*int), flagName, defaultInt, desc)
			case reflect.Bool:
				defaultBool := false
				if defaultVal != "" {
					if b, err := strconv.ParseBool(defaultVal); err == nil {
						defaultBool = b
					}
				}
				fs.BoolVar(fieldValue.Addr().Interface().(*bool), flagName, defaultBool, desc)
			default:
				return fmt.Errorf("unsupported field type %s for field %s", field.Type.Kind(), fieldName)
			}

			// Bind the newly created flag to viper
			err := e.BindPFlag(fieldName, fs.Lookup(flagName))
			if err != nil {
				return errors.Wrapf(err, "binding flag for %s", fieldName)
			}
		} else {
			// Only bind existing flag to viper (for the InitConfigWithFlagSet case)
			if flag := fs.Lookup(flagName); flag != nil {
				err := e.BindPFlag(fieldName, flag)
				if err != nil {
					return errors.Wrapf(err, "binding flag for %s", fieldName)
				}
			}
		}

		// Always bind environment variables
		err := e.BindEnv(fieldName, fmt.Sprintf("%s_%s", types.EnvPrefix, envName))
		if err != nil {
			return errors.Wrapf(err, "binding env var for %s", fieldName)
		}
	}

	return nil
}
