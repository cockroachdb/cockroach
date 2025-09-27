// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package config

import (
	"flag"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
)

// Config holds the configuration for the application.
type Config struct {
	LogLevel                  string `env:"LOG_LEVEL" default:"info" description:"Logging level (debug, info, warn, error)"`
	ApiPort                   int    `env:"API_PORT" default:"8080" description:"HTTP API port"`
	ApiBaseURL                string `env:"API_BASE_URL" default:"" description:"Base URL for API endpoints"`
	ApiMetrics                bool   `env:"API_METRICS" default:"true" description:"Enable metrics"`
	ApiMetricsPort            int    `env:"API_METRICS_PORT" default:"8081" description:"Metrics HTTP port"`
	ApiAuthenticationDisabled bool   `env:"API_AUTHENTICATION_DISABLED" default:"false" description:"Disable API authentication"`
	ApiAuthenticationHeader   string `env:"API_AUTHENTICATION_JWT_HEADER" default:"X-Goog-IAP-JWT-Assertion" description:"JWT authentication header"`
	ApiAuthenticationAudience string `env:"API_AUTHENTICATION_JWT_AUDIENCE" default:"" description:"JWT audience for authentication"`
}

// Load loads the configuration from environment variables and flags
// and returns a Config struct.
func Load(prefix string) (*Config, error) {
	config := &Config{}

	if err := loadDefaults(config); err != nil {
		return nil, fmt.Errorf("loading defaults: %w", err)
	}

	if err := loadEnv(prefix, config); err != nil {
		return nil, fmt.Errorf("loading env vars: %w", err)
	}

	flags := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	if err := loadFlags(config, flags); err != nil {
		return nil, fmt.Errorf("loading flags: %w", err)
	}

	return config, nil
}

// loadDefaults loads default values from struct tags.
func loadDefaults(config *Config) error {
	v := reflect.ValueOf(config).Elem()
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if defaultVal := field.Tag.Get("default"); defaultVal != "" {
			if err := setField(v.Field(i), defaultVal); err != nil {
				return fmt.Errorf("setting default for %s: %w", field.Name, err)
			}
		}
	}
	return nil
}

// loadEnv loads configuration from environment variables.
func loadEnv(prefix string, config *Config) error {
	v := reflect.ValueOf(config).Elem()
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if envKey := field.Tag.Get("env"); envKey != "" {
			if val := os.Getenv(prefix + "_" + envKey); val != "" {
				if err := setField(v.Field(i), val); err != nil {
					return fmt.Errorf("setting env var for %s: %w", field.Name, err)
				}
			}
		}
	}
	return nil
}

// loadFlags defines and parses flags for the configuration struct.
func loadFlags(config *Config, flags *flag.FlagSet) error {
	v := reflect.ValueOf(config).Elem()
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		flagName := kebabCase(field.Tag.Get("env"))
		desc := field.Tag.Get("description")

		switch field.Type.Kind() {
		case reflect.String:
			flags.StringVar(v.Field(i).Addr().Interface().(*string), flagName, v.Field(i).String(), desc)
		case reflect.Int:
			flags.IntVar(v.Field(i).Addr().Interface().(*int), flagName, int(v.Field(i).Int()), desc)
		case reflect.Bool:
			flags.BoolVar(v.Field(i).Addr().Interface().(*bool), flagName, v.Field(i).Bool(), desc)
		}
	}

	return flags.Parse(os.Args[1:])
}

// setField sets a value on a struct field.
func setField(field reflect.Value, value string) error {
	switch field.Kind() {
	case reflect.String:
		field.SetString(value)
	case reflect.Int:
		i, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		field.SetInt(int64(i))
	case reflect.Bool:
		b, err := strconv.ParseBool(value)
		if err != nil {
			return err
		}
		field.SetBool(b)
	default:
		return fmt.Errorf("unsupported type: %s", field.Kind())
	}
	return nil
}

// kebabCase converts a string to kebab-case.
// Example: "SOME_VAR_NAME" -> "some-var-name"
func kebabCase(s string) string {
	return strings.ToLower(strings.ReplaceAll(s, "_", "-"))
}
