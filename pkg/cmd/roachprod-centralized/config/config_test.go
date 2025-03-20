// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package config

import (
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoad(t *testing.T) {
	tests := []struct {
		name        string
		prefix      string
		envVars     map[string]string
		args        []string
		expected    *Config
		shouldError bool
	}{
		{
			name:   "default values",
			prefix: "APP",
			expected: &Config{
				LogLevel:                  "info",
				ApiPort:                   8080,
				ApiBaseURL:                "",
				ApiMetrics:                true,
				ApiMetricsPort:            8081,
				ApiAuthenticationDisabled: false,
				ApiAuthenticationHeader:   "X-Goog-IAP-JWT-Assertion",
				ApiAuthenticationAudience: "",
			},
		},
		{
			name:   "environment variables",
			prefix: "APP",
			envVars: map[string]string{
				"APP_LOG_LEVEL":                       "debug",
				"APP_API_PORT":                        "9000",
				"APP_API_BASE_URL":                    "http://localhost",
				"APP_API_METRICS":                     "false",
				"APP_API_METRICS_PORT":                "9001",
				"APP_API_AUTHENTICATION_DISABLED":     "true",
				"APP_API_AUTHENTICATION_JWT_HEADER":   "Custom-Header",
				"APP_API_AUTHENTICATION_JWT_AUDIENCE": "test-audience",
			},
			expected: &Config{
				LogLevel:                  "debug",
				ApiPort:                   9000,
				ApiBaseURL:                "http://localhost",
				ApiMetrics:                false,
				ApiMetricsPort:            9001,
				ApiAuthenticationDisabled: true,
				ApiAuthenticationHeader:   "Custom-Header",
				ApiAuthenticationAudience: "test-audience",
			},
		},
		{
			name:   "invalid environment variable",
			prefix: "APP",
			envVars: map[string]string{
				"APP_API_PORT": "invalid",
			},
			shouldError: true,
		},
		{
			name:        "invalid flag value for int",
			prefix:      "APP",
			args:        []string{"--api-port", "invalid"},
			shouldError: true,
		},
		{
			name:        "unknown flag",
			prefix:      "APP",
			args:        []string{"--unknown-flag", "value"},
			shouldError: true,
		},
		{
			name:   "invalid env var for int",
			prefix: "APP",
			envVars: map[string]string{
				"APP_API_PORT": "invalid",
			},
			shouldError: true,
		},
		{
			name:   "invalid env var for bool",
			prefix: "APP",
			envVars: map[string]string{
				"APP_API_METRICS": "invalid",
			},
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset environment
			os.Clearenv()
			for k, v := range tt.envVars {
				err := os.Setenv(k, v)
				assert.NoError(t, err)
			}

			// Save and restore original args
			oldArgs := os.Args
			defer func() { os.Args = oldArgs }()
			os.Args = append([]string{oldArgs[0]}, tt.args...)

			config, err := Load(tt.prefix)

			if tt.shouldError {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, config)
		})
	}
}

func TestSetField(t *testing.T) {
	type TestStruct struct {
		String string
		Int    int
		Bool   bool
		Chan   chan int // unsupported type
	}

	tests := []struct {
		name        string
		field       interface{}
		value       string
		shouldError bool
	}{
		{
			name:  "valid string",
			field: reflect.ValueOf(&TestStruct{}).Elem().FieldByName("String"),
			value: "test",
		},
		{
			name:  "valid int",
			field: reflect.ValueOf(&TestStruct{}).Elem().FieldByName("Int"),
			value: "42",
		},
		{
			name:  "valid bool",
			field: reflect.ValueOf(&TestStruct{}).Elem().FieldByName("Bool"),
			value: "true",
		},
		{
			name:        "invalid int",
			field:       reflect.ValueOf(&TestStruct{}).Elem().FieldByName("Int"),
			value:       "invalid",
			shouldError: true,
		},
		{
			name:        "invalid bool",
			field:       reflect.ValueOf(&TestStruct{}).Elem().FieldByName("Bool"),
			value:       "invalid",
			shouldError: true,
		},
		{
			name:        "unsupported type",
			field:       reflect.ValueOf(&TestStruct{}).Elem().FieldByName("Chan"),
			value:       "any",
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := setField(tt.field.(reflect.Value), tt.value)
			if tt.shouldError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestKebabCase(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{
			input:    "SOME_VAR_NAME",
			expected: "some-var-name",
		},
		{
			input:    "ALREADY_KEBAB-CASE",
			expected: "already-kebab-case",
		},
		{
			input:    "simple",
			expected: "simple",
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := kebabCase(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
