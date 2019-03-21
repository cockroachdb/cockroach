// Copyright 2019 The Cockroach Authors.
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
//

package pgerror

import "fmt"

// WithHint adds a hint. Nothing is added if the hint is empty.
func WithHint(err error, hint string) error {
	if err == nil || hint == "" {
		return err
	}

	return &withHint{cause: err, hint: hint}
}

// WithHintf adds a hint. Nothing is added if the hint ends up empty.
func WithHintf(err error, format string, args ...interface{}) error {
	if err == nil || (format == "" && len(args) == 0) {
		return err
	}

	return &withHint{cause: err, hint: fmt.Sprintf(format, args...)}
}

// WithDetail adds some detail. Nothing is added if the detail text is empty.
func WithDetail(err error, detail string) error {
	if err == nil || detail == "" {
		return err
	}

	return &withDetail{cause: err, detail: detail}
}

// WithDetailf adds some detail. Nothing is added if the detail ends
// up empty.
func WithDetailf(err error, format string, args ...interface{}) error {
	if err == nil || (format == "" && len(args) == 0) {
		return err
	}

	return &withDetail{cause: err, detail: fmt.Sprintf(format, args...)}
}

// WithTelemetryKey adds a telemetry key. Nothing is added if the
// telemetry key string is empty.
func WithTelemetryKey(key string, err error) error {
	if err == nil || key == "" {
		return err
	}

	return &withTelemetryKey{cause: err, key: key}
}
