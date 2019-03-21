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

package sqlerror

import "fmt"

// WithHintf adds a hint.
func WithHintf(err error, format string, args ...interface{}) error {
	if err == nil {
		return err
	}

	return &withHint{cause: err, hint: fmt.Sprintf(format, args...)}
}

// WithDetailf adds some detail.
func WithDetailf(err error, format string, args ...interface{}) error {
	if err == nil {
		return err
	}

	return &withDetail{cause: err, detail: fmt.Sprintf(format, args...)}
}

// WithTelemetryKey adds a telemetry key.
func WithTelemetryKey(key string, err error) error {
	if err == nil {
		return err
	}

	return &withTelemetryKey{cause: err, key: key}
}
