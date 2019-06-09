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

package errors

import "github.com/cockroachdb/cockroach/pkg/errors/safedetails"

// WithSafeDetails forwards a definition.
var WithSafeDetails func(err error, format string, args ...interface{}) error = safedetails.WithSafeDetails

// SafeMessager forwards a definition.
type SafeMessager = safedetails.SafeMessager

// Safe forwards a definition.
var Safe func(v interface{}) SafeMessager = safedetails.Safe

// Redact returns a redacted version of the supplied item that is safe
// to use in anonymized reporting.
var Redact func(r interface{}) string = safedetails.Redact
