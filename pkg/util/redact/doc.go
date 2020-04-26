// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package redact provides facilities for separating “safe” and
// “unsafe” pieces of data when logging and constructing error object.
//
// An item is said to be “safe” if it is proven to not contain
// PII or otherwise confidential information that should not escape
// the boundaries of the current system, for example via telemetry
// or crash reporting. Conversely, data is considered “unsafe”
// until/unless it is known to be “safe”.
//
// TODO(knz): Move this package into a separate top-level repository
// for use from cockroachdb/errors and other projects in the Go
// community.
package redact
