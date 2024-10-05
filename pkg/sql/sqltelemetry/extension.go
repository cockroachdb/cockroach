// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqltelemetry

import "github.com/cockroachdb/cockroach/pkg/server/telemetry"

// CreateExtensionCounter returns a counter to increment for creating extensions.
func CreateExtensionCounter(ext string) telemetry.Counter {
	return telemetry.GetCounter("sql.extension.create." + ext)
}
