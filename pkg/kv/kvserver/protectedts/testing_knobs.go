// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package protectedts

import "github.com/cockroachdb/cockroach/pkg/base"

// TestingKnobs provide fine-grained control over the various span config
// components for testing.
type TestingKnobs struct {
	// DisableProtectedTimestampForMultiTenant when set to true, runs the
	// deprecated protected timestamp subsystem that does not work in a
	// multi-tenant environment.
	//
	// TODO(adityamaru): Delete in 22.2.
	DisableProtectedTimestampForMultiTenant bool

	// WriteDeprecatedPTSRecords When set to true, deprecated protected timestamp
	// records will be written, only if deprecated spans are supplied.
	WriteDeprecatedPTSRecords bool

	// UseMetaTable	forces PTS management to update the meta table whenever
	// any record is updated.
	UseMetaTable bool
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (t *TestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = (*TestingKnobs)(nil)
