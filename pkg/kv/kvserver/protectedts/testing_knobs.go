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
	// EnableProtectedTimestampForMultiTenant when set to true, runs the protected
	// timestamp subsystem that depends on span configuration reconciliation.
	//
	// TODO(adityamaru,arulajmani): Default this to true, or flip it to
	// `DisableProtectedTimestampForMultiTenant` prior to release 22.1.
	EnableProtectedTimestampForMultiTenant bool
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (t *TestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = (*TestingKnobs)(nil)
