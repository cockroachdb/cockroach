// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package optbuilder

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestSupportedCRDBInternalBuiltinsNotChanged verifies that the
// SupportedCRDBInternalBuiltins map has not changed from its expected values.
// This test ensures no builtins are inadvertently added to this locked list.
func TestSupportedCRDBInternalBuiltinsNotChanged(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Hardcoded expected values for SupportedCRDBInternalBuiltins
	// IMPORTANT: This list is LOCKED and should NOT be modified.
	// New builtins should be added to information_schema instead.
	expectedBuiltins := map[string]struct{}{
		`crdb_internal.datums_to_bytes`:           {},
		`crdb_internal.increment_feature_counter`: {},
		`crdb_internal.deserialize_session`:       {},
	}

	// Check that the actual map matches the expected map
	if len(SupportedCRDBInternalBuiltins) != len(expectedBuiltins) {
		t.Fatalf("FAILURE: SupportedCRDBInternalBuiltins has been modified!\n"+
			"This list is LOCKED and should NOT be changed.\n"+
			"New crdb_internal builtins should be added to information_schema instead.\n"+
			"Expected %d builtins, but found %d builtins.\n"+
			"See: https://docs.google.com/document/d/1STbb8bljTzK_jXRIJrxtijWsPhGErdH1vZdunzPwXvs/edit",
			len(expectedBuiltins), len(SupportedCRDBInternalBuiltins))
	}

	// Check each expected builtin is present
	for builtin := range expectedBuiltins {
		if _, ok := SupportedCRDBInternalBuiltins[builtin]; !ok {
			t.Fatalf("FAILURE: SupportedCRDBInternalBuiltins has been modified!\n" +
				"This list is LOCKED and should NOT be changed.\n" +
				"New crdb_internal builtins should be added to information_schema instead.\n" +
				"See: https://docs.google.com/document/d/1STbb8bljTzK_jXRIJrxtijWsPhGErdH1vZdunzPwXvs/edit")
		}
	}
}
