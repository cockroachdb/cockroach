// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuildstmt

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/stretchr/testify/require"
)

func TestSupportedStatementTags(t *testing.T) {
	sv := &settings.Values{}
	// Non-existent tags should error out.
	require.Error(t, forceDeclarativeStatements.Validate(sv, "FAKE STATEMENT"))
	// Generate the full set of statements
	allTags := strings.Builder{}
	noTags := strings.Builder{}
	first := true
	for tag := range supportedStatementTags {
		// Validate all tags are supported.
		require.NoError(t, forceDeclarativeStatements.Validate(sv, "+"+tag))
		require.NoError(t, forceDeclarativeStatements.Validate(sv, "!"+tag))
		// Validate all of them can be specified at once.
		if !first {
			allTags.WriteString(",")
			noTags.WriteString(",")
		}
		first = false
		allTags.WriteString("+")
		allTags.WriteString(tag)
		noTags.WriteString("!")
		noTags.WriteString(tag)
	}
	require.NoError(t, forceDeclarativeStatements.Validate(sv, allTags.String()))
	require.NoError(t, forceDeclarativeStatements.Validate(sv, noTags.String()))
}
