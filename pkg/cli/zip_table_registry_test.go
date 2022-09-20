// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestQueryForTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	reg := DebugZipTableRegistry{
		"table_with_sensitive_cols": {
			nonSensitiveCols: NonSensitiveColumns{"x", "y", "z"},
		},
		"table_with_empty_sensitive_cols": {
			nonSensitiveCols: NonSensitiveColumns{},
		},
		"table_with_custom_queries": {
			customQueryUnredacted: "SELECT * FROM table_with_custom_queries",
			customQueryRedacted:   "SELECT a, b, c FROM table_with_custom_queries",
		},
	}

	t.Run("errors if no table config present in registry", func(t *testing.T) {
		actual, err := reg.QueryForTable("does_not_exist", false /* redact */)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no entry found")
		assert.Empty(t, actual)
	})

	t.Run("produces `TABLE` query when unredacted with no custom query", func(t *testing.T) {
		table := "table_with_sensitive_cols"
		expected := "TABLE table_with_sensitive_cols"
		actual, err := reg.QueryForTable(table, false /* redact */)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})

	t.Run("produces custom query when unredacted and custom query supplied", func(t *testing.T) {
		table := "table_with_custom_queries"
		expected := "SELECT * FROM table_with_custom_queries"
		actual, err := reg.QueryForTable(table, false /* redact */)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})

	t.Run("produces query with only non-sensitive columns when redacted and no custom query", func(t *testing.T) {
		table := "table_with_sensitive_cols"
		expected := `SELECT x, y, z FROM table_with_sensitive_cols`
		actual, err := reg.QueryForTable(table, true /* redact */)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})

	t.Run("produces custom when redacted and custom query supplied", func(t *testing.T) {
		table := "table_with_custom_queries"
		expected := "SELECT a, b, c FROM table_with_custom_queries"
		actual, err := reg.QueryForTable(table, true /* redact */)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})

	t.Run("returns error when no custom queries and no non-sensitive columns supplied", func(t *testing.T) {
		table := "table_with_empty_sensitive_cols"
		actual, err := reg.QueryForTable(table, true /* redact */)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no non-sensitive columns defined")
		assert.Empty(t, actual)
	})
}

func TestNoForbiddenSystemTablesInDebugZip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	forbiddenSysTables := []string{
		"system.users",
		"system.web_sessions",
		"system.join_tokens",
		"system.comments",
		"system.ui",
		"system.zones",
		"system.statement_bundle_chunks",
		"system.statement_statistics",
		"system.transaction_statistics",
	}
	for _, forbiddenTable := range forbiddenSysTables {
		query, err := zipSystemTables.QueryForTable(forbiddenTable, false /* redact */)
		assert.Equal(t, "", query)
		assert.Error(t, err)
		assert.Equal(t, fmt.Sprintf("no entry found in table registry for: %s", forbiddenTable), err.Error())
	}
}
