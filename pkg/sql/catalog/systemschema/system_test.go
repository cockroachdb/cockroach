// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package systemschema

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestSecretBearingTables records whether each system table is secret-bearing
// (see secretBearingTables) in a golden file, together with the table's
// columns. A newly added table or column shows up as a diff, which is the
// prompt to decide whether it can carry a secret before regenerating the file
// with -rewrite.
func TestSecretBearingTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tables := MakeSystemTables()
	slices.SortFunc(tables, func(a, b SystemTable) int {
		return strings.Compare(a.GetName(), b.GetName())
	})
	for name := range secretBearingTables {
		require.Truef(t, slices.ContainsFunc(tables, func(tbl SystemTable) bool {
			return tbl.GetName() == string(name)
		}), "%s is not a system table", name)
	}

	var sb strings.Builder
	for i, tbl := range tables {
		if i > 0 {
			sb.WriteString("\n")
		}
		classification := "not secret-bearing"
		if IsSecretBearingTable(catconstants.SystemTableName(tbl.GetName())) {
			classification = "secret-bearing"
		}
		fmt.Fprintf(&sb, "%s: %s\n", tbl.GetName(), classification)
		for _, col := range tbl.PublicColumns() {
			fmt.Fprintf(&sb, "  - %s\n", col.GetName())
		}
	}
	echotest.Require(t, sb.String(), datapathutils.TestDataPath(t, "secret_bearing_tables"))
}

// TestIsSecretBearingTable pins the IsSecretBearingTable contract independently
// of the golden builder, including that a name not in secretBearingTables (such
// as a non-system-table name) reports false.
func TestIsSecretBearingTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name     string
		table    catconstants.SystemTableName
		expected bool
	}{
		{name: "secret-bearing table", table: catconstants.UsersTableName, expected: true},
		{name: "non-secret-bearing table", table: catconstants.DescriptorTableName, expected: false},
		{name: "not a system table", table: "does_not_exist", expected: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, IsSecretBearingTable(tc.table))
		})
	}
}
