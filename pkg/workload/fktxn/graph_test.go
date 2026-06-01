// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fktxn

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// discoverSchemaFromDDL creates a database, executes DDL, and returns
// the discovered Schema.
func discoverSchemaFromDDL(
	t *testing.T,
	srv serverutils.TestServerInterface,
	sqlDB *sqlutils.SQLRunner,
	dbName string,
	ddl string,
) *Schema {
	t.Helper()
	sqlDB.Exec(t, fmt.Sprintf("CREATE DATABASE %s", tree.NameString(dbName)))
	testDB := srv.ApplicationLayer().SQLConn(t, serverutils.DBName(dbName))
	testSQL := sqlutils.MakeSQLRunner(testDB)
	for _, stmt := range strings.Split(ddl, ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		testSQL.Exec(t, stmt)
	}
	s, err := DiscoverSchema(testDB, dbName)
	require.NoError(t, err)
	return s
}

func graphTableNames(g *FKGraph) []string {
	names := make([]string, 0, len(g.Tables))
	for name := range g.Tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func TestBuildFKGraphs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	tests := []struct {
		name           string
		ddl            string
		expectedGraphs [][]string // sorted table names per graph
	}{
		{
			// Each FK uses a different column, so no column overlap.
			// Each FK edge is its own graph.
			name: "simple_chain_no_overlap",
			ddl: `
				CREATE TABLE a (id INT PRIMARY KEY);
				CREATE TABLE b (id INT PRIMARY KEY, a_id INT NOT NULL REFERENCES a(id));
				CREATE TABLE c (id INT PRIMARY KEY, b_id INT NOT NULL REFERENCES b(id))`,
			expectedGraphs: [][]string{{"a", "b"}, {"b", "c"}},
		},
		{
			name:           "fan_out",
			ddl:            fanOutDDL,
			expectedGraphs: [][]string{{"a", "b", "c"}},
		},
		{
			name:           "fan_out_with_separate_child",
			ddl:            fanOutWithSeparateChildDDL,
			expectedGraphs: [][]string{{"a", "b", "c"}, {"b", "d"}, {"c", "d"}},
		},
		{
			name:           "two_disconnected_fks",
			ddl:            twoDisconnectedFKsDDL,
			expectedGraphs: [][]string{{"a", "b"}, {"c", "d"}},
		},
		{
			name:           "transitive_overlap",
			ddl:            transitiveOverlapDDL,
			expectedGraphs: [][]string{{"countries", "regions", "stores"}},
		},
		{
			name: "non_transitive",
			ddl:  nonTransitiveDDL,
			expectedGraphs: [][]string{
				{"countries", "regions", "stores"},
				{"inventory", "stores"},
			},
		},
		{
			name:           "diamond_composite",
			ddl:            diamondCompositeDDL,
			expectedGraphs: [][]string{{"depts", "orgs", "projects", "teams"}},
		},
		{
			name:           "deep_transitive_chain",
			ddl:            deepTransitiveChainDDL,
			expectedGraphs: [][]string{{"l1", "l2", "l3", "l4"}},
		},
		{
			name:           "fan_in_composite",
			ddl:            fanInCompositeDDL,
			expectedGraphs: [][]string{{"comments", "posts", "tenants", "users"}},
		},
		{
			name: "mixed_overlap_and_isolated",
			ddl:  mixedOverlapAndIsolatedDDL,
			expectedGraphs: [][]string{
				{"configs", "settings"},
				{"posts", "tenants", "users"},
			},
		},
		{
			name:           "self_referencing",
			ddl:            selfRefDDL,
			expectedGraphs: [][]string{{"employees"}},
		},
		{
			name:           "no_fks",
			ddl:            noFKsDDL,
			expectedGraphs: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := discoverSchemaFromDDL(t, srv, sqlDB, "bg_"+tc.name, tc.ddl)
			graphs := BuildFKGraphs(s)

			if tc.expectedGraphs == nil {
				require.Empty(t, graphs)
				return
			}

			require.Len(t, graphs, len(tc.expectedGraphs))
			for i, g := range graphs {
				require.Equal(t, tc.expectedGraphs[i], graphTableNames(g))
			}
		})
	}
}
