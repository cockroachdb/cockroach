// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuild_test

import (
	"bufio"
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps/sctestdeps"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps/sctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestBuildDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer utilccl.TestingEnableEnterprise()()

	ctx := context.Background()

	datadriven.Walk(t, testutils.TestDataPath(t), func(t *testing.T, path string) {
		for _, depsType := range []struct {
			name                string
			dependenciesWrapper func(*testing.T, serverutils.TestServerInterface, *sqlutils.SQLRunner, func(scbuild.Dependencies))
		}{
			{
				"sql_dependencies",
				func(t *testing.T, s serverutils.TestServerInterface, tdb *sqlutils.SQLRunner, fn func(scbuild.Dependencies)) {
					sctestutils.WithBuilderDependenciesFromTestServer(s, fn)
				},
			},
			{
				"test_dependencies",
				func(t *testing.T, s serverutils.TestServerInterface, tdb *sqlutils.SQLRunner, fn func(scbuild.Dependencies)) {
					// Create test dependencies and execute the schema changer.
					// The schema changer test dependencies do not hold any reference to the
					// test cluster, here the SQLRunner is only used to populate the mocked
					// catalog state.
					fn(sctestdeps.NewTestDependencies(
						sctestdeps.WithDescriptors(sctestdeps.ReadDescriptorsFromDB(ctx, t, tdb).Catalog),
						sctestdeps.WithNamespace(sctestdeps.ReadNamespaceFromDB(t, tdb).Catalog),
						sctestdeps.WithCurrentDatabase(sctestdeps.ReadCurrentDatabaseFromDB(t, tdb)),
						sctestdeps.WithSessionData(sctestdeps.ReadSessionDataFromDB(t, tdb, func(
							sd *sessiondata.SessionData,
						) {
							// For setting up a builder inside tests we will ensure that the new schema
							// changer will allow non-fully implemented operations.
							sd.NewSchemaChangerMode = sessiondatapb.UseNewSchemaChangerUnsafe
							sd.ApplicationName = ""
						}))))
				},
			},
		} {
			t.Run(depsType.name, func(t *testing.T) {
				s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
				defer s.Stopper().Stop(ctx)
				tdb := sqlutils.MakeSQLRunner(sqlDB)
				datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
					return run(ctx, t, depsType.name, d, s, tdb, depsType.dependenciesWrapper)
				})
			})
		}
	})
}

func run(
	ctx context.Context,
	t *testing.T,
	depsTypeName string,
	d *datadriven.TestData,
	s serverutils.TestServerInterface,
	tdb *sqlutils.SQLRunner,
	withDependencies func(*testing.T, serverutils.TestServerInterface, *sqlutils.SQLRunner, func(scbuild.Dependencies)),
) string {
	switch d.Cmd {
	case "create-table", "create-view", "create-type", "create-sequence", "create-schema", "create-database":
		stmts, err := parser.Parse(d.Input)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		tableName := ""
		switch node := stmts[0].AST.(type) {
		case *tree.CreateTable:
			tableName = node.Table.String()
		case *tree.CreateSequence:
			tableName = node.Name.String()
		case *tree.CreateView:
			tableName = node.Name.String()
		case *tree.CreateType:
			tableName = ""
		case *tree.CreateSchema:
			tableName = ""
		case *tree.CreateDatabase:
			tableName = ""
		default:
			t.Fatal("not a supported CREATE statement")
		}
		tdb.Exec(t, d.Input)

		if len(tableName) > 0 {
			var tableID descpb.ID
			tdb.QueryRow(t, fmt.Sprintf(`SELECT '%s'::REGCLASS::INT`, tableName)).Scan(&tableID)
			if tableID == 0 {
				t.Fatalf("failed to read ID of new table %s", tableName)
			}
			t.Logf("created relation with id %d", tableID)
		}

		return ""
	case "build":
		if a := d.CmdArgs; len(a) > 0 && a[0].Key == "skip" {
			for _, v := range a[0].Vals {
				if v == depsTypeName {
					return d.Expected
				}
			}
		}
		var output scpb.CurrentState
		withDependencies(t, s, tdb, func(deps scbuild.Dependencies) {
			stmts, err := parser.Parse(d.Input)
			require.NoError(t, err)
			for i := range stmts {
				output, err = scbuild.Build(ctx, deps, output, stmts[i].AST)
				require.NoErrorf(t, err, "%s", stmts[i].SQL)
			}
		})
		return marshalState(t, output)

	case "unimplemented":
		withDependencies(t, s, tdb, func(deps scbuild.Dependencies) {
			stmts, err := parser.Parse(d.Input)
			require.NoError(t, err)
			require.NotEmpty(t, stmts)

			for _, stmt := range stmts {
				_, err = scbuild.Build(ctx, deps, scpb.CurrentState{}, stmt.AST)
				expected := scerrors.NotImplementedError(nil)
				require.Errorf(t, err, "%s: expected %T instead of success for", stmt.SQL, expected)
				require.Truef(t, scerrors.HasNotImplemented(err), "%s: expected %T instead of %v", stmt.SQL, expected, err)
			}
		})
		return ""

	default:
		return fmt.Sprintf("unknown command: %s", d.Cmd)
	}
}

// indentText indents text for formatting out marshaled data.
func indentText(input string, tab string) string {
	result := strings.Builder{}
	scanner := bufio.NewScanner(strings.NewReader(input))
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := scanner.Text()
		result.WriteString(tab)
		result.WriteString(line)
		result.WriteString("\n")
	}
	return result.String()
}

// marshalState marshals a scpb.CurrentState to YAML.
func marshalState(t *testing.T, state scpb.CurrentState) string {
	var sortedEntries []string
	for i, status := range state.Current {
		node := screl.Node{
			Target:        &state.Targets[i],
			CurrentStatus: status,
		}
		yaml, err := sctestutils.ProtoToYAML(node.Target.Element())
		require.NoError(t, err)
		entry := strings.Builder{}
		entry.WriteString("- ")
		entry.WriteString(screl.NodeString(&node))
		entry.WriteString("\n")
		entry.WriteString(indentText("details:\n", "  "))
		entry.WriteString(indentText(yaml, "    "))
		sortedEntries = append(sortedEntries, entry.String())
	}
	// Sort the output buffer of state for determinism.
	result := strings.Builder{}
	sort.Strings(sortedEntries)
	for _, entry := range sortedEntries {
		result.WriteString(entry)
	}
	return result.String()
}
