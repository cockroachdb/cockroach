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
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestBuilderAlterTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(sqlDB)

	// Store table names (extracted from the statement) and IDs (currently read
	// via SQL, for as long as we actually write and read the table descriptor via
	// SQL) so they can be referred to in later statements. This might improve
	// readability. I'm not totally sure this is a good idea.
	tableIDs := make(map[string]descpb.ID)

	datadriven.RunTest(t, filepath.Join("testdata", "alter_table"), func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "create-table":
			stmts, err := parser.Parse(d.Input)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			create, ok := stmts[0].AST.(*tree.CreateTable)
			if !ok {
				t.Fatal("not a CREATE TABLE statement")
			}

			tdb.Exec(t, d.Input)

			tableName := create.Table.String()
			var tableID descpb.ID
			tdb.QueryRow(t, `SELECT $1::regclass::int`, tableName).Scan(&tableID)
			if tableID == 0 {
				t.Fatalf("failed to read ID of new table %s", tableName)
			}
			t.Logf("created table with id %d", tableID)
			tableIDs[tableName] = tableID

			return ""
		case "build":
			b, cleanup := newTestingBuilder(s)
			defer cleanup()

			stmts, err := parser.Parse(d.Input)
			require.NoError(t, err)

			var ts []scpb.TargetState
			for i := range stmts {
				alter, ok := stmts[i].AST.(*tree.AlterTable)
				if !ok {
					t.Fatalf("not an ALTER TABLE statement: %s", stmts[i].SQL)
				}
				next, err := b.AlterTable(ctx, ts, alter)
				require.NoError(t, err)

				tableName := alter.Table.String()
				if _, ok := tableIDs[tableName]; !ok {
					t.Fatalf("unknown table name %q (for now, specified table names must be syntactically equal)", tableName)
				}

				ts = next
			}

			return marshalTargetStates(t, ts, tableIDs)
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

// marshalTargetStates basically marshals a []TargetState to YAML, but
// additionally annotates each TargetState with its type and rewrites table IDs.
func marshalTargetStates(
	t *testing.T, ts []scpb.TargetState, tableIDs map[string]descpb.ID,
) string {
	// TODO (lucy): It might be better to implement this with maps and interface{}
	// values with mapstructure or something, given how fiddly this is and how
	// unlikely we are to ever unmarshal this output, but this seems fine for now.
	type targetStateWithType struct {
		Type   string
		Target scpb.Target
		State  scpb.State
	}
	withTypes := make([]targetStateWithType, 0, len(ts))
	for i := range ts {
		withTypes = append(withTypes, targetStateWithType{
			Type:   reflect.TypeOf(ts[i].Target).String(),
			Target: ts[i].Target,
			State:  ts[i].State,
		})
	}

	out, err := yaml.Marshal(withTypes)
	require.NoError(t, err)
	s := string(out)

	// Expedient hack to replace table IDs with their names in the output. This is
	// not going to work when the IDs get too large.
	for name, id := range tableIDs {
		s = strings.ReplaceAll(
			s,
			fmt.Sprintf(`tableid: %d`, id),
			fmt.Sprintf(`tableid: $%s`, name),
		)
	}
	return s
}

func newTestingBuilder(s serverutils.TestServerInterface) (*scbuild.Builder, func()) {
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	ip, cleanup := sql.NewInternalPlanner(
		"test",
		kv.NewTxn(context.Background(), s.DB(), s.NodeID()),
		security.RootUserName(),
		&sql.MemoryMetrics{},
		&execCfg,
		// Setting the database on the session data to "defaultdb" in the obvious
		// way doesn't seem to do what we want.
		sessiondatapb.SessionData{},
	)
	planner := ip.(interface {
		resolver.SchemaResolver
		SemaCtx() *tree.SemaContext
		EvalContext() *tree.EvalContext
	})
	return scbuild.NewBuilder(planner, planner.SemaCtx(), planner.EvalContext()), cleanup
}
