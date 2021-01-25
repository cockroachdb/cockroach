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
	"bytes"
	"context"
	gojson "encoding/json"
	"fmt"
	"path/filepath"
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
	"github.com/gogo/protobuf/jsonpb"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestBuilderAlterTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	datadriven.Walk(t, filepath.Join("testdata"), func(t *testing.T, path string) {
		s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
		defer s.Stopper().Stop(ctx)

		tdb := sqlutils.MakeSQLRunner(sqlDB)
		run := func(t *testing.T, d *datadriven.TestData) string {
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

				return ""
			case "build":
				b, cleanup := newTestingBuilder(s)
				defer cleanup()

				stmts, err := parser.Parse(d.Input)
				require.NoError(t, err)

				var ts []*scpb.Node
				for i := range stmts {
					next, err := b.Build(ctx, ts, stmts[i].AST)
					require.NoError(t, err)
					ts = next
				}

				return marshalNodes(t, ts)
			case "unimplemented":
				b, cleanup := newTestingBuilder(s)
				defer cleanup()

				stmts, err := parser.Parse(d.Input)
				require.NoError(t, err)
				require.Len(t, stmts, 1)

				stmt := stmts[0]
				alter, ok := stmt.AST.(*tree.AlterTable)
				require.Truef(t, ok, "not an ALTER TABLE statement: %s", stmt.SQL)
				_, err = b.AlterTable(ctx, nil, alter)
				require.Truef(t, scbuild.HasNotImplemented(err), "expected unimplemented, got %v", err)
				return ""

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		}
		datadriven.RunTest(t, path, run)
	})
}

// marshalNodes marshals a []*scpb.Node to YAML.
func marshalNodes(t *testing.T, nodes []*scpb.Node) string {
	type mapNode struct {
		Target map[string]interface{}
		State  string
	}
	mapNodes := make([]mapNode, 0, len(nodes))
	for _, node := range nodes {
		var buf bytes.Buffer
		require.NoError(t, (&jsonpb.Marshaler{}).Marshal(&buf, node.Target))

		target := make(map[string]interface{})
		require.NoError(t, gojson.Unmarshal(buf.Bytes(), &target))

		mapNodes = append(mapNodes, mapNode{
			Target: target,
			State:  node.State.String(),
		})
	}

	out, err := yaml.Marshal(mapNodes)
	require.NoError(t, err)
	return string(out)
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
