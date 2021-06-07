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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
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
			case "create-table", "create-view", "create-type", "create-sequence":
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
				default:
					t.Fatal("not a CREATE TABLE/SEQUENCE/VIEW statement")
				}
				tdb.Exec(t, d.Input)

				if len(tableName) > 0 {
					var tableID descpb.ID
					tdb.QueryRow(t, `SELECT $1::regclass::int`, tableName).Scan(&tableID)
					if tableID == 0 {
						t.Fatalf("failed to read ID of new table %s", tableName)
					}
					t.Logf("created relation with id %d", tableID)
				}

				return ""
			case "build":
				deps, cleanup := newTestingBuilderDeps(s)
				defer cleanup()

				stmts, err := parser.Parse(d.Input)
				require.NoError(t, err)
				var outputNodes []*scpb.Node
				for i := range stmts {
					outputNodes, err = scbuild.Build(ctx, stmts[i].AST, *deps, outputNodes)
					require.NoError(t, err)
				}

				return marshalNodes(t, outputNodes)
			case "unimplemented":
				deps, cleanup := newTestingBuilderDeps(s)
				defer cleanup()

				stmts, err := parser.Parse(d.Input)
				require.NoError(t, err)
				require.Len(t, stmts, 1)

				stmt := stmts[0]
				alter, ok := stmt.AST.(*tree.AlterTable)
				require.Truef(t, ok, "not an ALTER TABLE statement: %s", stmt.SQL)
				_, err = scbuild.Build(ctx, alter, *deps, nil)
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

func newTestingBuilderDeps(s serverutils.TestServerInterface) (*scbuild.Dependencies, func()) {
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
		scbuild.AuthorizationAccessor
		Descriptors() *descs.Collection
	})
	buildDeps := scbuild.Dependencies{
		Res:          planner,
		SemaCtx:      planner.SemaCtx(),
		EvalCtx:      planner.EvalContext(),
		Descs:        planner.Descriptors(),
		AuthAccessor: planner,
	}
	return &buildDeps, cleanup
}
