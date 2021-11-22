// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package svstorage

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/singleversion/svtestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestTableDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(sqlDB)
	setup := func(t *testing.T) (tableName string, tableID descpb.ID) {
		return svtestutils.SetUpTestingTable(t, tdb)
	}

	datadriven.Walk(t, testutils.TestDataPath(t), func(t *testing.T, path string) {
		tableName, tableID := setup(t)
		tc := tableCodec{
			codec:   keys.SystemSQLCodec,
			tableID: tableID,
		}
		parseRowArgs := func(t require.TestingT, args []datadriven.CmdArg) []Row {
			var a Action
			var ids catalog.DescriptorIDSet
			var sessionID sqlliveness.SessionID
			for _, arg := range args {
				switch arg.Key {
				case "action":
					require.Len(t, arg.Vals, 1)
					var err error
					a, err = decodeAction(arg.Vals[0])
					require.NoError(t, err)
				case "id":
					for _, v := range arg.Vals {
						parsed, err := strconv.Atoi(v)
						require.NoError(t, err)
						ids.Add(descpb.ID(parsed))
					}
				case "session":
					require.Len(t, arg.Vals, 1)
					sessionID = sqlliveness.SessionID(arg.Vals[0])
				default:
					t.Errorf("unknown arg %s", arg.Key)
				}
			}
			return makeRows(a, ids, sessionID)
		}
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			rt := testDataT{t: t, d: d}
			switch d.Cmd {
			case "put":
				rows := parseRowArgs(rt, d.CmdArgs)
				require.NoError(rt, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					return putRows(ctx, txn, tc, rows)
				}))
				return ""
			case "del":
				rows := parseRowArgs(rt, d.CmdArgs)
				require.NoError(rt, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					return deleteRows(ctx, txn, tc, rows)
				}))
				return ""
			case "scan":
				prefixes := parseRowArgs(rt, d.CmdArgs)
				var rowMat [][]string
				require.NoError(rt, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					rowMat = nil
					return scan(ctx, txn, tc, prefixes, func(r Row) {
						rowMat = append(rowMat, []string{
							r.Action.String(),
							fmt.Sprintf("%d", r.Descriptor),
							string(r.Session),
						})
					})
				}))
				return sqlutils.MatrixToStr(rowMat)
			case "sql":
				q := os.Expand(d.Input, func(s string) string {
					if s == "TABLE" {
						return tableName
					}
					return s
				})
				rows, err := sqlDB.Query(q)
				require.NoError(rt, err)
				rowMat, err := sqlutils.RowsToStrMatrix(rows)
				require.NoError(rt, err)
				return sqlutils.MatrixToStr(rowMat)
			default:
				d.Fatalf(t, "unknown command %s", d.Cmd)
				return ""
			}
		})
	})
}

type testDataT struct {
	t *testing.T
	d *datadriven.TestData
}

func (t testDataT) Errorf(format string, args ...interface{}) {
	t.t.Helper()
	t.d.Fatalf(t.t, format, args...)
}

func (t testDataT) FailNow() {
	t.d.Fatalf(t.t, "")
}

var _ require.TestingT = (*testDataT)(nil)
