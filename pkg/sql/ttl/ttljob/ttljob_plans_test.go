// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ttljob

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catsessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/ttl/ttlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestQueryPlansDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	runner := sqlutils.MakeSQLRunner(sqlDB)
	runner.Exec(t, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false;")
	db := s.InternalDB().(*sql.InternalDB)
	cutoff, err := tree.MakeDTimestamp(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), time.Second)
	require.NoError(t, err)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	sd := sql.NewInternalSessionData(ctx, s.ClusterSettings(), "test" /* opName */)
	sds := sessiondata.NewStack(sd)
	dsdp := catsessiondata.NewDescriptorSessionDataStackProvider(sds)
	descsCol := execCfg.CollectionFactory.NewCollection(ctx, descs.WithDescriptorSessionDataProvider(dsdp))

	getExplainPlan := func(query string, overrides string) string {
		rows := runner.Query(t, fmt.Sprintf(`SELECT crdb_internal.execute_internally('EXPLAIN %s', '%s');`, query, overrides))
		var sb strings.Builder
		for i := 0; rows.Next(); i++ {
			// Omit first three rows that are of the form:
			//  distribution: local
			//  vectorized: true
			//
			if i >= 3 {
				if i > 3 {
					sb.WriteString("\n")
				}
				var explainRow string
				require.NoError(t, rows.Scan(&explainRow))
				sb.WriteString(explainRow)
			}
		}
		require.NoError(t, rows.Err())
		return sb.String()
	}

	var selectBounds QueryBounds
	var deleteIDs []string

	datadriven.Walk(t, datapathutils.TestDataPath(t, "ttljob_plans"), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "setup":
				// 'setup' command allows for executing an arbitrary single
				// query (e.g. table creation).
				runner.Exec(t, d.Input)
				return ""

			case "select-bounds":
				// 'select-bounds' command expects two lines that specify equal
				// number of bounds. The argument of the 'select-bounds' command
				// specifies the type of datums.
				var pkType string
				d.ScanArgs(t, "pkType", &pkType)
				if strings.ToUpper(pkType) != "INT" {
					t.Fatalf("only INT PRIMARY KEY is currently supported")
				}
				lines := strings.Split(d.Input, "\n")
				if len(lines) != 2 {
					t.Fatalf("expected two lines (for Stand and End bounds, respectively), found %d lines", len(lines))
				}
				starts, ends := strings.Split(lines[0], ","), strings.Split(lines[1], ",")
				if len(starts) != len(ends) {
					t.Fatalf("expected equal number of comma-separated datums for Stand and End bounds, "+
						"found %d and %d", len(starts), len(ends))
				}
				selectBounds = QueryBounds{}
				for i := range starts {
					bound, err := strconv.Atoi(starts[i])
					if err != nil {
						t.Fatal(err)
					}
					selectBounds.Start = append(selectBounds.Start, tree.NewDInt(tree.DInt(bound)))
					bound, err = strconv.Atoi(ends[i])
					if err != nil {
						t.Fatal(err)
					}
					selectBounds.End = append(selectBounds.End, tree.NewDInt(tree.DInt(bound)))
				}
				return ""

			case "delete-ids":
				deleteIDs = strings.Split(d.Input, ",")
				return ""

			case "check-query", "check-plan":
				// 'check-query' and 'check-plan' commands perform the
				// verification that the query itself and its plan produced by
				// the TTL query builders are as expected.
				var builder, tableName, overrides string
				d.ScanArgs(t, "builder", &builder)
				d.ScanArgs(t, "table", &tableName)
				d.ScanArgs(t, "overrides", &overrides)

				var tableID int64
				row := runner.QueryRow(t, fmt.Sprintf("SELECT '%s'::REGCLASS::OID;", tableName))
				row.Scan(&tableID)
				relationName, _, pkColNames, _, pkColDirs, _, _, err := getTableInfo(
					ctx, db, descsCol, descpb.ID(tableID),
				)
				require.NoError(t, err)

				switch strings.ToLower(builder) {
				case "select":
					selectBuilder := MakeSelectQueryBuilder(
						SelectQueryParams{
							RelationName:    relationName,
							PKColNames:      pkColNames,
							PKColDirs:       pkColDirs,
							Bounds:          selectBounds,
							SelectBatchSize: ttlbase.DefaultSelectBatchSizeValue,
							TTLExpr:         catpb.DefaultTTLExpirationExpr,
						},
						cutoff.UTC(),
					)
					// TODO(yuzefovich): this function might not work if
					// multiple bounds are specified.
					replacePlaceholders := func(query string) string {
						query = strings.ReplaceAll(query, "'", "''")
						query = strings.ReplaceAll(query, "$1", "'"+cutoff.String()+"'")
						// Confusingly, all End bounds go before all Start
						// bounds when assigning placeholder indices in the
						// SelectQueryBuilder.
						for i, d := range selectBounds.End {
							query = strings.ReplaceAll(query, fmt.Sprintf("$%d", i+2), d.String())
						}
						for i, d := range selectBounds.Start {
							query = strings.ReplaceAll(query, fmt.Sprintf("$%d", i+2+len(selectBounds.End)), d.String())
						}
						return query
					}
					selectQuery := replacePlaceholders(selectBuilder.buildQuery())
					if d.Cmd == "check-query" {
						return selectQuery
					}
					return getExplainPlan(selectQuery, overrides)

				case "delete":
					deleteBuilder := MakeDeleteQueryBuilder(
						DeleteQueryParams{
							RelationName: relationName,
							PKColNames:   pkColNames,
							TTLExpr:      catpb.DefaultTTLExpirationExpr,
						},
						cutoff.UTC(),
					)
					replacePlaceholders := func(query string) string {
						query = strings.ReplaceAll(query, "'", "''")
						for i := len(deleteIDs) - 1; i >= 0; i-- {
							query = strings.ReplaceAll(query, fmt.Sprintf("$%d", i+2), deleteIDs[i])
						}
						query = strings.ReplaceAll(query, "$1", "'"+cutoff.String()+"'")
						return query
					}
					deleteQuery := replacePlaceholders(deleteBuilder.buildQuery(len(deleteIDs)))
					if d.Cmd == "check-query" {
						return deleteQuery
					}
					return getExplainPlan(deleteQuery, overrides)

				default:
					t.Fatalf("only select and delete builder options are supported")
					return ""
				}

			default:
				t.Fatalf("unknown command %q", d.Cmd)
				return ""
			}
		})
	})
}
