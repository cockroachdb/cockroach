// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestIndexMutationKVOps is another poor-man's logictest that make
// assertions about the KV traces for indexes in various states of
// mutations.
func TestIndexMutationKVOps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	params, _ := createTestServerParams()
	// Decrease the adopt loop interval so that retries happen quickly.
	params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
	params.Knobs.SQLEvalContext = &eval.TestingKnobs{
		ForceProductionValues: true,
	}

	datadriven.Walk(t, datapathutils.TestDataPath(t, "index_mutations"), func(t *testing.T, path string) {
		s, sqlDB, kvDB := serverutils.StartServer(t, params)
		defer s.Stopper().Stop(ctx)
		defer lease.TestingDisableTableLeases()()
		_, err := sqlDB.Exec("CREATE DATABASE t; USE t")
		require.NoError(t, err)
		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "mutate-index":
				if len(td.CmdArgs) < 3 {
					td.Fatalf(t, "mutate-index requires at least an index name and a state")
				}
				tableName := td.CmdArgs[0].Key
				name := td.CmdArgs[1].Key
				stateStr := strings.ToUpper(td.CmdArgs[2].Key)
				state := descpb.DescriptorMutation_State(descpb.DescriptorMutation_State_value[stateStr])

				mutFn := func(idx *descpb.IndexDescriptor) error {
					if len(td.CmdArgs) < 4 {
						return nil
					}
					for _, arg := range td.CmdArgs[3:] {
						switch arg.Key {
						case "use_delete_preserving_encoding":
							b, err := strconv.ParseBool(arg.Vals[0])
							if err != nil {
								td.Fatalf(t, "use_delete_preserving_encoding expects a boolean: %s", err)
							}
							idx.UseDeletePreservingEncoding = b
						default:
							td.Fatalf(t, "unknown index option %q", arg.Key)
						}
					}
					return nil
				}
				codec := s.ExecutorConfig().(sql.ExecutorConfig).Codec
				tableDesc := desctestutils.TestingGetMutableExistingTableDescriptor(kvDB, codec, "t", tableName)
				err = mutateIndexByName(kvDB, codec, tableDesc, name, mutFn, state)
				require.NoError(t, err)
			case "statement":
				_, err := sqlDB.Exec(td.Input)
				require.NoError(t, err)
			case "kvtrace":
				_, err := sqlDB.Exec("SET TRACING=on,kv")
				require.NoError(t, err)
				_, err = sqlDB.Exec(td.Input)
				require.NoError(t, err)
				_, err = sqlDB.Exec("SET TRACING=off")
				require.NoError(t, err)
				return getKVTrace(t, sqlDB)
			default:
				td.Fatalf(t, "unknown directive: %s", td.Cmd)
			}
			return ""
		})
	})
}

func getKVTrace(t *testing.T, db *gosql.DB) string {
	// These are the same KVOps looked at by logictest.
	allowedKVOpTypes := []string{
		"CPut",
		"Put",
		"Del",
		"DelRange",
		"ClearRange",
		"Get",
		"Scan",
	}
	var sb strings.Builder
	sb.WriteString("SELECT message FROM [SHOW KV TRACE FOR SESSION] WHERE ")
	for i, op := range allowedKVOpTypes {
		if i != 0 {
			sb.WriteString("OR ")
		}
		sb.WriteString(fmt.Sprintf("message like '%s%%'", op))
	}
	traceMessagesQuery := sb.String()

	rows, err := db.Query(traceMessagesQuery)
	require.NoError(t, err)

	var trace strings.Builder
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		trace.WriteString(s)
		trace.WriteRune('\n')
	}
	require.NoError(t, rows.Err())
	return trace.String()
}
