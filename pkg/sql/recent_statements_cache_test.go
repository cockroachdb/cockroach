// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestRecentStatementsCacheBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var cache *RecentStatementsCache

	datadriven.Walk(t, testutils.TestDataPath(t, "recent_statements_cache"), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			ctx := context.Background()
			switch d.Cmd {
			case "init":
				var capacity int
				d.ScanArgs(t, "capacity", &capacity)

				st := &cluster.Settings{}
				cache = NewRecentStatementsCache(st)

				RecentStatementsCacheCapacity.Override(ctx, &st.SV, int64(capacity))

				return fmt.Sprintf("cache_size: %d", cache.len())
			case "addStatement":
				var sessionIDStr string
				var stmtIDStr string
				d.ScanArgs(t, "sessionID", &sessionIDStr)
				d.ScanArgs(t, "stmtID", &stmtIDStr)
				sessionIDInt, err := uint128.FromString(sessionIDStr)
				stmtIDInt, err := uint128.FromString(stmtIDStr)
				require.NoError(t, err)

				sessionID := clusterunique.ID{Uint128: sessionIDInt}
				stmtID := clusterunique.ID{Uint128: stmtIDInt}

				qm := queryMeta{
					rawStmt: "test query",
				}
				cache.add(sessionID, stmtID, &qm)
				require.NoError(t, err)

				return fmt.Sprintf("cache_size: %d", cache.len())
			case "clear":
				cache.clear()
				return "ok"
			case "show":
				var sessionIDStr string
				d.ScanArgs(t, "sessionID", &sessionIDStr)
				sessionIDInt, err := uint128.FromString(sessionIDStr)
				require.NoError(t, err)
				sessionID := clusterunique.ID{Uint128: sessionIDInt}

				var result []string

				stmtIDToQm := cache.getRecentStatementsForSession(sessionID)
				for stmtID, query := range stmtIDToQm {
					result = append(result,
						fmt.Sprintf(`sessionID: %s, queryID: %s, statement: %s`,
							sessionIDStr, stmtID.String(), query.rawStmt))
				}
				if len(result) == 0 {
					return "empty"
				}
				return strings.Join(result, "\n")
			}
			return ""
		})
	})

}
