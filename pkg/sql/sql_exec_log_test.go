// Copyright 2023 The Cockroach Authors.
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
	"encoding/json"
	"math"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtestutils"
	"github.com/stretchr/testify/require"
)

// Begin statements are executed specially by the connExecutor.
// Verify that the query age is properly recorded for begin statements.
// Tests fix for #114571.
func TestBeginStatementLogQueryAge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	cleanup := logtestutils.InstallLogFileSink(sc, t, logpb.Channel_SQL_EXEC)
	defer cleanup()

	ctx := context.Background()

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			EventLog: &EventLogTestingKnobs{
				// The sampling checks below need to have a deterministic
				// number of statements run by internal executor.
				SyncWrites: true,
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	// This connection will be used to set cluster settings.
	setupConn := sqlutils.MakeSQLRunner(s.ApplicationLayer().SQLConn(t))
	setupConn.Exec(t, "SET application_name = 'setupConnApp'")
	// Verify no logs appear when sql exec log is off.
	entries, err := log.FetchEntriesFromFiles(
		0,
		math.MaxInt,
		100,
		regexp.MustCompile(`"EventType":"query_execute"`),
		log.WithMarkedSensitiveData,
	)
	require.NoError(t, err)
	require.Empty(t, entries)

	setupConn.Exec(t, "SET CLUSTER SETTING sql.trace.log_statement_execute = true")

	// In #114571, it was found that the query age was not properly recorded for BEGIN statements.
	// This lead to the query age being a noticeably large value when it was the first statement
	// in a session, as the session query received time was not yet set. We can easily verify
	// if this is fixed by executing BEGIN as the first stmt in the connection below, and
	// observing the recorded query age.
	conn := sqlutils.MakeSQLRunner(s.ApplicationLayer().SQLConn(t))
	queries := []string{"BEGIN", "SELECT 1", "COMMIT"}
	for _, q := range queries {
		conn.Exec(t, q)
	}
	log.FlushFiles()

	entries, err = log.FetchEntriesFromFiles(
		0,
		math.MaxInt,
		100,
		regexp.MustCompile(`"EventType":"query_execute"`),
		log.WithMarkedSensitiveData,
	)
	require.NoError(t, err)
	require.NotEmpty(t, entries)

	// FetchEntriesFromFiles delivers entries in reverse order.
	entries = entries[:len(entries)-1]
	require.GreaterOrEqual(t, len(entries), len(queries))
	queryIdx := 0
	for i := len(entries) - 1; i >= 0; i-- {
		var qe eventpb.QueryExecute
		if err := json.Unmarshal([]byte(entries[i].Message), &qe); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(qe.ApplicationName, "setupConnApp") || qe.ExecMode == executorTypeInternal.logLabel() {
			continue
		}
		// Verify expected log.
		expectedStmt := queries[queryIdx]
		require.Containsf(t, qe.Statement.StripMarkers(), expectedStmt, "entry: %v", entries[i])
		queryIdx++

		// Query age should be less than 3 seconds.
		require.Lessf(t, qe.Age, float32(3000), "entry: %v", entries[i])
	}
}
