// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/redact"
)

func TestStructuredEventLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// We really need to have the logs go to files, so that -show-logs
	// does not break the "authlog" directives.
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	ctx := context.Background()

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	testStartTs := timeutil.Now()

	// Make a prepared statement that changes a cluster setting:
	// - we want a prepared statement to verify that the reporting of
	//   placeholders works during EXECUTE.
	// - we don't care about the particular cluster setting; any
	//   setting that does not otherwise impact the test's semantics
	//   will do.
	const setStmt = `SET CLUSTER SETTING "sql.defaults.default_int_size" = $1`
	if _, err := conn.ExecContext(ctx,
		`PREPARE a(INT) AS `+setStmt,
	); err != nil {
		t.Fatal(err)
	}
	// Run the prepared statement. This triggers a structured entry
	// for the cluster setting change.
	if _, err := conn.ExecContext(ctx, `EXECUTE a(8)`); err != nil {
		t.Fatal(err)
	}

	// Ensure that the entries hit the OS so they can be read back below.
	log.Flush()

	entries, err := log.FetchEntriesFromFiles(testStartTs.UnixNano(),
		math.MaxInt64, 10000, execLogRe, log.WithMarkedSensitiveData)
	if err != nil {
		t.Fatal(err)
	}

	foundEntry := false
	for _, e := range entries {
		if !strings.Contains(e.Message, "set_cluster_setting") {
			continue
		}
		foundEntry = true
		// TODO(knz): Remove this when crdb-v2 becomes the new format.
		e.Message = strings.TrimPrefix(e.Message, "Structured entry:")
		// crdb-v2 starts json with an equal sign.
		e.Message = strings.TrimPrefix(e.Message, "=")
		jsonPayload := []byte(e.Message)
		var ev eventpb.SetClusterSetting
		if err := json.Unmarshal(jsonPayload, &ev); err != nil {
			t.Errorf("unmarshalling %q: %v", e.Message, err)
		}
		if expected := redact.Sprint(setStmt); ev.Statement != expected {
			t.Errorf("wrong statement: expected %q, got %q", expected, ev.Statement)
		}
		if expected := []string{string(redact.Sprint("8"))}; !reflect.DeepEqual(expected, ev.PlaceholderValues) {
			t.Errorf("wrong placeholders: expected %+v, got %+v", expected, ev.PlaceholderValues)
		}
	}
	if !foundEntry {
		t.Error("structured entry for set_cluster_setting not found in log")
	}
}

var execLogRe = regexp.MustCompile(`event_log.go`)
