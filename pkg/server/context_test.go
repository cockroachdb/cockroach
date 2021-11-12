// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/stretchr/testify/require"
)

// TestContextTags checks that the server identifiers (either node ID
// or SQL instance ID containers) are present in the context tags of log
// messages produced by basic server operation.
func TestContextTags(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.Scope(t)
	defer sc.Close(t)

	// Set up a logging configuration that will save the log messages as JSON.
	// Enable logging channels.
	log.TestingResetActive()
	cfg := logconfig.DefaultConfig()
	// Make a sink for all messages.
	jf := "json"
	cfg.Sinks.FileGroups = map[string]*logconfig.FileSinkConfig{
		"all-json": {
			FileDefaults: logconfig.FileDefaults{
				CommonSinkConfig: logconfig.CommonSinkConfig{Format: &jf},
			},
			Channels: logconfig.SelectChannels(logconfig.AllChannels()...),
		}}
	dir := sc.GetDirectory()
	require.NoError(t, cfg.Validate(&dir))
	cleanup, err := log.ApplyConfig(cfg)
	require.NoError(t, err)
	defer cleanup()

	// Create and initialize a cluster.
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.Background())

	// Perform some SQL activity.
	db := tc.ServerConn(0)
	_, err = db.Exec("CREATE DATABASE test")
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE test.foo (id INT PRIMARY KEY, val STRING)")
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		_, err = db.Exec("INSERT INTO test.foo VALUES($1, $2)", i, "test")
		require.NoError(t, err)
	}
	_, err = db.Exec("DROP DATABASE test CASCADE")
	require.NoError(t, err)

	// Flush the entries so far.
	log.Flush()

	// Now read the log entries from the files created above.
	finfos, err := log.ListLogFiles()
	require.NoError(t, err)
	var myFiles []string
	for _, fi := range finfos {
		if strings.Contains(fi.Name, "all-json") {
			myFiles = append(myFiles, fi.Name)
		}
	}
	require.GreaterOrEqual(t, len(myFiles), 1)

	for _, fname := range myFiles {
		// Use a closure to ensure that the deferred Close runs on each
		// iteration.
		func() {
			reader, err := log.GetLogReader(fname)
			require.NoError(t, err)
			defer reader.Close()

			allJSON, err := ioutil.ReadAll(reader)
			require.NoError(t, err)

			lines := bytes.Split(allJSON, []byte("\n"))
			for _, line := range lines {
				if len(line) == 0 {
					// Empty line. Ignore.
					continue
				}
				// t.Logf("log line: %q", line)
				var entry struct {
					Severity string
					File     string
					Line     int
					Message  string
					Tags     map[string]string
					Channel  string
				}
				require.NoError(t, json.Unmarshal(line, &entry))

				// The "config" entries at the start of a file never contain
				// server identifiers.
				if _, config := entry.Tags["config"]; config && len(entry.Tags) == 1 {
					continue
				}
				// Entries produced by the code in the testcluster/testserver
				// packages are coordination only and not expected to be
				// specific to a particular server.
				if strings.HasPrefix(entry.File, "testutils/testcluster") {
					continue
				}

				// We seem to have a bug in Pebble whereby the configured
				// context tags are not properly propagated for a particular
				// message.
				// See: https://github.com/cockroachdb/cockroach/issues/72683
				if len(entry.Tags) == 0 && entry.Channel == "DEV" &&
					strings.Contains(entry.Message, "ingest failed to remove original file: invalid argument") {
					continue
				}

				// Now, our required condition for the test to pass:
				_, hasNode := entry.Tags["n"]
				_, hasSQLi := entry.Tags["sqli"]
				if !hasNode && !hasSQLi {
					t.Errorf("missing server identifier in entry: %s@%s %s:%d %v %s",
						entry.Severity,
						entry.Channel,
						entry.File,
						entry.Line,
						entry.Tags,
						entry.Message)
				}
			}
		}()
	}
}
