// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func Example_statement_diag() {
	// The times are reported in the local timezone. Fix it to UTC so the output
	// is always the same.
	time.Local = time.UTC

	c := newCLITest(cliTestParams{})
	defer c.cleanup()

	// First, set up some diagnostics state.
	commands := []string{
		`INSERT INTO system.statement_bundle_chunks(id, data) VALUES (1001, 'chunk1'), (1002, 'chunk2'), (1003, 'chunk3')`,

		`INSERT INTO system.statement_diagnostics(id, statement_fingerprint, statement, collected_at, bundle_chunks)
		 VALUES (10, 'SELECT _ FROM _',             'SELECT a FROM t',             '2010-01-02 03:04:05', ARRAY[1001]),
		        (20, 'SELECT _ FROM _ WHERE _ > _', 'SELECT a FROM t WHERE b > 1', '2010-01-02 03:04:06', ARRAY[1001,1002,1003])`,

		`INSERT INTO system.statement_diagnostics_requests(id, completed, statement_fingerprint, statement_diagnostics_id, requested_at)
		 VALUES (1, TRUE, 'SELECT _ FROM _', 10, '2010-01-02 03:04:00'),
		        (2, TRUE, 'SELECT _ FROM _ WHERE _ > _', 20, '2010-01-02 03:04:02'),
						(3, FALSE, 'SELECT _ + _', NULL, '2010-01-02 03:04:10')`,
	}

	for _, cmd := range commands {
		_, err := c.RunWithCaptureArgs([]string{"sql", "-e", cmd})
		if err != nil {
			log.Fatalf(context.Background(), "Couldn't execute sql: %s", err)
		}
	}
	c.RunWithArgs([]string{"statement-diag", "list"})
	c.RunWithArgs([]string{"statement-diag", "download", "13", "foo.zip"})
	tmpfile, err := ioutil.TempFile("", "bundle-*.zip")
	if err != nil {
		log.Fatalf(context.Background(), "Couldn't execute sql: %s", err)
	}
	bundleFile := tmpfile.Name()
	_ = tmpfile.Close()
	defer func() { _ = os.Remove(bundleFile) }()

	fmt.Printf("statement-diag download 20 tempfile.zip\n")
	_, err = c.RunWithCaptureArgs([]string{"statement-diag", "download", "20", bundleFile})
	if err != nil {
		log.Fatalf(context.Background(), "Error downloading bundle: %s", err)
	}
	data, err := ioutil.ReadFile(bundleFile)
	if err != nil {
		log.Fatalf(context.Background(), "Error reading bundle: %s", err)
	}
	fmt.Printf("bundle data: %s\n", data)

	c.RunWithArgs([]string{"statement-diag", "delete", "10"})
	c.RunWithArgs([]string{"statement-diag", "list"})
	c.RunWithArgs([]string{"statement-diag", "cancel", "13"})
	c.RunWithArgs([]string{"statement-diag", "cancel", "3"})
	c.RunWithArgs([]string{"statement-diag", "list"})
	c.RunWithArgs([]string{"statement-diag", "delete", "20"})
	c.RunWithArgs([]string{"statement-diag", "list"})

	// Output:
	// statement-diag list
	// Statement diagnostics bundles:
	//   ID  Collection time          Statement
	//   20  2010-01-02 03:04:06 UTC  SELECT _ FROM _ WHERE _ > _
	//   10  2010-01-02 03:04:05 UTC  SELECT _ FROM _
	//
	// Outstanding activation requests:
	//   ID  Activation time          Statement
	//   3   2010-01-02 03:04:10 UTC  SELECT _ + _
	// statement-diag download 13 foo.zip
	// ERROR: no statement diagnostics bundle with ID 13
	// statement-diag download 20 tempfile.zip
	// bundle data: chunk1chunk2chunk3
	// statement-diag delete 10
	// statement-diag list
	// Statement diagnostics bundles:
	//   ID  Collection time          Statement
	//   20  2010-01-02 03:04:06 UTC  SELECT _ FROM _ WHERE _ > _
	//
	// Outstanding activation requests:
	//   ID  Activation time          Statement
	//   3   2010-01-02 03:04:10 UTC  SELECT _ + _
	// statement-diag cancel 13
	// ERROR: no outstanding activation requests with ID 13
	// statement-diag cancel 3
	// statement-diag list
	// Statement diagnostics bundles:
	//   ID  Collection time          Statement
	//   20  2010-01-02 03:04:06 UTC  SELECT _ FROM _ WHERE _ > _
	//
	// No outstanding activation requests.
	// statement-diag delete 20
	// statement-diag list
	// No statement diagnostics bundles available.
	// No outstanding activation requests.
}
