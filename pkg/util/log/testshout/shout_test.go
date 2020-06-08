// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testshout

import (
	"context"
	"flag"
	"os"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logflags"
)

// Example_shout_before_log verifies that Shout output emitted after
// the log flags were set, but before the first log message was
// output, properly appears on stderr.
//
// This test needs to occur in its own test package where there is no
// other activity on the log flags, and no other log activity,
// otherwise the test's behavior will break on `make stress`.
func Example_shout_before_log() {
	origStderr := log.OrigStderr
	log.OrigStderr = os.Stdout
	defer func() { log.OrigStderr = origStderr }()

	if err := flag.Set(logflags.LogToStderrName, "WARNING"); err != nil {
		panic(err)
	}

	log.Shout(context.Background(), log.Severity_INFO, "hello world")

	// output:
	// *
	// * INFO: hello world
	// *
}
