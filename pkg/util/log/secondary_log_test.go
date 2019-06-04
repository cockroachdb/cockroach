// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package log

import (
	"context"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtags"
)

func TestSecondaryLog(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)
	setFlags()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Make a new logger, in the same directory.
	l := NewSecondaryLogger(ctx, &logging.logDir, "woo", true, false)

	// Interleave some messages.
	Infof(context.Background(), "test1")
	ctx = logtags.AddTag(ctx, "hello", "world")
	l.Logf(ctx, "story time")
	Infof(context.Background(), "test2")

	// Make sure the content made it to disk.
	Flush()

	// Check that the messages indeed made it to different files.

	contents, err := ioutil.ReadFile(logging.file.(*syncBuffer).file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), "test1") || !strings.Contains(string(contents), "test2") {
		t.Errorf("log does not contain error text\n%s", contents)
	}
	if strings.Contains(string(contents), "world") {
		t.Errorf("secondary log spilled into primary\n%s", contents)
	}

	contents, err = ioutil.ReadFile(l.logger.file.(*syncBuffer).file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), "hello") ||
		!strings.Contains(string(contents), "world") ||
		!strings.Contains(string(contents), "story time") {
		t.Errorf("secondary log does not contain text\n%s", contents)
	}
	if strings.Contains(string(contents), "test1") {
		t.Errorf("primary log spilled into secondary\n%s", contents)
	}

}
