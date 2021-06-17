// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdctest

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
)

// TestFeedFactory is an interface to create changefeeds.
type TestFeedFactory interface {
	// Feed creates a new TestFeed.
	Feed(create string, args ...interface{}) (TestFeed, error)
	// Server returns the raw underlying TestServer, if applicable.
	Server() serverutils.TestServerInterface
}

// TestFeedMessage represents one row update or resolved timestamp message from
// a changefeed.
type TestFeedMessage struct {
	Topic, Partition string
	Key, Value       []byte
	Resolved         []byte
}

func (m TestFeedMessage) String() string {
	if m.Resolved != nil {
		return string(m.Resolved)
	}
	return fmt.Sprintf(`%s: %s->%s`, m.Topic, m.Key, m.Value)
}

// TestFeed abstracts over reading from the various types of
// changefeed sinks.
//
// TODO(ssd): These functions need to take a context or otherwise
// allow us to time them out safely.
type TestFeed interface {
	// Partitions returns the domain of values that may be returned as a partition
	// by Next.
	Partitions() []string
	// Next returns the next message. Within a given topic+partition, the order is
	// preserved, but not otherwise. Either len(key) and len(value) will be
	// greater than zero (a row updated) or len(payload) will be (a resolved
	// timestamp).
	Next() (*TestFeedMessage, error)
	// Close shuts down the changefeed and releases resources.
	Close() error
}

// EnterpriseTestFeed augments TestFeed with additional methods applicable
// to enterprise feeds.
type EnterpriseTestFeed interface {
	// JobID returns the job id for this feed.
	JobID() jobspb.JobID
	// Pause stops the feed from running. Next will continue to return any results
	// that were queued before the pause, eventually blocking or erroring once
	// they've all been drained.
	Pause() error
	// Resume restarts the feed from the last changefeed-wide resolved timestamp.
	Resume() error
	// Details returns changefeed details for this feed.
	Details() (*jobspb.ChangefeedDetails, error)
}
