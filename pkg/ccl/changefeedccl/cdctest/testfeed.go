// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cdctest

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// TestFeedFactory is an interface to create changefeeds.
type TestFeedFactory interface {
	// Feed creates a new TestFeed.
	Feed(create string, args ...interface{}) (TestFeed, error)

	// AsUser connects to the database as the specified user, calls fn() with the
	// user's connection, then goes back to using the same root connection. Will
	// return an error if the initial connection to the database fails, but fn is
	// responsible for failing the test on other errors.
	AsUser(user string, fn func(runner *sqlutils.SQLRunner)) error
}

// TestFeedMessage represents one row update or resolved timestamp message from
// a changefeed.
type TestFeedMessage struct {
	Topic, Partition string
	Key, Value       []byte
	Resolved         []byte

	// RawMessage is the sink-specific message type.
	RawMessage interface{}
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
	// WaitForState waits for the provided func to return true, or returns an error.
	WaitForState(func(s jobs.State) bool) error
	// WaitDurationForState waits for a specified time for the provided func to return true, or returns an error.
	WaitDurationForState(dur time.Duration, statusPred func(state jobs.State) bool) error
	// FetchTerminalJobErr retrieves the error message from changefeed job.
	FetchTerminalJobErr() error
	// FetchStatus retrieves running status from changefeed job.
	FetchStatusMessage() (string, error)
	// Details returns changefeed details for this feed.
	Details() (*jobspb.ChangefeedDetails, error)
	// Progress returns the changefeed progress for this feed.
	Progress() (*jobspb.ChangefeedProgress, error)
	// HighWaterMark returns feed highwatermark.
	HighWaterMark() (hlc.Timestamp, error)
	// TickHighWaterMark waits until job highwatermark progresses beyond specified threshold.
	TickHighWaterMark(minHWM hlc.Timestamp) error
}
