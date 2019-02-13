// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdctest

import (
	"bufio"
	"context"
	gosql "database/sql"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
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

// TestFeed abstracts over reading from the various types of changefeed sinks.
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

type sinklessFeedFactory struct {
	s    serverutils.TestServerInterface
	sink url.URL
}

// MakeSinklessFeedFactory returns a TestFeedFactory implementation using the
// `experimental-sql` sink.
func MakeSinklessFeedFactory(s serverutils.TestServerInterface, sink url.URL) TestFeedFactory {
	return &sinklessFeedFactory{s: s, sink: sink}
}

// Feed implements the TestFeedFactory interface
func (f *sinklessFeedFactory) Feed(create string, args ...interface{}) (TestFeed, error) {
	sink := f.sink
	q := sink.Query()
	q.Add(`results_buffer_size`, `1`)
	sink.RawQuery = q.Encode()
	sink.Path = `d`
	// Use pgx directly instead of database/sql so we can close the conn
	// (instead of returning it to the pool).
	pgxConfig, err := pgx.ParseConnectionString(sink.String())
	if err != nil {
		return nil, err
	}
	s := &sinklessFeed{seen: make(map[string]struct{})}
	s.conn, err = pgx.Connect(pgxConfig)
	if err != nil {
		return nil, err
	}

	// The syntax for a sinkless changefeed is `EXPERIMENTAL CHANGEFEED FOR ...`
	// but it's convenient to accept the `CREATE CHANGEFEED` syntax from the
	// test, so we can keep the current abstraction of running each test over
	// both types. This bit turns what we received into the real sinkless
	// syntax.
	create = strings.Replace(create, `CREATE CHANGEFEED`, `EXPERIMENTAL CHANGEFEED`, 1)

	s.rows, err = s.conn.Query(create, args...)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Server implements the TestFeedFactory interface.
func (f *sinklessFeedFactory) Server() serverutils.TestServerInterface {
	return f.s
}

// sinklessFeed is an implementation of the `TestFeed` interface for a
// "sinkless" (results returned over pgwire) feed.
type sinklessFeed struct {
	conn *pgx.Conn
	rows *pgx.Rows
	seen map[string]struct{}
}

// Partitions implements the TestFeed interface.
func (c *sinklessFeed) Partitions() []string { return []string{`sinkless`} }

// Next implements the TestFeed interface.
func (c *sinklessFeed) Next() (*TestFeedMessage, error) {
	m := &TestFeedMessage{Partition: `sinkless`}
	for {
		if !c.rows.Next() {
			return nil, c.rows.Err()
		}
		var maybeTopic gosql.NullString
		if err := c.rows.Scan(&maybeTopic, &m.Key, &m.Value); err != nil {
			return nil, err
		}
		if len(maybeTopic.String) > 0 {
			m.Topic = maybeTopic.String
			// TODO(dan): This skips duplicates, since they're allowed by the
			// semantics of our changefeeds. Now that we're switching to RangeFeed,
			// this can actually happen (usually because of splits) and cause flakes.
			// However, we really should be de-duping key+ts, this is too coarse.
			// Fixme.
			seenKey := m.Topic + m.Partition + string(m.Key) + string(m.Value)
			if _, ok := c.seen[seenKey]; ok {
				continue
			}
			c.seen[seenKey] = struct{}{}
			return m, nil
		}
		m.Resolved = m.Value
		m.Key, m.Value = nil, nil
		return m, nil
	}
}

// Close implements the TestFeed interface.
func (c *sinklessFeed) Close() error {
	return c.conn.Close()
}

type jobFeed struct {
	db      *gosql.DB
	flushCh chan struct{}

	JobID  int64
	jobErr error
}

func (f *jobFeed) fetchJobError() error {
	// To avoid busy waiting, we wait for the AfterFlushHook (which is called
	// after results are flushed to a sink) in between polls. It is required
	// that this is hooked up to `flushCh`, which is usually handled by the
	// `enterpriseTest` helper.
	//
	// The trickiest bit is handling errors in the changefeed. The tests want to
	// eventually notice them, but want to return all generated results before
	// giving up and returning the error. This is accomplished by checking the
	// job error immediately before every poll. If it's set, the error is
	// stashed and one more poll's result set is paged through, before finally
	// returning the error. If we're careful to run the last poll after getting
	// the error, then it's guaranteed to contain everything flushed by the
	// changefeed before it shut down.
	if f.jobErr != nil {
		return f.jobErr
	}

	// We're not guaranteed to get a flush notification if the feed exits,
	// so bound how long we wait.
	select {
	case <-f.flushCh:
	case <-time.After(30 * time.Millisecond):
	}

	// If the error was set, save it, but do one more poll as described
	// above.
	var errorStr gosql.NullString
	if err := f.db.QueryRow(
		`SELECT error FROM [SHOW JOBS] WHERE job_id=$1`, f.JobID,
	).Scan(&errorStr); err != nil {
		return err
	}
	if len(errorStr.String) > 0 {
		f.jobErr = errors.New(errorStr.String)
	}
	return nil
}

type tableFeedFactory struct {
	s       serverutils.TestServerInterface
	db      *gosql.DB
	flushCh chan struct{}
	sink    url.URL
}

// MakeTableFeedFactory returns a TestFeedFactory implementation using the
// `experimental-sql` sink.
func MakeTableFeedFactory(
	s serverutils.TestServerInterface, db *gosql.DB, flushCh chan struct{}, sink url.URL,
) TestFeedFactory {
	return &tableFeedFactory{s: s, db: db, flushCh: flushCh, sink: sink}
}

// Feed implements the TestFeedFactory interface
func (f *tableFeedFactory) Feed(create string, args ...interface{}) (TestFeed, error) {
	sink := f.sink
	sink.Path = fmt.Sprintf(`table_%d`, timeutil.Now().UnixNano())

	db, err := gosql.Open("postgres", sink.String())
	if err != nil {
		return nil, err
	}

	sink.Scheme = `experimental-sql`
	c := &TableFeed{
		jobFeed: jobFeed{
			db:      db,
			flushCh: f.flushCh,
		},
		sinkURI: sink.String(),
		seen:    make(map[string]struct{}),
	}
	if _, err := c.db.Exec(`CREATE DATABASE ` + sink.Path); err != nil {
		return nil, err
	}

	parsed, err := parser.ParseOne(create)
	if err != nil {
		return nil, err
	}
	createStmt := parsed.AST.(*tree.CreateChangefeed)
	if createStmt.SinkURI != nil {
		return nil, errors.Errorf(
			`unexpected sink provided: "INTO %s"`, tree.AsString(createStmt.SinkURI))
	}
	createStmt.SinkURI = tree.NewStrVal(c.sinkURI)

	if err := f.db.QueryRow(createStmt.String(), args...).Scan(&c.JobID); err != nil {
		return nil, err
	}
	return c, nil
}

// Server implements the TestFeedFactory interface.
func (f *tableFeedFactory) Server() serverutils.TestServerInterface {
	return f.s
}

// TableFeed is a TestFeed implementation using the `experimental-sql` sink.
type TableFeed struct {
	jobFeed
	sinkURI string

	rows *gosql.Rows
	seen map[string]struct{}
}

// Partitions implements the TestFeed interface.
func (c *TableFeed) Partitions() []string {
	// The sqlSink hardcodes these.
	return []string{`0`, `1`, `2`}
}

// Next implements the TestFeed interface.
func (c *TableFeed) Next() (*TestFeedMessage, error) {
	// sinkSink writes all changes to a table with primary key of topic,
	// partition, message_id. To simulate the semantics of kafka, message_ids
	// are only comparable within a given (topic, partition). Internally the
	// message ids are generated as a 64 bit int with a timestamp in bits 1-49
	// and a hash of the partition in 50-64. This TableFeed.Next function works
	// by repeatedly fetching and deleting all rows in the table. Then it pages
	// through the results until they are empty and repeats.
	for {
		if c.rows != nil && c.rows.Next() {
			m := &TestFeedMessage{}
			var msgID int64
			if err := c.rows.Scan(
				&m.Topic, &m.Partition, &msgID, &m.Key, &m.Value, &m.Resolved,
			); err != nil {
				return nil, err
			}

			// Scan turns NULL bytes columns into a 0-length, non-nil byte
			// array, which is pretty unexpected. Nil them out before returning.
			// Either key+value or payload will be set, but not both.
			if len(m.Key) > 0 || len(m.Value) > 0 {
				// TODO(dan): This skips duplicates, since they're allowed by the
				// semantics of our changefeeds. Now that we're switching to RangeFeed,
				// this can actually happen (usually because of splits) and cause
				// flakes. However, we really should be de-duping key+ts, this is too
				// coarse. Fixme.
				seenKey := m.Topic + m.Partition + string(m.Key) + string(m.Value)
				if _, ok := c.seen[seenKey]; ok {
					continue
				}
				c.seen[seenKey] = struct{}{}

				m.Resolved = nil
				return m, nil
			}
			m.Key, m.Value = nil, nil
			return m, nil
		}
		if c.rows != nil {
			if err := c.rows.Close(); err != nil {
				return nil, err
			}
			c.rows = nil
		}

		if err := c.fetchJobError(); err != nil {
			return nil, c.jobErr
		}

		// TODO(dan): It's a bummer that this mutates the sqlsink table. I
		// originally tried paging through message_id by repeatedly generating a
		// new high-water with GenerateUniqueInt, but this was racy with rows
		// being flushed out by the sink. An alternative is to steal the nanos
		// part from `high_water_timestamp` in `crdb_internal.jobs` and run it
		// through `builtins.GenerateUniqueID`, but that would mean we're only
		// ever running tests on rows that have gotten a resolved timestamp,
		// which seems limiting.
		var err error
		c.rows, err = c.db.Query(
			`SELECT * FROM [DELETE FROM sqlsink RETURNING *] ORDER BY topic, partition, message_id`)
		if err != nil {
			return nil, err
		}
	}
}

// Close implements the TestFeed interface.
func (c *TableFeed) Close() error {
	if c.rows != nil {
		if err := c.rows.Close(); err != nil {
			return errors.Errorf(`could not close rows: %v`, err)
		}
	}
	if _, err := c.db.Exec(`CANCEL JOB $1`, c.JobID); err != nil {
		log.Infof(context.Background(), `could not cancel feed %d: %v`, c.JobID, err)
	}
	return c.db.Close()
}

var cloudFeedFileRE = regexp.MustCompile(`^\d{33}-(.+?)-(\d+)-`)

type cloudFeedFactory struct {
	s       serverutils.TestServerInterface
	db      *gosql.DB
	dir     string
	flushCh chan struct{}

	feedIdx int
}

// MakeCloudFeedFactory returns a TestFeedFactory implementation using the cloud
// storage sink.
func MakeCloudFeedFactory(
	s serverutils.TestServerInterface, db *gosql.DB, dir string, flushCh chan struct{},
) TestFeedFactory {
	return &cloudFeedFactory{s: s, db: db, dir: dir, flushCh: flushCh}
}

// Feed implements the TestFeedFactory interface
func (f *cloudFeedFactory) Feed(create string, args ...interface{}) (TestFeed, error) {
	parsed, err := parser.ParseOne(create)
	if err != nil {
		return nil, err
	}
	createStmt := parsed.AST.(*tree.CreateChangefeed)
	if createStmt.SinkURI != nil {
		return nil, errors.Errorf(`unexpected sink provided: "INTO %s"`, tree.AsString(createStmt.SinkURI))
	}
	feedDir := strconv.Itoa(f.feedIdx)
	f.feedIdx++
	sinkURI := `experimental-nodelocal:///` + feedDir
	// TODO(dan): This is a pretty unsatisfying way to test that the sink passes
	// through params it doesn't understand to ExportStorage.
	sinkURI += `?should_be=ignored`
	createStmt.SinkURI = tree.NewStrVal(sinkURI)

	// Nodelocal puts its dir under `ExternalIODir`, which is passed into
	// cloudFeedFactory.
	feedDir = filepath.Join(f.dir, feedDir)
	if err := os.Mkdir(feedDir, 0755); err != nil {
		return nil, err
	}

	c := &cloudFeed{
		jobFeed: jobFeed{
			db:      f.db,
			flushCh: f.flushCh,
		},
		dir:  feedDir,
		seen: make(map[string]struct{}),
	}
	if err := f.db.QueryRow(createStmt.String(), args...).Scan(&c.JobID); err != nil {
		return nil, err
	}
	return c, nil
}

// Server implements the TestFeedFactory interface.
func (f *cloudFeedFactory) Server() serverutils.TestServerInterface {
	return f.s
}

type cloudFeedEntry struct {
	topic          string
	value, payload []byte
}

type cloudFeed struct {
	jobFeed
	dir string

	resolved string
	rows     []cloudFeedEntry

	seen map[string]struct{}
}

const cloudFeedPartition = ``

// Partitions implements the TestFeed interface.
func (c *cloudFeed) Partitions() []string {
	// TODO(dan): Try to plumb these through somehow?
	return []string{cloudFeedPartition}
}

// Next implements the TestFeed interface.
func (c *cloudFeed) Next() (*TestFeedMessage, error) {
	for {
		if len(c.rows) > 0 {
			e := c.rows[0]
			c.rows = c.rows[1:]
			m := &TestFeedMessage{
				Topic:    e.topic,
				Value:    e.value,
				Resolved: e.payload,
			}

			if len(m.Key) > 0 || len(m.Value) > 0 {
				seenKey := m.Topic + m.Partition + string(m.Key) + string(m.Value)
				if _, ok := c.seen[seenKey]; ok {
					continue
				}
				c.seen[seenKey] = struct{}{}
				m.Resolved = nil
				return m, nil
			}
			m.Key, m.Value = nil, nil
			return m, nil
		}

		if err := c.fetchJobError(); err != nil {
			return nil, err
		}
		if err := filepath.Walk(c.dir, c.walkDir); err != nil {
			return nil, err
		}
	}
}

func (c *cloudFeed) walkDir(path string, info os.FileInfo, _ error) error {
	if info.IsDir() {
		// Nothing to do for directories.
		return nil
	}

	var rows []cloudFeedEntry
	if strings.Compare(c.resolved, path) >= 0 {
		// Already output this in a previous walkDir.
		return nil
	}
	if strings.HasSuffix(path, `RESOLVED`) {
		c.rows = append(c.rows, rows...)
		resolvedPayload, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}
		resolvedEntry := cloudFeedEntry{payload: resolvedPayload}
		c.rows = append(c.rows, resolvedEntry)
		c.resolved = path
		return nil
	}

	var topic string
	subs := cloudFeedFileRE.FindStringSubmatch(filepath.Base(path))
	if subs == nil {
		return errors.Errorf(`unexpected file: %s`, path)
	}
	topic = subs[1]

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	// NB: This is the logic for JSON. Avro will involve parsing an
	// "Object Container File".
	s := bufio.NewScanner(f)
	for s.Scan() {
		c.rows = append(c.rows, cloudFeedEntry{
			topic: topic,
			value: s.Bytes(),
		})
	}
	return nil
}

// Close implements the TestFeed interface.
func (c *cloudFeed) Close() error {
	if _, err := c.db.Exec(`CANCEL JOB $1`, c.JobID); err != nil {
		log.Infof(context.Background(), `could not cancel feed %d: %v`, c.JobID, err)
	}
	return c.db.Close()
}
