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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
)

// TestFeedFactory is an interface to create changefeeds.
type TestFeedFactory interface {
	// Feed creates a new TestFeed.
	Feed(t testing.TB, create string, args ...interface{}) TestFeed
	// Server returns the raw underlying TestServer, if applicable.
	Server() serverutils.TestServerInterface
}

// TestFeed abstracts over reading from the various types of changefeed sinks.
type TestFeed interface {
	// Partitions returns the domain of values that may be returned as a partition
	// by Next.
	Partitions() []string
	// Next returns the next message. Within a given topic+partition, the order is
	// preserved, but not otherwise. Either len(key) and len(value) will be
	// greater than zero (a row updated) or len(payload) will be (a resolved
	// timestamp). If there are no more messages, Next will return false and Err
	// should be checked.
	Next(t testing.TB) (topic, partition string, key, value, payload []byte, ok bool)
	// If Next returns false, Err will return the relevant error, if any.
	Err() error
	// Close shuts down the changefeed and releases resources.
	Close(t testing.TB)
}

type sinklessFeedFactory struct {
	s serverutils.TestServerInterface
}

// MakeSinklessFeedFactory returns a TestFeedFactory implementation for
// "sinkless" (results returned over pgwire) feeds.
func MakeSinklessFeedFactory(s serverutils.TestServerInterface) TestFeedFactory {
	return &sinklessFeedFactory{s: s}
}

// Feed implements the TestFeedFactory interface
func (f *sinklessFeedFactory) Feed(t testing.TB, create string, args ...interface{}) TestFeed {
	t.Helper()
	url, cleanup := sqlutils.PGUrl(t, f.s.ServingAddr(), t.Name(), url.User(security.RootUser))
	q := url.Query()
	q.Add(`results_buffer_size`, `1`)
	url.RawQuery = q.Encode()
	s := &sinklessFeed{cleanup: cleanup, seen: make(map[string]struct{})}
	url.Path = `d`
	// Use pgx directly instead of database/sql so we can close the conn
	// (instead of returning it to the pool).
	pgxConfig, err := pgx.ParseConnectionString(url.String())
	if err != nil {
		t.Fatal(err)
	}
	s.conn, err = pgx.Connect(pgxConfig)
	if err != nil {
		t.Fatal(err)
	}

	// The syntax for a sinkless changefeed is `EXPERIMENTAL CHANGEFEED FOR ...`
	// but it's convenient to accept the `CREATE CHANGEFEED` syntax from the
	// test, so we can keep the current abstraction of running each test over
	// both types. This bit turns what we received into the real sinkless
	// syntax.
	create = strings.Replace(create, `CREATE CHANGEFEED`, `EXPERIMENTAL CHANGEFEED`, 1)

	s.rows, err = s.conn.Query(create, args...)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// Server implements the TestFeedFactory interface.
func (f *sinklessFeedFactory) Server() serverutils.TestServerInterface {
	return f.s
}

// sinklessFeed is an implementation of the `TestFeed` interface for a
// "sinkless" (results returned over pgwire) feed.
type sinklessFeed struct {
	conn    *pgx.Conn
	cleanup func()
	rows    *pgx.Rows
	seen    map[string]struct{}
}

// Partitions implements the TestFeed interface.
func (c *sinklessFeed) Partitions() []string { return []string{`sinkless`} }

// Next implements the TestFeed interface.
func (c *sinklessFeed) Next(
	t testing.TB,
) (topic, partition string, key, value, resolved []byte, ok bool) {
	t.Helper()
	partition = `sinkless`
	var noKey, noValue, noResolved []byte
	for {
		if !c.rows.Next() {
			return ``, ``, nil, nil, nil, false
		}
		var maybeTopic gosql.NullString
		if err := c.rows.Scan(&maybeTopic, &key, &value); err != nil {
			t.Fatal(err)
		}
		if len(maybeTopic.String) > 0 {
			// TODO(dan): This skips duplicates, since they're allowed by the
			// semantics of our changefeeds. Now that we're switching to
			// RangeFeed, this can actually happen (usually because of splits)
			// and cause flakes. However, we really should be de-deuping key+ts,
			// this is too coarse. Fixme.
			seenKey := maybeTopic.String + partition + string(key) + string(value)
			if _, ok := c.seen[seenKey]; ok {
				continue
			}
			c.seen[seenKey] = struct{}{}
			return maybeTopic.String, partition, key, value, noResolved, true
		}
		resolvedPayload := value
		return ``, partition, noKey, noValue, resolvedPayload, true
	}
}

// Err implements the TestFeed interface.
func (c *sinklessFeed) Err() error {
	if c.rows != nil {
		return c.rows.Err()
	}
	return nil
}

// Close implements the TestFeed interface.
func (c *sinklessFeed) Close(t testing.TB) {
	t.Helper()
	if err := c.conn.Close(); err != nil {
		t.Error(err)
	}
	c.cleanup()
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
}

// MakeTableFeedFactory returns a TestFeedFactory implementation using the
// `experimental-sql` sink.
func MakeTableFeedFactory(
	s serverutils.TestServerInterface, db *gosql.DB, flushCh chan struct{},
) TestFeedFactory {
	return &tableFeedFactory{s: s, db: db, flushCh: flushCh}
}

// Feed implements the TestFeedFactory interface
func (f *tableFeedFactory) Feed(t testing.TB, create string, args ...interface{}) TestFeed {
	t.Helper()

	sink, cleanup := sqlutils.PGUrl(t, f.s.ServingAddr(), t.Name(), url.User(security.RootUser))
	sink.Path = fmt.Sprintf(`table_%d`, timeutil.Now().UnixNano())

	db, err := gosql.Open("postgres", sink.String())
	if err != nil {
		t.Fatal(err)
	}

	sink.Scheme = `experimental-sql`
	c := &TableFeed{
		jobFeed: jobFeed{
			db:      db,
			flushCh: f.flushCh,
		},
		urlCleanup: cleanup,
		sinkURI:    sink.String(),
		seen:       make(map[string]struct{}),
	}
	if _, err := c.db.Exec(`CREATE DATABASE ` + sink.Path); err != nil {
		t.Fatal(err)
	}

	parsed, err := parser.ParseOne(create)
	if err != nil {
		t.Fatal(err)
	}
	createStmt := parsed.AST.(*tree.CreateChangefeed)
	if createStmt.SinkURI != nil {
		t.Fatalf(`unexpected sink provided: "INTO %s"`, tree.AsString(createStmt.SinkURI))
	}
	createStmt.SinkURI = tree.NewStrVal(c.sinkURI)

	if err := f.db.QueryRow(createStmt.String(), args...).Scan(&c.JobID); err != nil {
		t.Fatal(err)
	}
	return c
}

// Server implements the TestFeedFactory interface.
func (f *tableFeedFactory) Server() serverutils.TestServerInterface {
	return f.s
}

// TableFeed is a TestFeed implementation using the `experimental-sql` sink.
type TableFeed struct {
	jobFeed
	sinkURI    string
	urlCleanup func()

	rows *gosql.Rows
	seen map[string]struct{}
}

// Partitions implements the TestFeed interface.
func (c *TableFeed) Partitions() []string {
	// The sqlSink hardcodes these.
	return []string{`0`, `1`, `2`}
}

// Next implements the TestFeed interface.
func (c *TableFeed) Next(
	t testing.TB,
) (topic, partition string, key, value, payload []byte, ok bool) {
	// sinkSink writes all changes to a table with primary key of topic,
	// partition, message_id. To simulate the semantics of kafka, message_ids
	// are only comparable within a given (topic, partition). Internally the
	// message ids are generated as a 64 bit int with a timestamp in bits 1-49
	// and a hash of the partition in 50-64. This TableFeed.Next function works
	// by repeatedly fetching and deleting all rows in the table. Then it pages
	// through the results until they are empty and repeats.
	for {
		if c.rows != nil && c.rows.Next() {
			var msgID int64
			if err := c.rows.Scan(&topic, &partition, &msgID, &key, &value, &payload); err != nil {
				t.Fatal(err)
			}

			// Scan turns NULL bytes columns into a 0-length, non-nil byte
			// array, which is pretty unexpected. Nil them out before returning.
			// Either key+value or payload will be set, but not both.
			if len(key) > 0 || len(value) > 0 {
				// TODO(dan): This skips duplicates, since they're allowed by
				// the semantics of our changefeeds. Now that we're switching to
				// RangeFeed, this can actually happen (usually because of
				// splits) and cause flakes. However, we really should be
				// de-deuping key+ts, this is too coarse. Fixme.
				seenKey := topic + partition + string(key) + string(value)
				if _, ok := c.seen[seenKey]; ok {
					continue
				}
				c.seen[seenKey] = struct{}{}

				payload = nil
			} else {
				key, value = nil, nil
			}
			return topic, partition, key, value, payload, true
		}
		if c.rows != nil {
			if err := c.rows.Close(); err != nil {
				t.Fatal(err)
			}
			c.rows = nil
		}

		if err := c.fetchJobError(); err != nil {
			return ``, ``, nil, nil, nil, false
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
			t.Fatal(err)
		}
	}
}

// Err implements the TestFeed interface.
func (c *TableFeed) Err() error {
	return c.jobErr
}

// Close implements the TestFeed interface.
func (c *TableFeed) Close(t testing.TB) {
	if c.rows != nil {
		if err := c.rows.Close(); err != nil {
			t.Errorf(`could not close rows: %v`, err)
		}
	}
	if _, err := c.db.Exec(`CANCEL JOB $1`, c.JobID); err != nil {
		log.Infof(context.Background(), `could not cancel feed %d: %v`, c.JobID, err)
	}
	if err := c.db.Close(); err != nil {
		t.Error(err)
	}
	c.urlCleanup()
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
func (f *cloudFeedFactory) Feed(t testing.TB, create string, args ...interface{}) TestFeed {
	t.Helper()

	parsed, err := parser.ParseOne(create)
	if err != nil {
		t.Fatal(err)
	}
	createStmt := parsed.AST.(*tree.CreateChangefeed)
	if createStmt.SinkURI != nil {
		t.Fatalf(`unexpected sink provided: "INTO %s"`, tree.AsString(createStmt.SinkURI))
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
		t.Fatal(err)
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
		t.Fatal(err)
	}
	return c
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
func (c *cloudFeed) Next(
	t testing.TB,
) (topic, partition string, key, value, payload []byte, ok bool) {
	for {
		if len(c.rows) > 0 {
			e := c.rows[0]
			c.rows = c.rows[1:]
			topic, key, value, payload = e.topic, nil, e.value, e.payload

			if len(value) > 0 {
				seen := topic + string(value)
				if _, ok := c.seen[seen]; ok {
					continue
				}
				c.seen[seen] = struct{}{}
				payload = nil
				return topic, cloudFeedPartition, key, value, payload, true
			}
			key, value = nil, nil
			return topic, cloudFeedPartition, key, value, payload, true
		}

		if err := c.fetchJobError(); err != nil {
			return ``, ``, nil, nil, nil, false
		}
		if err := filepath.Walk(c.dir, c.walkDir); err != nil {
			t.Fatal(err)
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

// Err implements the TestFeed interface.
func (c *cloudFeed) Err() error {
	return c.jobErr
}

// Close implements the TestFeed interface.
func (c *cloudFeed) Close(t testing.TB) {
	if _, err := c.db.Exec(`CANCEL JOB $1`, c.JobID); err != nil {
		log.Infof(context.Background(), `could not cancel feed %d: %v`, c.JobID, err)
	}
	if err := c.db.Close(); err != nil {
		t.Error(err)
	}
}
