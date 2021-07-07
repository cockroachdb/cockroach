// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"bufio"
	"bytes"
	"context"
	gosql "database/sql"
	gojson "encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx"
)

type sinklessFeedFactory struct {
	s    serverutils.TestServerInterface
	sink url.URL
}

// makeSinklessFeedFactory returns a TestFeedFactory implementation using the
// `experimental-sql` uri.
func makeSinklessFeedFactory(
	s serverutils.TestServerInterface, sink url.URL,
) cdctest.TestFeedFactory {
	return &sinklessFeedFactory{s: s, sink: sink}
}

// Feed implements the TestFeedFactory interface
func (f *sinklessFeedFactory) Feed(create string, args ...interface{}) (cdctest.TestFeed, error) {
	sink := f.sink
	sink.RawQuery = sink.Query().Encode()
	sink.Path = `d`
	// Use pgx directly instead of database/sql so we can close the conn
	// (instead of returning it to the pool).
	pgxConfig, err := pgx.ParseConnectionString(sink.String())
	if err != nil {
		return nil, err
	}
	s := &sinklessFeed{
		seenTrackerMap: make(map[string]struct{}),
		create:         create,
		args:           args,
		connCfg:        pgxConfig,
	}
	return s, s.start()
}

// Server implements the TestFeedFactory interface.
func (f *sinklessFeedFactory) Server() serverutils.TestServerInterface {
	return f.s
}

type seenTracker interface {
	reset()
}

type seenTrackerMap map[string]struct{}

func (t seenTrackerMap) markSeen(m *cdctest.TestFeedMessage) (isNew bool) {
	// TODO(dan): This skips duplicates, since they're allowed by the
	// semantics of our changefeeds. Now that we're switching to RangeFeed,
	// this can actually happen (usually because of splits) and cause flakes.
	// However, we really should be de-duping key+ts, this is too coarse.
	// Fixme.
	seenKey := m.Topic + m.Partition + string(m.Key) + string(m.Value)
	if _, ok := t[seenKey]; ok {
		return false
	}
	t[seenKey] = struct{}{}
	return true
}

func (t seenTrackerMap) reset() {
	for k := range t {
		delete(t, k)
	}
}

// sinklessFeed is an implementation of the `TestFeed` interface for a
// "sinkless" (results returned over pgwire) feed.
type sinklessFeed struct {
	seenTrackerMap
	create  string
	args    []interface{}
	connCfg pgx.ConnConfig

	conn           *pgx.Conn
	rows           *pgx.Rows
	latestResolved hlc.Timestamp
}

// Partitions implements the TestFeed interface.
func (c *sinklessFeed) Partitions() []string { return []string{`sinkless`} }

// Next implements the TestFeed interface.
func (c *sinklessFeed) Next() (*cdctest.TestFeedMessage, error) {
	m := &cdctest.TestFeedMessage{Partition: `sinkless`}
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
			if isNew := c.markSeen(m); !isNew {
				continue
			}
			return m, nil
		}
		m.Resolved = m.Value
		m.Key, m.Value = nil, nil

		// Keep track of the latest resolved timestamp so Resume can use it.
		// TODO(dan): Also do this for non-json feeds.
		if _, resolved, err := cdctest.ParseJSONValueTimestamps(m.Resolved); err == nil {
			c.latestResolved.Forward(resolved)
		}

		return m, nil
	}
}

// Resume implements the TestFeed interface.
func (c *sinklessFeed) start() error {
	var err error
	c.conn, err = pgx.Connect(c.connCfg)
	if err != nil {
		return err
	}

	// The syntax for a sinkless changefeed is `EXPERIMENTAL CHANGEFEED FOR ...`
	// but it's convenient to accept the `CREATE CHANGEFEED` syntax from the
	// test, so we can keep the current abstraction of running each test over
	// both types. This bit turns what we received into the real sinkless
	// syntax.
	create := strings.Replace(c.create, `CREATE CHANGEFEED`, `EXPERIMENTAL CHANGEFEED`, 1)
	if !c.latestResolved.IsEmpty() {
		// NB: The TODO in Next means c.latestResolved is currently never set for
		// non-json feeds.
		if strings.Contains(create, `WITH`) {
			create += fmt.Sprintf(`, cursor='%s'`, c.latestResolved.AsOfSystemTime())
		} else {
			create += fmt.Sprintf(` WITH cursor='%s'`, c.latestResolved.AsOfSystemTime())
		}
	}
	c.rows, err = c.conn.Query(create, c.args...)
	return err
}

// Close implements the TestFeed interface.
func (c *sinklessFeed) Close() error {
	c.rows = nil
	return c.conn.Close()
}

// reportErrorResumer is a job resumer which reports OnFailOrCancel events.
type reportErrorResumer struct {
	wrapped   jobs.Resumer
	jobFailed func()
}

var _ jobs.Resumer = (*reportErrorResumer)(nil)

// Resume implements jobs.Resumer
func (r *reportErrorResumer) Resume(ctx context.Context, execCtx interface{}) error {
	return r.wrapped.Resume(ctx, execCtx)
}

// OnFailOrCancel implements jobs.Resumer
func (r *reportErrorResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	defer r.jobFailed()
	return r.wrapped.OnFailOrCancel(ctx, execCtx)
}

// OnPauseRequest implements PauseRequester interface.
func (r *reportErrorResumer) OnPauseRequest(
	ctx context.Context, execCtx interface{}, txn *kv.Txn, details *jobspb.Progress,
) error {
	return r.wrapped.(*changefeedResumer).OnPauseRequest(ctx, execCtx, txn, details)
}

type wrapSinkFn func(sink Sink) Sink

// jobFeed indicates that the feed is an "enterprise feed" -- that is,
// it uses jobs system to manage its state.
type jobFeed struct {
	db       *gosql.DB
	shutdown chan struct{}
	makeSink wrapSinkFn

	jobID jobspb.JobID

	mu struct {
		syncutil.Mutex
		terminalErr error
	}
}

var _ cdctest.EnterpriseTestFeed = (*jobFeed)(nil)

func newJobFeed(db *gosql.DB, wrapper wrapSinkFn) *jobFeed {
	return &jobFeed{
		db:       db,
		shutdown: make(chan struct{}),
		makeSink: wrapper,
	}
}

// jobFailed marks this job as failed.
func (f *jobFeed) jobFailed() {
	// protect against almost concurrent terminations of the same job.
	// this could happen if the caller invokes `cancel job` just as we're
	// trying to close this feed.  Part of jobFailed handling involves
	// closing shutdown channel -- and doing this multiple times panics.

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.mu.terminalErr != nil {
		// Already failed/done.
		return
	}
	f.mu.terminalErr = f.fetchTerminalJobErr()
	close(f.shutdown)
}

func (f *jobFeed) terminalJobError() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mu.terminalErr
}

// JobID implements EnterpriseTestFeed interface.
func (f *jobFeed) JobID() jobspb.JobID {
	return f.jobID
}

func (f *jobFeed) waitForStatus(statusPred func(status jobs.Status) bool) error {
	if f.jobID == jobspb.InvalidJobID {
		// Job may not have been started.
		return nil
	}
	// Wait for the job status predicate to become true.
	return testutils.SucceedsSoonError(func() error {
		var status string
		if err := f.db.QueryRowContext(context.Background(),
			`SELECT status FROM system.jobs WHERE id = $1`, f.jobID).Scan(&status); err != nil {
			return err
		}
		if statusPred(jobs.Status(status)) {
			return nil

		}
		return errors.Newf("still waiting for job status; current %s", status)
	})
}

// Pause implements the TestFeed interface.
func (f *jobFeed) Pause() error {
	_, err := f.db.Exec(`PAUSE JOB $1`, f.jobID)
	if err != nil {
		return err
	}
	return f.waitForStatus(func(s jobs.Status) bool { return s == jobs.StatusPaused })
}

// Resume implements the TestFeed interface.
func (f *jobFeed) Resume() error {
	_, err := f.db.Exec(`RESUME JOB $1`, f.jobID)
	return err
}

// Details implements FeedJob interface.
func (f *jobFeed) Details() (*jobspb.ChangefeedDetails, error) {
	var payloadBytes []byte
	if err := f.db.QueryRow(
		`SELECT payload FROM system.jobs WHERE id=$1`, f.jobID,
	).Scan(&payloadBytes); err != nil {
		return nil, err
	}
	var payload jobspb.Payload
	if err := protoutil.Unmarshal(payloadBytes, &payload); err != nil {
		return nil, err
	}
	return payload.GetChangefeed(), nil
}

// fetchTerminalJobErr retrieves the error message from changefeed job.
func (f *jobFeed) fetchTerminalJobErr() error {
	var errStr string
	if err := f.db.QueryRow(
		`SELECT error FROM [SHOW JOBS] WHERE job_id=$1`, f.jobID,
	).Scan(&errStr); err != nil {
		return err
	}

	if errStr != "" {
		return errors.Newf("%s", errStr)
	}
	return nil
}

// Close closes job feed.
func (f *jobFeed) Close() error {
	// Signal shutdown.
	select {
	case <-f.shutdown:
	// Already failed/or failing.
	default:
		// TODO(yevgeniy): Cancel job w/out producing spurious error messages in the logs.
		if _, err := f.db.Exec(`CANCEL JOB $1`, f.jobID); err != nil {
			log.Infof(context.Background(), `could not cancel feed %d: %v`, f.jobID, err)
		}
	}

	return nil
}

// sinkSychronizer allows testfeed's Next() method to synchronize itself
// with the sink operations.
type sinkSynchronizer struct {
	syncutil.Mutex
	waitor  chan struct{}
	flushed bool
}

// eventReady returns a channel that can be waited on until the next
// event.
func (s *sinkSynchronizer) eventReady() chan struct{} {
	s.Lock()
	defer s.Unlock()

	ready := make(chan struct{})
	if s.flushed {
		close(ready)
		s.flushed = false
	} else {
		s.waitor = ready
	}

	return ready
}

func (s *sinkSynchronizer) addFlush() {
	s.Lock()
	defer s.Unlock()
	s.flushed = true
	if s.waitor != nil {
		close(s.waitor)
		s.waitor = nil
		s.flushed = false
	}
}

// notifyFlushSink keeps track of the number of emitted rows and timestamps,
// and provides a way for the caller to block until some events have been emitted.
type notifyFlushSink struct {
	Sink
	sync *sinkSynchronizer
}

func (s *notifyFlushSink) Flush(ctx context.Context) error {
	defer s.sync.addFlush()
	return s.Sink.Flush(ctx)
}

var _ Sink = (*notifyFlushSink)(nil)

// feedInjectable is the subset of the
// TestServerInterface/TestTenantInterface needed for depInjector to
// work correctly.
type feedInjectable interface {
	JobRegistry() interface{}
	DistSQLServer() interface{}
}

// depInjector facilitates dependency injection to provide orchestration
// between test feed and the changefeed itself.
// A single instance of depInjector should be used per feed factory.
// The reason we have have this dep injector (as opposed to configuring
// knobs directly) is that various knob settings are static (per sever).
// What we want is to have dependency injection per feed (since we can start
// multiple feeds inside a test).
type depInjector struct {
	syncutil.Mutex
	cond        *sync.Cond
	pendingJob  *jobFeed
	startedJobs map[jobspb.JobID]*jobFeed
}

// newDepInjector configures specified server with necessary hooks and knobs.
func newDepInjector(s feedInjectable) *depInjector {
	di := &depInjector{
		startedJobs: make(map[jobspb.JobID]*jobFeed),
	}
	di.cond = sync.NewCond(di)

	// Arrange for our wrapped sink to be instantiated.
	s.DistSQLServer().(*distsql.ServerImpl).TestingKnobs.Changefeed.(*TestingKnobs).WrapSink =
		func(s Sink, jobID jobspb.JobID) Sink {
			f := di.getJobFeed(jobID)
			return f.makeSink(s)
		}

	// Arrange for error reporting resumer to be used.
	s.JobRegistry().(*jobs.Registry).TestingResumerCreationKnobs =
		map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
			jobspb.TypeChangefeed: func(raw jobs.Resumer) jobs.Resumer {
				f := di.getJobFeed(raw.(*changefeedResumer).job.ID())
				return &reportErrorResumer{wrapped: raw, jobFailed: f.jobFailed}
			},
		}

	return di
}

// prepareJob must be called before starting the changefeed.
// it arranges for the pendingJob field to be initialized, which is needed
// when constructing "canary" sinks prior the changefeed resumer creation.
func (di *depInjector) prepareJob(jf *jobFeed) {
	di.Lock()
	defer di.Unlock()
	// Wait for the previously set pendingJob to be nil (see startJob).
	// Note: this is needed only if we create multiple feeds per feed factory rapidly.
	for di.pendingJob != nil {
		di.cond.Wait()
	}
	di.pendingJob = jf
}

// startJob must be called when changefeed job starts to register job feed
// with this dependency injector.
func (di *depInjector) startJob(jf *jobFeed) {
	di.Lock()
	defer di.Unlock()
	if _, alreadyStarted := di.startedJobs[jf.jobID]; alreadyStarted {
		panic("unexpected state: job already started")
	}
	if di.pendingJob != jf {
		panic("expected pending job to be equal to started job")
	}
	di.startedJobs[jf.jobID] = jf
	di.pendingJob = nil
	di.cond.Broadcast()
}

// getJobFeed returns jobFeed associated with the specified jobID.
// This method blocks until the job actually starts (i.e. startJob has been called).
func (di *depInjector) getJobFeed(jobID jobspb.JobID) *jobFeed {
	di.Lock()
	defer di.Unlock()
	for {
		if f, started := di.startedJobs[jobID]; started {
			return f
		}
		if di.pendingJob != nil {
			return di.pendingJob
		}
		di.cond.Wait()
	}
}

type enterpriseFeedFactory struct {
	s  serverutils.TestServerInterface
	di *depInjector
	db *gosql.DB
}

func (e enterpriseFeedFactory) startFeedJob(f *jobFeed, create string, args ...interface{}) error {
	e.di.prepareJob(f)
	if err := e.db.QueryRow(create, args...).Scan(&f.jobID); err != nil {
		return err
	}
	e.di.startJob(f)
	return nil
}

type tableFeedFactory struct {
	enterpriseFeedFactory
	uri url.URL
}

// makeTableFeedFactory returns a TestFeedFactory implementation using the
// `experimental-sql` uri.
func makeTableFeedFactory(
	srv serverutils.TestServerInterface, db *gosql.DB, sink url.URL,
) cdctest.TestFeedFactory {
	return &tableFeedFactory{
		enterpriseFeedFactory: enterpriseFeedFactory{
			s:  srv,
			di: newDepInjector(srv),
			db: db,
		},
		uri: sink,
	}
}

// Feed implements the TestFeedFactory interface
func (f *tableFeedFactory) Feed(
	create string, args ...interface{},
) (_ cdctest.TestFeed, err error) {
	sinkURI := f.uri
	sinkURI.Path = fmt.Sprintf(`table_%d`, timeutil.Now().UnixNano())

	sinkDB, err := gosql.Open("postgres", sinkURI.String())
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = sinkDB.Close()
		}
	}()

	sinkURI.Scheme = `experimental-sql`
	if _, err := sinkDB.Exec(`CREATE DATABASE ` + sinkURI.Path); err != nil {
		return nil, err
	}

	ss := &sinkSynchronizer{}
	wrapSink := func(s Sink) Sink {
		return &notifyFlushSink{Sink: s, sync: ss}
	}

	c := &tableFeed{
		jobFeed:        newJobFeed(f.db, wrapSink),
		ss:             ss,
		seenTrackerMap: make(map[string]struct{}),
		sinkDB:         sinkDB,
	}

	parsed, err := parser.ParseOne(create)
	if err != nil {
		return nil, err
	}
	createStmt := parsed.AST.(*tree.CreateChangefeed)
	if createStmt.SinkURI != nil {
		return nil, errors.Errorf(
			`unexpected uri provided: "INTO %s"`, tree.AsString(createStmt.SinkURI))
	}
	createStmt.SinkURI = tree.NewStrVal(sinkURI.String())

	if err := f.startFeedJob(c.jobFeed, createStmt.String(), args...); err != nil {
		return nil, err
	}
	return c, nil
}

// Server implements the TestFeedFactory interface.
func (f *tableFeedFactory) Server() serverutils.TestServerInterface {
	return f.s
}

// tableFeed is a TestFeed implementation using the `experimental-sql` uri.
type tableFeed struct {
	*jobFeed
	seenTrackerMap
	ss     *sinkSynchronizer
	sinkDB *gosql.DB // Changefeed emits messages into table in this DB.
	toSend []*cdctest.TestFeedMessage
}

// Partitions implements the TestFeed interface.
func (c *tableFeed) Partitions() []string {
	// The sqlSink hardcodes these.
	return []string{`0`, `1`, `2`}
}

// Next implements the TestFeed interface.
func (c *tableFeed) Next() (*cdctest.TestFeedMessage, error) {
	// sinkSink writes all changes to a table with primary key of topic,
	// partition, message_id. To simulate the semantics of kafka, message_ids
	// are only comparable within a given (topic, partition). Internally the
	// message ids are generated as a 64 bit int with a timestamp in bits 1-49
	// and a hash of the partition in 50-64. This tableFeed.Next function works
	// by repeatedly fetching and deleting all rows in the table. Then it pages
	// through the results until they are empty and repeats.
	for {
		if len(c.toSend) > 0 {
			toSend := c.toSend[0]
			c.toSend = c.toSend[1:]
			return toSend, nil
		}

		select {
		case <-c.ss.eventReady():
		case <-c.shutdown:
			return nil, c.terminalJobError()
		}

		var toSend []*cdctest.TestFeedMessage
		if err := crdb.ExecuteTx(context.Background(), c.sinkDB, nil, func(tx *gosql.Tx) error {
			_, err := tx.Exec("SET TRANSACTION PRIORITY LOW")
			if err != nil {
				return err
			}

			toSend = nil // reset for this iteration
			// TODO(dan): It's a bummer that this mutates the sqlsink table. I
			// originally tried paging through message_id by repeatedly generating a
			// new high-water with GenerateUniqueInt, but this was racy with rows
			// being flushed out by the uri. An alternative is to steal the nanos
			// part from `high_water_timestamp` in `crdb_internal.jobs` and run it
			// through `builtins.GenerateUniqueID`, but that would mean we're only
			// ever running tests on rows that have gotten a resolved timestamp,
			// which seems limiting.
			rows, err := tx.Query(
				`SELECT * FROM [DELETE FROM sqlsink RETURNING *] ORDER BY topic, partition, message_id`)
			if err != nil {
				return err
			}
			for rows.Next() {
				m := &cdctest.TestFeedMessage{}
				var msgID int64
				if err := rows.Scan(
					&m.Topic, &m.Partition, &msgID, &m.Key, &m.Value, &m.Resolved,
				); err != nil {
					return err
				}

				// Scan turns NULL bytes columns into a 0-length, non-nil byte
				// array, which is pretty unexpected. Nil them out before returning.
				// Either key+value or payload will be set, but not both.
				if len(m.Key) > 0 || len(m.Value) > 0 {
					m.Resolved = nil
				} else {
					m.Key, m.Value = nil, nil
				}
				toSend = append(toSend, m)
			}
			return rows.Err()
		}); err != nil {
			return nil, err
		}

		for _, m := range toSend {
			// NB: We should not filter seen keys in the query above -- doing so will
			// result in flaky tests if txn gets restarted.
			if len(m.Key) > 0 {
				if isNew := c.markSeen(m); !isNew {
					continue
				}
			}
			c.toSend = append(c.toSend, m)
		}
	}
}

// Close implements the TestFeed interface.
func (c *tableFeed) Close() error {
	return errors.CombineErrors(c.jobFeed.Close(), c.sinkDB.Close())
}

var cloudFeedFileRE = regexp.MustCompile(`^\d{33}-(.+?)-(\d+)-(\d+)-([0-9a-fA-F]{8})-(.+?)-`)

type cloudFeedFactory struct {
	enterpriseFeedFactory
	dir     string
	feedIdx int
}

// makeCloudFeedFactory returns a TestFeedFactory implementation using the cloud
// storage uri.
func makeCloudFeedFactory(
	srv serverutils.TestServerInterface, db *gosql.DB, dir string,
) cdctest.TestFeedFactory {
	return &cloudFeedFactory{
		enterpriseFeedFactory: enterpriseFeedFactory{
			s:  srv,
			di: newDepInjector(srv),
			db: db,
		},
		dir: dir,
	}
}

// Feed implements the TestFeedFactory interface
func (f *cloudFeedFactory) Feed(
	create string, args ...interface{},
) (tf cdctest.TestFeed, err error) {
	parsed, err := parser.ParseOne(create)
	if err != nil {
		return nil, err
	}
	createStmt := parsed.AST.(*tree.CreateChangefeed)
	if createStmt.SinkURI != nil {
		return nil, errors.Errorf(`unexpected uri provided: "INTO %s"`, tree.AsString(createStmt.SinkURI))
	}
	feedDir := strconv.Itoa(f.feedIdx)
	f.feedIdx++
	sinkURI := `experimental-nodelocal://0/` + feedDir
	// TODO(dan): This is a pretty unsatisfying way to test that the uri passes
	// through params it doesn't understand to ExternalStorage.
	sinkURI += `?should_be=ignored`
	createStmt.SinkURI = tree.NewStrVal(sinkURI)

	// Nodelocal puts its dir under `ExternalIODir`, which is passed into
	// cloudFeedFactory.
	feedDir = filepath.Join(f.dir, feedDir)
	if err := os.Mkdir(feedDir, 0755); err != nil {
		return nil, err
	}

	ss := &sinkSynchronizer{}
	wrapSink := func(s Sink) Sink {
		return &notifyFlushSink{Sink: s, sync: ss}
	}

	c := &cloudFeed{
		jobFeed:        newJobFeed(f.db, wrapSink),
		ss:             ss,
		seenTrackerMap: make(map[string]struct{}),
		dir:            feedDir,
	}
	if err := f.startFeedJob(c.jobFeed, createStmt.String(), args...); err != nil {
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
	*jobFeed
	seenTrackerMap
	ss  *sinkSynchronizer
	dir string

	resolved string
	rows     []cloudFeedEntry
}

const cloudFeedPartition = ``

// Partitions implements the TestFeed interface.
func (c *cloudFeed) Partitions() []string {
	// TODO(dan): Try to plumb these through somehow?
	return []string{cloudFeedPartition}
}

// reformatJSON marshals a golang stdlib based JSON into a byte slice preserving
// whitespace in accordance with the crdb json library.
func reformatJSON(j interface{}) ([]byte, error) {
	printed, err := gojson.Marshal(j)
	if err != nil {
		return nil, err
	}
	// The golang stdlib json library prints whitespace differently than our
	// internal one. Roundtrip through the crdb json library to get the
	// whitespace back to where it started.
	parsed, err := json.ParseJSON(string(printed))
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	parsed.Format(&buf)
	return buf.Bytes(), nil
}

// extractKeyFromJSONValue extracts the `WITH key_in_value` key from a `WITH
// format=json, envelope=wrapped` value.
func extractKeyFromJSONValue(wrapped []byte) (key []byte, value []byte, _ error) {
	parsed := make(map[string]interface{})
	if err := gojson.Unmarshal(wrapped, &parsed); err != nil {
		return nil, nil, err
	}
	keyParsed := parsed[`key`]
	delete(parsed, `key`)

	var err error
	if key, err = reformatJSON(keyParsed); err != nil {
		return nil, nil, err
	}
	if value, err = reformatJSON(parsed); err != nil {
		return nil, nil, err
	}
	return key, value, nil
}

// Next implements the TestFeed interface.
func (c *cloudFeed) Next() (*cdctest.TestFeedMessage, error) {
	for {
		if len(c.rows) > 0 {
			e := c.rows[0]
			c.rows = c.rows[1:]
			m := &cdctest.TestFeedMessage{
				Topic:    e.topic,
				Value:    e.value,
				Resolved: e.payload,
			}

			// The other TestFeed impls check both key and value here, but cloudFeeds
			// don't have keys.
			if len(m.Value) > 0 {
				// Cloud storage sinks default the `WITH key_in_value` option so that
				// the key is recoverable. Extract it out of the value (also removing it
				// so the output matches the other sinks). Note that this assumes the
				// format is json, this will have to be fixed once we add format=avro
				// support to cloud storage.
				//
				// TODO(dan): Leave the key in the value if the TestFeed user
				// specifically requested it.
				var err error
				if m.Key, m.Value, err = extractKeyFromJSONValue(m.Value); err != nil {
					return nil, err
				}
				if isNew := c.markSeen(m); !isNew {
					continue
				}
				m.Resolved = nil
				return m, nil
			}
			m.Key, m.Value = nil, nil
			return m, nil
		}

		select {
		case <-c.ss.eventReady():
		case <-c.shutdown:
			return nil, c.terminalJobError()
		}

		if err := filepath.Walk(c.dir, c.walkDir); err != nil {
			return nil, err
		}
	}
}

func (c *cloudFeed) walkDir(path string, info os.FileInfo, err error) error {
	if strings.HasSuffix(path, `.tmp`) {
		// File in the process of being written by ExternalStorage. Ignore.
		return nil
	}

	if err != nil {
		// From filepath.WalkFunc:
		//  If there was a problem walking to the file or directory named by
		//  path, the incoming error will describe the problem and the function
		//  can decide how to handle that error (and Walk will not descend into
		//  that directory). In the case of an error, the info argument will be
		//  nil. If an error is returned, processing stops.
		return err
	}

	if info.IsDir() {
		// Nothing to do for directories.
		return nil
	}

	if strings.Compare(c.resolved, path) >= 0 {
		// Already output this in a previous walkDir.
		return nil
	}
	if strings.HasSuffix(path, `RESOLVED`) {
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
	topic = subs[5]

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
			value: append([]byte(nil), s.Bytes()...),
		})
	}
	return nil
}

// teeGroup facilitates reading messages from input channel
// and sending them to one or more output channels.
type teeGroup struct {
	g    ctxgroup.Group
	done chan struct{}
}

func newTeeGroup() *teeGroup {
	return &teeGroup{
		g:    ctxgroup.WithContext(context.Background()),
		done: make(chan struct{}),
	}
}

// tee reads incoming messages from input channel and sends them out to one or more output channels.
func (tg *teeGroup) tee(in <-chan *sarama.ProducerMessage, out ...chan<- *sarama.ProducerMessage) {
	tg.g.Go(func() error {
		for {
			select {
			case <-tg.done:
				return nil
			case m := <-in:
				for i := range out {
					select {
					case <-tg.done:
						return nil
					case out[i] <- m:
					}
				}
			}
		}
	})
}

// wait shuts down tee group.
func (tg *teeGroup) wait() error {
	close(tg.done)
	return tg.g.Wait()
}

type fakeKafkaClient struct{}

func (c *fakeKafkaClient) Partitions(topic string) ([]int32, error) {
	return []int32{0}, nil
}

func (c *fakeKafkaClient) RefreshMetadata(topics ...string) error {
	return nil
}

func (c *fakeKafkaClient) Close() error {
	return nil
}

var _ kafkaClient = (*fakeKafkaClient)(nil)

type ignoreCloseProducer struct {
	*asyncProducerMock
}

func (p *ignoreCloseProducer) Close() error {
	return nil
}

// fakeKafkaSink is a sink that arranges for fake kafka client and producer
// to be used.
type fakeKafkaSink struct {
	Sink
	tg     *teeGroup
	feedCh chan *sarama.ProducerMessage
}

var _ Sink = (*fakeKafkaSink)(nil)

// Dial implements Sink interface
func (s *fakeKafkaSink) Dial() error {
	kafka := s.Sink.(*kafkaSink)
	kafka.client = &fakeKafkaClient{}
	// The producer we give to kafka sink ignores close call.
	// This is because normally, kafka sinks owns the producer and so it closes it.
	// But in this case, if we let the sink close this producer, the test will panic
	// because we will attempt to send acknowledgements on a closed channel.
	producer := &ignoreCloseProducer{newAsyncProducerMock(unbuffered)}

	// TODO(yevgeniy): Support error injection either by acknowledging on the "errors"
	//  channel, or by injecting error message into sarama.ProducerMessage.Metadata.
	s.tg.tee(producer.inputCh, s.feedCh, producer.successesCh)
	kafka.producer = producer
	kafka.start()
	return nil
}

type kafkaFeedFactory struct {
	enterpriseFeedFactory
}

var _ cdctest.TestFeedFactory = (*kafkaFeedFactory)(nil)

// makeKafkaFeedFactory returns a TestFeedFactory implementation using the `kafka` uri.
func makeKafkaFeedFactory(
	srv serverutils.TestServerInterface, db *gosql.DB,
) cdctest.TestFeedFactory {
	return &kafkaFeedFactory{
		enterpriseFeedFactory: enterpriseFeedFactory{
			s:  srv,
			db: db,
			di: newDepInjector(srv),
		},
	}
}

func exprAsString(expr tree.Expr) (string, error) {
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	semaCtx := tree.MakeSemaContext()
	te, err := expr.TypeCheck(context.Background(), &semaCtx, types.String)
	if err != nil {
		return "", err
	}
	datum, err := te.Eval(evalCtx)
	if err != nil {
		return "", err
	}
	return string(tree.MustBeDString(datum)), nil
}

// Feed implements cdctest.TestFeedFactory
func (k *kafkaFeedFactory) Feed(create string, args ...interface{}) (cdctest.TestFeed, error) {
	parsed, err := parser.ParseOne(create)
	if err != nil {
		return nil, err
	}
	createStmt := parsed.AST.(*tree.CreateChangefeed)

	// Set SinkURI if it wasn't provided.  It's okay if it is -- since we may
	// want to set some kafka specific URI parameters.
	if createStmt.SinkURI == nil {
		createStmt.SinkURI = tree.NewStrVal(
			fmt.Sprintf("%s://does.not.matter/", changefeedbase.SinkSchemeKafka))
	}

	var registry *cdctest.SchemaRegistry
	for _, opt := range createStmt.Options {
		if opt.Key == changefeedbase.OptFormat {
			format, err := exprAsString(opt.Value)
			if err != nil {
				return nil, err
			}
			if format == string(changefeedbase.OptFormatAvro) {
				// Must use confluent schema registry so that we register our schema
				// in order to be able to decode kafka messages.
				registry = cdctest.StartTestSchemaRegistry()
				registryOption := tree.KVOption{
					Key:   changefeedbase.OptConfluentSchemaRegistry,
					Value: tree.NewStrVal(registry.URL()),
				}
				createStmt.Options = append(createStmt.Options, registryOption)
				break
			}
		}
	}

	tg := newTeeGroup()
	feedCh := make(chan *sarama.ProducerMessage)
	wrapSink := func(s Sink) Sink {
		return &fakeKafkaSink{
			Sink:   s,
			tg:     tg,
			feedCh: feedCh,
		}
	}

	c := &kafkaFeed{
		jobFeed:        newJobFeed(k.db, wrapSink),
		seenTrackerMap: make(map[string]struct{}),
		source:         feedCh,
		tg:             tg,
		registry:       registry,
	}

	if err := k.startFeedJob(c.jobFeed, createStmt.String(), args...); err != nil {
		return nil, errors.CombineErrors(err, c.Close())
	}
	return c, nil
}

// Server implements TestFeedFactory
func (k *kafkaFeedFactory) Server() serverutils.TestServerInterface {
	return k.s
}

type kafkaFeed struct {
	*jobFeed
	seenTrackerMap

	source chan *sarama.ProducerMessage
	tg     *teeGroup

	// Registry is set if we're emitting avro.
	registry *cdctest.SchemaRegistry
}

var _ cdctest.TestFeed = (*kafkaFeed)(nil)

// Partitions implements TestFeed
func (k *kafkaFeed) Partitions() []string {
	// TODO(yevgeniy): Support multiple partitions.
	return []string{`kafka`}
}

// Next implements TestFeed
func (k *kafkaFeed) Next() (*cdctest.TestFeedMessage, error) {
	for {
		var msg *sarama.ProducerMessage
		select {
		case <-k.shutdown:
			return nil, k.terminalJobError()
		case msg = <-k.source:
		}

		fm := &cdctest.TestFeedMessage{
			Topic:     msg.Topic,
			Partition: `kafka`, // TODO(yevgeniy): support multiple partitions.
		}

		decode := func(encoded sarama.Encoder, dest *[]byte) error {
			// It's a bit weird to use encoder to get decoded bytes.
			// But it's correct: we produce messages to sarama, and we set
			// key/value to sarama.ByteEncoder(payload) -- and sarama ByteEncoder
			// is just the type alias to []byte -- alas, we can't cast it, so just "encode"
			// it to get back our original byte array.
			decoded, err := encoded.Encode()
			if err != nil {
				return err
			}
			if k.registry == nil {
				*dest = decoded
			} else {
				// Convert avro record to json.
				jsonBytes, err := k.registry.AvroToJSON(decoded)
				if err != nil {
					return err
				}
				*dest = jsonBytes
			}
			return nil
		}

		if msg.Key == nil {
			// It's a resolved timestamp
			if err := decode(msg.Value, &fm.Resolved); err != nil {
				return nil, err
			}
			return fm, nil
		}
		// It's a regular message
		if err := decode(msg.Key, &fm.Key); err != nil {
			return nil, err
		}
		if err := decode(msg.Value, &fm.Value); err != nil {
			return nil, err
		}

		if isNew := k.markSeen(fm); isNew {
			return fm, nil
		}
	}
}

// Close implements TestFeed interface.
func (k *kafkaFeed) Close() error {
	if k.registry != nil {
		defer k.registry.Close()
	}
	return errors.CombineErrors(k.jobFeed.Close(), k.tg.wait())
}

type webhookFeedFactory struct {
	enterpriseFeedFactory
}

var _ cdctest.TestFeedFactory = (*webhookFeedFactory)(nil)

// makeWebhookFeedFactory returns a TestFeedFactory implementation using the `webhook-webhooks` uri.
func makeWebhookFeedFactory(
	srv serverutils.TestServerInterface, db *gosql.DB,
) cdctest.TestFeedFactory {
	return &webhookFeedFactory{
		enterpriseFeedFactory: enterpriseFeedFactory{
			s:  srv,
			db: db,
			di: newDepInjector(srv),
		},
	}
}

func (f *webhookFeedFactory) Feed(create string, args ...interface{}) (cdctest.TestFeed, error) {
	parsed, err := parser.ParseOne(create)
	if err != nil {
		return nil, err
	}
	createStmt := parsed.AST.(*tree.CreateChangefeed)

	cert, _, err := cdctest.NewCACertBase64Encoded()
	if err != nil {
		return nil, err
	}
	sinkDest, err := cdctest.StartMockWebhookSink(cert)
	if err != nil {
		return nil, err
	}

	if createStmt.SinkURI == nil {
		createStmt.SinkURI = tree.NewStrVal(
			fmt.Sprintf("webhook-%s?insecure_tls_skip_verify=true", sinkDest.URL()))
	}
	ss := &sinkSynchronizer{}
	wrapSink := func(s Sink) Sink {
		return &notifyFlushSink{Sink: s, sync: ss}
	}

	c := &webhookFeed{
		jobFeed:        newJobFeed(f.db, wrapSink),
		seenTrackerMap: make(map[string]struct{}),
		ss:             ss,
		mockSink:       sinkDest,
	}
	if err := f.startFeedJob(c.jobFeed, createStmt.String(), args...); err != nil {
		sinkDest.Close()
		return nil, err
	}
	return c, nil
}

func (f *webhookFeedFactory) Server() serverutils.TestServerInterface {
	return f.s
}

type webhookFeed struct {
	*jobFeed
	seenTrackerMap
	ss       *sinkSynchronizer
	mockSink *cdctest.MockWebhookSink
}

var _ cdctest.TestFeed = (*webhookFeed)(nil)

// Partitions implements TestFeed
func (f *webhookFeed) Partitions() []string {
	return []string{``}
}

// isResolvedTimestamp determines if the given JSON message is a resolved timestamp message.
func isResolvedTimestamp(message []byte) (bool, error) {
	parsed := make(map[string]interface{})
	if err := gojson.Unmarshal(message, &parsed); err != nil {
		return false, err
	}
	_, ok := parsed[`resolved`]
	return ok, nil
}

// extractTopicFromJSONValue extracts the `WITH topic_in_value` topic from a `WITH
// format=json, envelope=wrapped` value.
func extractTopicFromJSONValue(wrapped []byte) (topic string, value []byte, _ error) {
	parsed := make(map[string]interface{})
	if err := gojson.Unmarshal(wrapped, &parsed); err != nil {
		return "", nil, err
	}
	topicParsed := parsed[`topic`]
	delete(parsed, `topic`)

	topic = fmt.Sprintf("%v", topicParsed)
	var err error
	if value, err = reformatJSON(parsed); err != nil {
		return "", nil, err
	}
	return topic, value, nil
}

// extractValueFromJSONMessage extracts the value of the first element of
// the payload array from an webhook sink JSON message.
func extractValueFromJSONMessage(message []byte) ([]byte, error) {
	parsed := make(map[string][]interface{})
	if err := gojson.Unmarshal(message, &parsed); err != nil {
		return nil, err
	}
	keyParsed := parsed[`payload`]
	if len(keyParsed) <= 0 {
		return nil, fmt.Errorf("payload value in json message contains no elements")
	}

	var err error
	var value []byte
	if value, err = reformatJSON(keyParsed[0]); err != nil {
		return nil, err
	}
	return value, nil
}

// Next implements TestFeed
func (f *webhookFeed) Next() (*cdctest.TestFeedMessage, error) {
	for {
		msg := f.mockSink.Pop()
		if msg != "" {
			m := &cdctest.TestFeedMessage{}
			if msg != "" {
				var err error
				var resolved bool
				resolved, err = isResolvedTimestamp([]byte(msg))
				if err != nil {
					return nil, err
				}
				if resolved {
					m.Resolved = []byte(msg)
				} else {
					wrappedValue, err := extractValueFromJSONMessage([]byte(msg))
					if err != nil {
						return nil, err
					}
					if m.Key, m.Value, err = extractKeyFromJSONValue([]byte(wrappedValue)); err != nil {
						return nil, err
					}
					if m.Topic, m.Value, err = extractTopicFromJSONValue(m.Value); err != nil {
						return nil, err
					}
					if isNew := f.markSeen(m); !isNew {
						continue
					}
				}
				return m, nil
			}
			m.Key, m.Value = nil, nil
			return m, nil
		}

		select {
		case <-f.ss.eventReady():
		case <-f.shutdown:
			return nil, f.terminalJobError()
		}
	}
}

// Close implements TestFeed
func (f *webhookFeed) Close() error {
	err := f.jobFeed.Close()
	if err != nil {
		return err
	}
	f.mockSink.Close()
	return nil
}
