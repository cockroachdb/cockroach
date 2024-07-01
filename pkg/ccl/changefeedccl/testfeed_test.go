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
	"encoding/base64"
	gojson "encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pubsubv1 "cloud.google.com/go/pubsub/apiv1"
	pb "cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/IBM/sarama"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/mocks"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/parquet"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/golang/mock/gomock"
	"github.com/jackc/pgx/v4"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type sinklessFeedFactory struct {
	s serverutils.ApplicationLayerInterface
	// postgres url used for creating sinkless changefeeds. This may be the same as
	// the rootURL.
	sink url.URL
	// postgres url for the root user, used for test internals.
	rootURL     url.URL
	sinkForUser sinkForUser
}

// makeSinklessFeedFactory returns a TestFeedFactory implementation using the
// `experimental-sql` uri.
func makeSinklessFeedFactory(
	s serverutils.ApplicationLayerInterface, sink url.URL, rootConn url.URL, sinkForUser sinkForUser,
) cdctest.TestFeedFactory {
	return &sinklessFeedFactory{s: s, sink: sink, rootURL: rootConn, sinkForUser: sinkForUser}
}

// AsUser executes fn as the specified user.
func (f *sinklessFeedFactory) AsUser(user string, fn func(*sqlutils.SQLRunner)) error {
	prevSink := f.sink
	password := `hunter2`
	if err := f.setPassword(user, password); err != nil {
		return err
	}
	defer func() { f.sink = prevSink }()
	var cleanup func()
	f.sink, cleanup = f.sinkForUser(user, password)
	pgconn := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(user, password),
		Host:   f.Server().SQLAddr(),
		Path:   `d`,
	}
	db2, err := gosql.Open("postgres", pgconn.String())
	if err != nil {
		return err
	}
	defer db2.Close()
	userDB := sqlutils.MakeSQLRunner(db2)
	fn(userDB)
	cleanup()
	return nil
}

func (f *sinklessFeedFactory) setPassword(user, password string) error {
	rootDB, err := gosql.Open("postgres", f.rootURL.String())
	if err != nil {
		return err
	}
	defer rootDB.Close()
	_, err = rootDB.Exec(fmt.Sprintf(`ALTER USER %s WITH PASSWORD '%s'`, user, password))
	return err
}

// Feed implements the TestFeedFactory interface
func (f *sinklessFeedFactory) Feed(create string, args ...interface{}) (cdctest.TestFeed, error) {
	sink := f.sink
	sink.RawQuery = sink.Query().Encode()
	// Use pgx directly instead of database/sql so we can close the conn
	// (instead of returning it to the pool).
	pgxConfig, err := pgx.ParseConfig(sink.String())
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
func (f *sinklessFeedFactory) Server() serverutils.ApplicationLayerInterface {
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
		if log.V(1) {
			log.Infof(context.Background(), "skip dup %s", seenKey)
		}
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
	connCfg *pgx.ConnConfig

	conn           *pgx.Conn
	rows           pgx.Rows
	latestResolved hlc.Timestamp
}

var _ cdctest.TestFeed = (*sinklessFeed)(nil)

func timeout() time.Duration {
	if util.RaceEnabled {
		return 5 * time.Minute
	}
	return 30 * time.Second
}

// Partitions implements the TestFeed interface.
func (c *sinklessFeed) Partitions() []string { return []string{`sinkless`} }

// Next implements the TestFeed interface.
func (c *sinklessFeed) Next() (*cdctest.TestFeedMessage, error) {
	defer time.AfterFunc(timeout(), func() {
		_ = c.conn.Close(context.Background())
	}).Stop()

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
func (c *sinklessFeed) start() (err error) {
	c.conn, err = pgx.ConnectConfig(context.Background(), c.connCfg)
	if err != nil {
		return err
	}

	create := c.create
	if !c.latestResolved.IsEmpty() {
		// NB: The TODO in Next means c.latestResolved is currently never set for
		// non-json feeds.
		if strings.Contains(create, `WITH`) {
			create += fmt.Sprintf(`, cursor='%s'`, c.latestResolved.AsOfSystemTime())
		} else {
			create += fmt.Sprintf(` WITH cursor='%s'`, c.latestResolved.AsOfSystemTime())
		}
	}
	c.rows, err = c.conn.Query(context.Background(), create, c.args...)
	return err
}

// Close implements the TestFeed interface.
func (c *sinklessFeed) Close() error {
	c.rows = nil
	return c.conn.Close(context.Background())
}

type logger interface {
	Log(args ...interface{})
}

type externalConnectionFeedFactory struct {
	cdctest.TestFeedFactory
	db     *gosql.DB
	logger logger
}

type externalConnectionCreator func(uri string) error

func (e *externalConnectionFeedFactory) Feed(
	create string, args ...interface{},
) (_ cdctest.TestFeed, err error) {

	randomExternalConnectionName := fmt.Sprintf("testconn%d", rand.Int63())

	var c externalConnectionCreator = func(uri string) error {
		e.logger.Log("creating external connection")
		createConnStmt := fmt.Sprintf(`CREATE EXTERNAL CONNECTION %s AS '%s'`, randomExternalConnectionName, uri)
		_, err := e.db.Exec(createConnStmt)
		e.logger.Log("ran create external connection")
		if err != nil {
			e.logger.Log("error creating external connection:" + err.Error())
		}
		return err
	}

	args = append([]interface{}{c}, args...)

	parsed, err := parser.ParseOne(create)
	if err != nil {
		return nil, err
	}
	createStmt := parsed.AST.(*tree.CreateChangefeed)
	if createStmt.SinkURI != nil {
		return nil, errors.Errorf(
			`unexpected uri provided: "INTO %s"`, tree.AsString(createStmt.SinkURI))
	}
	createStmt.SinkURI = tree.NewStrVal(`external://` + randomExternalConnectionName)

	return e.TestFeedFactory.Feed(createStmt.String(), args...)
}

func setURI(
	createStmt *tree.CreateChangefeed, uri string, allowOverride bool, args *[]interface{},
) error {
	if createStmt.SinkURI != nil {
		u, err := url.Parse(tree.AsStringWithFlags(createStmt.SinkURI, tree.FmtBareStrings))
		if err != nil {
			return err
		}
		if u.Scheme == changefeedbase.SinkSchemeExternalConnection {
			fn, ok := (*args)[0].(externalConnectionCreator)
			if ok {
				*args = (*args)[1:]
				return fn(uri)
			}
		}
		if allowOverride {
			return nil
		}
		return errors.Errorf(
			`unexpected uri provided: "INTO %s"`, tree.AsString(createStmt.SinkURI))
	}
	createStmt.SinkURI = tree.NewStrVal(uri)
	return nil
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
func (r *reportErrorResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, jobErr error,
) error {
	defer r.jobFailed()
	return r.wrapped.OnFailOrCancel(ctx, execCtx, jobErr)
}

// CollectProfile implements jobs.Resumer.
func (r *reportErrorResumer) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
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

type jobFailedMarker interface {
	jobFailed(err error)
}

// jobFailed marks this job as failed.
func (f *jobFeed) jobFailed(err error) {
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
	f.mu.terminalErr = err
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

func (f *jobFeed) status() (status string, err error) {
	err = f.db.QueryRowContext(context.Background(),
		`SELECT status FROM system.jobs WHERE id = $1`, f.jobID).Scan(&status)
	return
}

func (f *jobFeed) WaitForStatus(statusPred func(status jobs.Status) bool) error {
	if f.jobID == jobspb.InvalidJobID {
		// Job may not have been started.
		return nil
	}
	// Wait for the job status predicate to become true.
	return testutils.SucceedsSoonError(func() error {
		var status string
		var err error
		if status, err = f.status(); err != nil {
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
	return f.WaitForStatus(func(s jobs.Status) bool { return s == jobs.StatusPaused })
}

// Resume implements the TestFeed interface.
func (f *jobFeed) Resume() error {
	_, err := f.db.Exec(`RESUME JOB $1`, f.jobID)
	if err != nil {
		return err
	}
	return f.WaitForStatus(func(s jobs.Status) bool { return s == jobs.StatusRunning })
}

// Details implements FeedJob interface.
func (f *jobFeed) Details() (*jobspb.ChangefeedDetails, error) {
	var payloadBytes []byte
	if err := f.db.QueryRow(jobutils.JobPayloadByIDQuery, f.jobID).Scan(&payloadBytes); err != nil {
		return nil, errors.Wrapf(err, "Details for job %d", f.jobID)
	}
	var payload jobspb.Payload
	if err := protoutil.Unmarshal(payloadBytes, &payload); err != nil {
		return nil, err
	}
	return payload.GetChangefeed(), nil
}

// HighWaterMark implements FeedJob interface.
func (f *jobFeed) HighWaterMark() (hlc.Timestamp, error) {
	var details []byte
	if err := f.db.QueryRow(jobutils.JobProgressByIDQuery, f.jobID).Scan(&details); err != nil {
		return hlc.Timestamp{}, errors.Wrapf(err, "HighWaterMark for job %d", f.jobID)
	}
	var progress jobspb.Progress
	if err := protoutil.Unmarshal(details, &progress); err != nil {
		return hlc.Timestamp{}, err
	}
	h := progress.GetHighWater()
	var hwm hlc.Timestamp
	if h != nil {
		hwm = *h
	}
	return hwm, nil
}

// TickHighWaterMark implements the TestFeed interface.
func (f *jobFeed) TickHighWaterMark(minHWM hlc.Timestamp) error {
	return testutils.SucceedsWithinError(func() error {
		current, err := f.HighWaterMark()
		if err != nil {
			return err
		}
		if minHWM.Less(current) {
			return nil
		}
		return errors.Newf("waiting to tick: current=%s min=%s", current, minHWM)
	}, 10*time.Second)
}

// FetchTerminalJobErr retrieves the error message from changefeed job.
func (f *jobFeed) FetchTerminalJobErr() error {
	var errStr string
	if err := testutils.SucceedsSoonError(func() error {
		return f.db.QueryRow(
			`SELECT error FROM [SHOW JOBS] WHERE job_id=$1`, f.jobID,
		).Scan(&errStr)
	}); err != nil {
		return errors.Wrapf(err, "FetchTerminalJobErr for job %d", f.jobID)
	}

	if errStr != "" {
		return errors.Newf("%s", errStr)
	}
	return nil
}

// FetchRunningStatus retrieves running status from changefeed job.
func (f *jobFeed) FetchRunningStatus() (runningStatusStr string, err error) {
	if err = f.db.QueryRow(
		`SELECT running_status FROM [SHOW JOBS] WHERE job_id=$1`, f.jobID,
	).Scan(&runningStatusStr); err != nil {
		return "", errors.Wrapf(err, "FetchRunningStatus for job %d", f.jobID)
	}
	return runningStatusStr, err
}

// Close closes job feed.
func (f *jobFeed) Close() error {
	// Signal shutdown.
	select {
	case <-f.shutdown:
	// Already failed/or failing.
	default:
		// TODO(yevgeniy): Cancel job w/out producing spurious error messages in the logs.
		if f.jobID == jobspb.InvalidJobID {
			// Some tests may create a jobFeed without creating a new job. Hence, if
			// the jobID is invalid, skip trying to cancel the job.
			return nil
		}
		status, err := f.status()
		if err != nil {
			return err
		}
		if status == string(jobs.StatusSucceeded) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.mu.terminalErr = errors.New("changefeed completed")
			close(f.shutdown)
			return nil
		}
		if status == string(jobs.StatusFailed) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.mu.terminalErr = errors.New("changefeed failed")
			close(f.shutdown)
			return nil
		}
		if _, err := f.db.Exec(`CANCEL JOB $1`, f.jobID); err != nil {
			log.Infof(context.Background(), `could not cancel feed %d: %v`, f.jobID, err)
		} else {
			return f.WaitForStatus(func(s jobs.Status) bool { return s == jobs.StatusCanceled })
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

func (s *notifyFlushSink) EncodeAndEmitRow(
	ctx context.Context,
	updatedRow cdcevent.Row,
	prevRow cdcevent.Row,
	topic TopicDescriptor,
	updated, mvcc hlc.Timestamp,
	encodingOpts changefeedbase.EncodingOptions,
	alloc kvevent.Alloc,
) error {
	if sinkWithEncoder, ok := s.Sink.(SinkWithEncoder); ok {
		return sinkWithEncoder.EncodeAndEmitRow(ctx, updatedRow, prevRow, topic, updated, mvcc, encodingOpts, alloc)
	}
	return errors.AssertionFailedf("Expected a sink with encoder for, found %T", s.Sink)
}

var _ Sink = (*notifyFlushSink)(nil)

// feedInjectable is the subset of the
// TestServerInterface/ApplicationLayerInterface needed for depInjector to
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
func newDepInjector(srvs ...feedInjectable) *depInjector {
	di := &depInjector{
		startedJobs: make(map[jobspb.JobID]*jobFeed),
	}
	di.cond = sync.NewCond(di)

	for _, s := range srvs {
		// Arrange for our wrapped sink to be instantiated.
		s.DistSQLServer().(*distsql.ServerImpl).TestingKnobs.Changefeed.(*TestingKnobs).WrapSink =
			func(s Sink, jobID jobspb.JobID) Sink {
				f := di.getJobFeed(jobID)
				return f.makeSink(s)
			}

		// Arrange for error reporting resumer to be used.
		s.JobRegistry().(*jobs.Registry).TestingWrapResumerConstructor(
			jobspb.TypeChangefeed,
			func(raw jobs.Resumer) jobs.Resumer {
				f := di.getJobFeed(raw.(*changefeedResumer).job.ID())
				return &reportErrorResumer{
					wrapped: raw,
					jobFailed: func() {
						f.jobFailed(f.FetchTerminalJobErr())
					},
				}
			},
		)
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
	s  serverutils.ApplicationLayerInterface
	di *depInjector
	// db is used for creating changefeeds. This may be the same as rootDB.
	db *gosql.DB
	// rootDB is used for test internals.
	rootDB *gosql.DB
}

func (e *enterpriseFeedFactory) configureUserDB(db *gosql.DB) {
	e.db = db
}

func (e *enterpriseFeedFactory) jobsTableConn() *gosql.DB {
	return e.rootDB
}

// AsUser uses the previous connection to ensure
// the user has the ability to authenticate, and saves it to poll
// job status, then implements TestFeedFactory.AsUser().
func (e *enterpriseFeedFactory) AsUser(user string, fn func(*sqlutils.SQLRunner)) error {
	prevDB := e.db
	defer func() { e.db = prevDB }()
	password := `password`
	_, err := e.rootDB.Exec(fmt.Sprintf(`ALTER USER %s WITH PASSWORD '%s'`, user, password))
	if err != nil {
		return err
	}
	pgURL := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(user, password),
		Host:   e.s.SQLAddr(),
		Path:   `d`,
	}
	db2, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		return err
	}
	defer db2.Close()
	userDB := sqlutils.MakeSQLRunner(db2)

	e.db = db2
	fn(userDB)
	return nil
}

func (e enterpriseFeedFactory) startFeedJob(f *jobFeed, create string, args ...interface{}) error {
	log.Infof(context.Background(), "Starting feed job: %q", create)
	e.di.prepareJob(f)
	if err := e.db.QueryRow(create, args...).Scan(&f.jobID); err != nil {
		e.di.pendingJob = nil
		return errors.Wrapf(err, "failed to start feed for job %d", f.jobID)
	}
	e.di.startJob(f)
	return nil
}

type sinkForUser func(username string, password ...string) (uri url.URL, cleanup func())

type tableFeedFactory struct {
	enterpriseFeedFactory
	uri url.URL
}

func getInjectables(
	srvOrCluster interface{},
) (serverutils.ApplicationLayerInterface, []feedInjectable) {
	switch t := srvOrCluster.(type) {
	case serverutils.ApplicationLayerInterface:
		t.PGServer()
		return t, []feedInjectable{t}
	case serverutils.TestClusterInterface:
		servers := make([]feedInjectable, t.NumServers())
		for i := range servers {
			servers[i] = t.Server(i)
		}
		return t.Server(0), servers
	default:
		panic(errors.AssertionFailedf("unexpected type %T", t))
	}
}

// makeTableFeedFactory returns a TestFeedFactory implementation using the
// `experimental-sql` uri.
func makeTableFeedFactory(
	srvOrCluster interface{}, rootDB *gosql.DB, sink url.URL,
) cdctest.TestFeedFactory {
	s, injectables := getInjectables(srvOrCluster)
	return &tableFeedFactory{
		enterpriseFeedFactory: enterpriseFeedFactory{
			s:      s,
			di:     newDepInjector(injectables...),
			db:     rootDB,
			rootDB: rootDB,
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
		jobFeed:        newJobFeed(f.jobsTableConn(), wrapSink),
		ss:             ss,
		seenTrackerMap: make(map[string]struct{}),
		sinkDB:         sinkDB,
	}

	parsed, err := parser.ParseOne(create)
	if err != nil {
		return nil, err
	}
	createStmt := parsed.AST.(*tree.CreateChangefeed)
	if err := setURI(createStmt, sinkURI.String(), false, &args); err != nil {
		return nil, err
	}

	if err := f.startFeedJob(c.jobFeed, createStmt.String(), args...); err != nil {
		return nil, err
	}
	return c, nil
}

// Server implements the TestFeedFactory interface.
func (f *tableFeedFactory) Server() serverutils.ApplicationLayerInterface {
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

var _ cdctest.TestFeed = (*tableFeed)(nil)

// Partitions implements the TestFeed interface.
func (c *tableFeed) Partitions() []string {
	// The sqlSink hardcodes these.
	return []string{`0`, `1`, `2`}
}

func timeoutOp(op string, id jobspb.JobID) string {
	return fmt.Sprintf("%s-%d", op, id)
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

		if err := timeutil.RunWithTimeout(
			context.Background(), timeoutOp("tableFeed.Next", c.jobID), timeout(),
			func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-c.ss.eventReady():
					return nil
				case <-c.shutdown:
					return c.terminalJobError()
				}
			},
		); err != nil {
			return nil, err
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

var feedIdx int32

func feedSubDir() string {
	return strconv.Itoa(int(atomic.AddInt32(&feedIdx, 1)))
}

type cloudFeedFactory struct {
	enterpriseFeedFactory
	dir string
}

// makeCloudFeedFactory returns a TestFeedFactory implementation using the cloud
// storage uri.
func makeCloudFeedFactory(
	srvOrCluster interface{}, rootDB *gosql.DB, dir string,
) cdctest.TestFeedFactory {
	s, injectables := getInjectables(srvOrCluster)
	return &cloudFeedFactory{
		enterpriseFeedFactory: enterpriseFeedFactory{
			s:      s,
			di:     newDepInjector(injectables...),
			db:     rootDB,
			rootDB: rootDB,
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

	if createStmt.Select != nil {
		createStmt.Options = append(createStmt.Options,
			// Normally, cloud storage requires key_in_value; but if we use bare envelope,
			// this option is not required.  However, we need it to make this
			// test feed work -- so, set it.
			tree.KVOption{Key: changefeedbase.OptKeyInValue},
		)
	}

	formatSpecified := false
	explicitEnvelope := false
	for _, opt := range createStmt.Options {
		if string(opt.Key) == changefeedbase.OptFormat {
			formatSpecified = true
		}
		if string(opt.Key) == changefeedbase.OptEnvelope {
			explicitEnvelope = true
		}
	}

	if !formatSpecified {
		// Determine if we can enable the parquet format if the changefeed is not
		// being created with incompatible options. If it can be enabled, we will use
		// parquet format with a probability of 0.4.
		parquetPossible := includeParquestTestMetadata
		for _, opt := range createStmt.Options {
			for o := range changefeedbase.ParquetFormatUnsupportedOptions {
				if o == string(opt.Key) {
					parquetPossible = false
					break
				}
			}
		}
		randNum := rand.Intn(5)
		if randNum < 0 {
			parquetPossible = false
		}
		if parquetPossible {
			createStmt.Options = append(
				createStmt.Options,
				tree.KVOption{
					Key:   changefeedbase.OptFormat,
					Value: tree.NewStrVal(string(changefeedbase.OptFormatParquet)),
				},
			)
		}
	}

	feedDir := feedSubDir()
	sinkURI := `nodelocal://1/` + feedDir
	// TODO(dan): This is a pretty unsatisfying way to test that the uri passes
	// through params it doesn't understand to ExternalStorage.
	sinkURI += `?should_be=ignored`
	if err := setURI(createStmt, sinkURI, false, &args); err != nil {
		return nil, err
	}

	// Nodelocal puts its dir under `ExternalIODir`, which is passed into
	// cloudFeedFactory.
	feedDir = filepath.Join(f.dir, feedDir)
	if err := os.Mkdir(feedDir, 0755); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}

	ss := &sinkSynchronizer{}
	wrapSink := func(s Sink) Sink {
		return &notifyFlushSink{Sink: s, sync: ss}
	}

	c := &cloudFeed{
		jobFeed:        newJobFeed(f.jobsTableConn(), wrapSink),
		ss:             ss,
		seenTrackerMap: make(map[string]struct{}),
		dir:            feedDir,
		isBare:         createStmt.Select != nil && !explicitEnvelope,
	}
	if err := f.startFeedJob(c.jobFeed, createStmt.String(), args...); err != nil {
		return nil, err
	}
	return c, nil
}

// Server implements the TestFeedFactory interface.
func (f *cloudFeedFactory) Server() serverutils.ApplicationLayerInterface {
	return f.s
}

type cloudFeed struct {
	*jobFeed
	seenTrackerMap
	ss     *sinkSynchronizer
	dir    string
	isBare bool

	resolved  string
	seenFiles map[string]struct{}
	rows      []*cdctest.TestFeedMessage
}

var _ cdctest.TestFeed = (*cloudFeed)(nil)

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
		return nil, errors.Wrapf(err, "while reparsing json '%s' marshaled from %v", printed, j)
	}
	var buf bytes.Buffer
	parsed.Format(&buf)
	return buf.Bytes(), nil
}

func extractFieldFromJSONValue(
	fieldName string, isBare bool, wrapped []byte,
) (field gojson.RawMessage, value []byte, err error) {
	parsed := make(map[string]gojson.RawMessage)

	if err := gojson.Unmarshal(wrapped, &parsed); err != nil {
		return nil, nil, errors.Wrapf(err, "unmarshalling json '%s'", wrapped)
	}

	if isBare {
		meta := make(map[string]gojson.RawMessage)
		if metaVal, haveMeta := parsed[metaSentinel]; haveMeta {
			if err := gojson.Unmarshal(metaVal, &meta); err != nil {
				return nil, nil, errors.Wrapf(err, "unmarshalling json %v", metaVal)
			}
			field = meta[fieldName]
			delete(meta, fieldName)
			if len(meta) == 0 {
				delete(parsed, metaSentinel)
			} else {
				if metaVal, err = reformatJSON(meta); err != nil {
					return nil, nil, err
				}
				parsed[metaSentinel] = metaVal
			}
		}
	} else {
		field = parsed[fieldName]
		delete(parsed, fieldName)
	}

	if value, err = reformatJSON(parsed); err != nil {
		return nil, nil, err
	}
	return field, value, nil
}

// extractKeyFromJSONValue extracts the `WITH key_in_value` key from a `WITH
// format=json, envelope=wrapped` value.
func extractKeyFromJSONValue(isBare bool, wrapped []byte) (key []byte, value []byte, err error) {
	var keyParsed gojson.RawMessage
	keyParsed, value, err = extractFieldFromJSONValue("key", isBare, wrapped)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "extracting key from json payload %s", wrapped)
	}

	if key, err = reformatJSON(keyParsed); err != nil {
		return nil, nil, err
	}
	return key, value, nil
}

// appendParquetTestFeedMessages function reads the parquet file and converts
// each row to its JSON equivalent and appends it to the cloudfeed's row object.
func (c *cloudFeed) appendParquetTestFeedMessages(
	path string, topic string, envelopeType changefeedbase.EnvelopeType,
) (err error) {
	meta, datums, err := parquet.ReadFile(path)
	if err != nil {
		return err
	}

	primaryKeyColumnsString, ok := meta.MetaFields["keyCols"]
	if !ok {
		return errors.Errorf("could not find primary key column names in parquet metadata")
	}

	columnsNamesString, ok := meta.MetaFields["allCols"]
	if !ok {
		return errors.Errorf("could not find column names in parquet metadata")
	}

	primaryKeysNamesOrdered, primaryKeyColumnSet, err := deserializeMap(primaryKeyColumnsString)
	if err != nil {
		return err
	}
	valueColumnNamesOrdered, columnNameSet, err := deserializeMap(columnsNamesString)
	if err != nil {
		return err
	}

	// Extract metadata columns into metaColumnNameSet.
	extractMetaColumns := func(columnNameSet map[string]int) map[string]int {
		metaColumnNameSet := make(map[string]int)
		for colName, colIdx := range columnNameSet {
			switch colName {
			case parquetCrdbEventTypeColName:
				metaColumnNameSet[colName] = colIdx
			case parquetOptUpdatedTimestampColName:
				metaColumnNameSet[colName] = colIdx
			case parquetOptMVCCTimestampColName:
				metaColumnNameSet[colName] = colIdx
			case parquetOptDiffColName:
				metaColumnNameSet[colName] = colIdx
			default:
			}
		}
		return metaColumnNameSet
	}
	metaColumnNameSet := extractMetaColumns(columnNameSet)

	for _, row := range datums {
		rowJSONBuilder := json.NewObjectBuilder(len(valueColumnNamesOrdered) - len(metaColumnNameSet))
		keyJSONBuilder := json.NewArrayBuilder(len(primaryKeysNamesOrdered))

		for _, primaryKeyColumnName := range primaryKeysNamesOrdered {
			datum := row[primaryKeyColumnSet[primaryKeyColumnName]]
			j, err := tree.AsJSON(datum, sessiondatapb.DataConversionConfig{}, time.UTC)
			if err != nil {
				return err
			}
			keyJSONBuilder.Add(j)

		}
		for _, valueColumnName := range valueColumnNamesOrdered {
			if _, isMeta := metaColumnNameSet[valueColumnName]; isMeta {
				continue
			}

			datum := row[columnNameSet[valueColumnName]]
			j, err := tree.AsJSON(datum, sessiondatapb.DataConversionConfig{}, time.UTC)
			if err != nil {
				return err
			}
			rowJSONBuilder.Add(valueColumnName, j)
		}

		var valueWithAfter *json.ObjectBuilder

		isDeleted := *(row[metaColumnNameSet[parquetCrdbEventTypeColName]].(*tree.DString)) == *parquetEventDelete.DString()

		if envelopeType == changefeedbase.OptEnvelopeBare {
			valueWithAfter = rowJSONBuilder
		} else {
			valueWithAfter = json.NewObjectBuilder(1)
			if err != nil {
				return err
			}
			if isDeleted {
				nullJSON, err := tree.AsJSON(tree.DNull, sessiondatapb.DataConversionConfig{}, time.UTC)
				if err != nil {
					return err
				}
				valueWithAfter.Add("after", nullJSON)
			} else {
				vbJson := rowJSONBuilder.Build()
				valueWithAfter.Add("after", vbJson)
			}

			if updatedColIdx, updated := metaColumnNameSet[parquetOptUpdatedTimestampColName]; updated {
				j, err := tree.AsJSON(row[updatedColIdx], sessiondatapb.DataConversionConfig{}, time.UTC)
				if err != nil {
					return err
				}
				valueWithAfter.Add(changefeedbase.OptUpdatedTimestamps, j)
			}
			if mvccColIdx, mvcc := metaColumnNameSet[parquetOptMVCCTimestampColName]; mvcc {
				j, err := tree.AsJSON(row[mvccColIdx], sessiondatapb.DataConversionConfig{}, time.UTC)
				if err != nil {
					return err
				}
				valueWithAfter.Add(changefeedbase.OptMVCCTimestamps, j)
			}
			if mvccColIdx, mvcc := metaColumnNameSet[parquetOptDiffColName]; mvcc {
				j, err := tree.AsJSON(row[mvccColIdx], sessiondatapb.DataConversionConfig{}, time.UTC)
				if err != nil {
					return err
				}
				valueWithAfter.Add("before", j)
			}
		}

		keyJSON := keyJSONBuilder.Build()

		rowJSON := valueWithAfter.Build()

		var keyBuf bytes.Buffer
		keyJSON.Format(&keyBuf)

		var rowBuf bytes.Buffer
		rowJSON.Format(&rowBuf)

		m := &cdctest.TestFeedMessage{
			Topic: topic,
			Value: rowBuf.Bytes(),
			Key:   keyBuf.Bytes(),
		}

		if isNew := c.markSeen(m); !isNew {
			continue
		}

		c.rows = append(c.rows, m)
	}

	return nil
}

// readParquetResolvedPayload reads a resolved timestamp value from the
// specified parquet file and returns it encoded as JSON.
func (c *cloudFeed) readParquetResolvedPayload(path string) ([]byte, error) {
	meta, datums, err := parquet.ReadFile(path)
	if err != nil {
		return nil, err
	}

	if meta.NumRows != 1 || meta.NumCols != 1 {
		return nil, errors.AssertionFailedf("expected one row with one col containing the resolved timestamp")
	}

	resolvedDatum := datums[0][0]

	resolved := resolvedRaw{Resolved: resolvedDatum.String()}
	resolvedBytes, err := gojson.Marshal(resolved)
	if err != nil {
		return nil, err
	}

	return resolvedBytes, nil
}

// Next implements the TestFeed interface.
func (c *cloudFeed) Next() (*cdctest.TestFeedMessage, error) {
	for {
		if len(c.rows) > 0 {
			e := c.rows[0]
			c.rows = c.rows[1:]
			return e, nil
		}

		if err := timeutil.RunWithTimeout(
			context.Background(), timeoutOp("cloudfeed.Next", c.jobID), timeout(),
			func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-c.ss.eventReady():
					return nil
				case <-c.shutdown:
					return c.terminalJobError()
				}
			},
		); err != nil {
			return nil, err
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

	if strings.Contains(path, "crdb_external_storage_location") {
		// Marker file created when testing "external" connection.
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

	// Skip files we processed before.
	if c.seenFiles == nil {
		c.seenFiles = make(map[string]struct{})
	}
	if _, seen := c.seenFiles[path]; seen {
		log.Infof(context.Background(), "Skip file %s", path)
		return nil
	}
	c.seenFiles[path] = struct{}{}

	details, err := c.Details()
	if err != nil {
		return err
	}
	format := changefeedbase.FormatType(details.Opts[changefeedbase.OptFormat])

	if strings.HasSuffix(path, `RESOLVED`) {
		var resolvedPayload []byte
		var err error
		if format == changefeedbase.OptFormatParquet {
			resolvedPayload, err = c.readParquetResolvedPayload(path)
			if err != nil {
				return err
			}
		} else {
			resolvedPayload, err = os.ReadFile(path)
			if err != nil {
				return err
			}
		}

		resolvedEntry := &cdctest.TestFeedMessage{Resolved: resolvedPayload}
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

	// cloud storage uses a different delimiter. Let tests be agnostic.
	topic = strings.Replace(topic, `+`, `.`, -1)

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	if format == changefeedbase.OptFormatParquet {
		envelopeType := changefeedbase.EnvelopeType(details.Opts[changefeedbase.OptEnvelope])
		return c.appendParquetTestFeedMessages(path, topic, envelopeType)
	}

	s := bufio.NewScanner(f)
	for s.Scan() {
		value := append([]byte(nil), s.Bytes()...)
		m := &cdctest.TestFeedMessage{
			Topic: topic,
			Value: value,
		}

		// NB: This is the logic for JSON. Avro will involve parsing an
		// "Object Container File".
		switch format {
		case ``, changefeedbase.OptFormatJSON:
			// Cloud storage sinks default the `WITH key_in_value` option so that
			// the key is recoverable. Extract it out of the value (also removing it
			// so the output matches the other sinks). Note that this assumes the
			// format is json, this will have to be fixed once we add format=avro
			// support to cloud storage.
			//
			// TODO(dan): Leave the key in the value if the TestFeed user
			// specifically requested it.
			if m.Key, m.Value, err = extractKeyFromJSONValue(c.isBare, m.Value); err != nil {
				return err
			}
			if isNew := c.markSeen(m); !isNew {
				continue
			}
			m.Resolved = nil
			c.rows = append(c.rows, m)
		case changefeedbase.OptFormatCSV:
			c.rows = append(c.rows, m)
		default:
			return errors.Errorf(`unknown %s: %s`, changefeedbase.OptFormat, format)
		}
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
func (tg *teeGroup) tee(
	interceptor func(*sarama.ProducerMessage) bool,
	in <-chan *sarama.ProducerMessage,
	out ...chan<- *sarama.ProducerMessage,
) {
	tg.g.Go(func() error {
		for {
			select {
			case <-tg.done:
				return nil
			case m := <-in:
				if interceptor != nil && interceptor(m) {
					continue
				}
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

type fakeKafkaClient struct {
	config *sarama.Config
}

func (c *fakeKafkaClient) Partitions(topic string) ([]int32, error) {
	return []int32{0}, nil
}

func (c *fakeKafkaClient) RefreshMetadata(topics ...string) error {
	return nil
}

func (c *fakeKafkaClient) Close() error {
	return nil
}

func (c *fakeKafkaClient) Config() *sarama.Config {
	return c.config
}

var _ kafkaClient = (*fakeKafkaClient)(nil)

type syncIgnoreCloseProducer struct {
	*syncProducerMock
}

var _ sarama.SyncProducer = (*syncIgnoreCloseProducer)(nil)

func (p *syncIgnoreCloseProducer) Close() error {
	return nil
}

type asyncIgnoreCloseProducer struct {
	*asyncProducerMock
}

var _ sarama.AsyncProducer = (*asyncIgnoreCloseProducer)(nil)

func (p *asyncIgnoreCloseProducer) Close() error {
	return nil
}

// sinkKnobs override behavior for the simulated sink.
type sinkKnobs struct {
	// Only valid for the v1 sink.
	kafkaInterceptor func(m *sarama.ProducerMessage, client kafkaClient) error
}

// fakeKafkaSink is a sink that arranges for fake kafka client and producer
// to be used.
type fakeKafkaSink struct {
	Sink
	tg     *teeGroup
	feedCh chan *sarama.ProducerMessage
	knobs  *sinkKnobs
}

var _ Sink = (*fakeKafkaSink)(nil)

// Dial implements Sink interface
func (s *fakeKafkaSink) Dial() error {
	kafka := s.Sink.(*kafkaSink)
	kafka.knobs.OverrideClientInit = func(config *sarama.Config) (kafkaClient, error) {
		client := &fakeKafkaClient{config}
		return client, nil
	}

	kafka.knobs.OverrideAsyncProducerFromClient = func(client kafkaClient) (sarama.AsyncProducer, error) {
		// The producer we give to kafka sink ignores close call.
		// This is because normally, kafka sinks owns the producer and so it closes it.
		// But in this case, if we let the sink close this producer, the test will panic
		// because we will attempt to send acknowledgements on a closed channel.
		producer := &asyncIgnoreCloseProducer{newAsyncProducerMock(100)}

		interceptor := func(m *sarama.ProducerMessage) bool {
			if s.knobs != nil && s.knobs.kafkaInterceptor != nil {
				err := s.knobs.kafkaInterceptor(m, client)
				if err != nil {
					select {
					case producer.errorsCh <- &sarama.ProducerError{Msg: m, Err: err}:
					case <-s.tg.done:
					}
					return true
				}
			}
			return false
		}

		s.tg.tee(interceptor, producer.inputCh, s.feedCh, producer.successesCh)
		return producer, nil
	}

	kafka.knobs.OverrideSyncProducerFromClient = func(client kafkaClient) (sarama.SyncProducer, error) {
		return &syncIgnoreCloseProducer{&syncProducerMock{
			overrideSend: func(m *sarama.ProducerMessage) error {
				if s.knobs != nil && s.knobs.kafkaInterceptor != nil {
					err := s.knobs.kafkaInterceptor(m, client)
					if err != nil {
						return err
					}
				}
				select {
				case s.feedCh <- m:
				case <-kafka.stopWorkerCh:
				case <-s.tg.done:
				}
				return nil
			},
		}}, nil
	}

	return kafka.Dial()
}

func (s *fakeKafkaSink) Topics() []string {
	if sink, ok := s.Sink.(*kafkaSink); ok {
		return sink.Topics()
	}
	return nil
}

// fakeKafkaSinkV2 is a sink that arranges for fake kafka client and producer
// to be used.
type fakeKafkaSinkV2 struct {
	Sink
	// For compatibility with all the other fakeKafka test stuff, we convert kgo Records to sarama messages.
	// TODO: clean this up when we remove the v1 sink.
	feedCh      chan *sarama.ProducerMessage
	t           *testing.T
	ctrl        *gomock.Controller
	client      *mocks.MockKafkaClientV2
	adminClient *mocks.MockKafkaAdminClientV2
}

var _ Sink = (*fakeKafkaSinkV2)(nil)

// Dial implements Sink interface
func (s *fakeKafkaSinkV2) Dial() error {
	bs := s.Sink.(*batchingSink)
	kc := bs.client.(*kafkaSinkClientV2)
	s.ctrl = gomock.NewController(s.t)
	s.client = mocks.NewMockKafkaClientV2(s.ctrl)
	s.client.EXPECT().ProduceSync(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, msgs ...*kgo.Record) kgo.ProduceResults {
		for _, m := range msgs {
			var key sarama.Encoder
			if m.Key != nil {
				key = sarama.ByteEncoder(m.Key)
			}
			s.feedCh <- &sarama.ProducerMessage{
				Topic:     m.Topic,
				Key:       key,
				Value:     sarama.ByteEncoder(m.Value),
				Partition: m.Partition,
			}
		}
		return nil
	}).AnyTimes()
	s.client.EXPECT().Close().AnyTimes()

	kc.client.Close()
	kc.client = s.client

	s.adminClient = mocks.NewMockKafkaAdminClientV2(s.ctrl)
	s.adminClient.EXPECT().ListTopics(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
		// Say each topic has one partition and one replica.
		td := kadm.TopicDetails{}
		for _, topic := range topics {
			td[topic] = kadm.TopicDetail{
				Topic: topic,
				Partitions: map[int32]kadm.PartitionDetail{
					0: {Topic: topic, Partition: 0, Leader: 0, Replicas: []int32{0}, ISR: []int32{0}},
				},
			}
		}
		return td, nil
	}).AnyTimes()
	kc.adminClient = s.adminClient

	return bs.Dial()
}

func (s *fakeKafkaSinkV2) Topics() []string {
	return s.Sink.(*batchingSink).topicNamer.DisplayNamesSlice()
}

type kafkaFeedFactory struct {
	enterpriseFeedFactory
	knobs *sinkKnobs
	t     *testing.T
}

var _ cdctest.TestFeedFactory = (*kafkaFeedFactory)(nil)

func mustBeKafkaFeedFactory(f cdctest.TestFeedFactory) *kafkaFeedFactory {
	switch v := f.(type) {
	case *kafkaFeedFactory:
		return v
	case *externalConnectionFeedFactory:
		return mustBeKafkaFeedFactory(v.TestFeedFactory)
	default:
		panic(fmt.Errorf("expected kafkaFeedFactory but got %+v", v))
	}
}

// makeKafkaFeedFactory returns a TestFeedFactory implementation using the `kafka` uri.
func makeKafkaFeedFactory(
	t *testing.T, srvOrCluster interface{}, rootDB *gosql.DB,
) cdctest.TestFeedFactory {
	s, injectables := getInjectables(srvOrCluster)
	return &kafkaFeedFactory{
		knobs: &sinkKnobs{},
		enterpriseFeedFactory: enterpriseFeedFactory{
			s:      s,
			db:     rootDB,
			rootDB: rootDB,
			di:     newDepInjector(injectables...),
		},
		t: t,
	}
}

func exprAsString(expr tree.Expr) (string, error) {
	evalCtx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	semaCtx := tree.MakeSemaContext(nil /* resolver */)
	te, err := expr.TypeCheck(context.Background(), &semaCtx, types.String)
	if err != nil {
		return "", err
	}
	datum, err := eval.Expr(context.Background(), evalCtx, te)
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
	defaultURI := fmt.Sprintf("%s://does.not.matter/", changefeedbase.SinkSchemeKafka)
	if err := setURI(createStmt, defaultURI, true, &args); err != nil {
		return nil, err
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
	// feedCh must have some buffer to hold the messages.
	// basically, sarama is fully async, so we have to be async as well; otherwise, tests deadlock.
	// Fixed sized buffer is probably okay at this point, but we should probably
	// have  a proper fix.
	feedCh := make(chan *sarama.ProducerMessage, 1024)
	wrapSink := func(s Sink) Sink {
		return &fakeKafkaSink{
			Sink:   s,
			tg:     tg,
			feedCh: feedCh,
			knobs:  k.knobs,
		}
	}

	// Piggyback on the existing fakeKafka stuff. TODO: clean this up when we remove the v1 sink.
	if KafkaV2Enabled.Get(&k.s.ClusterSettings().SV) {
		wrapSink = func(s Sink) Sink {
			return &fakeKafkaSinkV2{
				Sink:   s,
				feedCh: feedCh,
				t:      k.t,
			}
		}
	}

	c := &kafkaFeed{
		jobFeed:        newJobFeed(k.jobsTableConn(), wrapSink),
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
func (k *kafkaFeedFactory) Server() serverutils.ApplicationLayerInterface {
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
		if err := timeutil.RunWithTimeout(
			context.Background(), timeoutOp("kafka.Next", k.jobID), timeout(),
			func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-k.shutdown:
					return k.terminalJobError()
				case msg = <-k.source:
					return nil
				}
			},
		); err != nil {
			return nil, err
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

// DiscardMessages spins up a goroutine to discard messages produced into the
// sink. Returns cleanup function.
func DiscardMessages(f cdctest.TestFeed) func() {
	switch t := f.(type) {
	case *kafkaFeed:
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.shutdown:
					return
				case <-t.source:
				}
			}
		}()
		return func() {
			cancel()
			wg.Wait()
		}
	default:
		return func() {}
	}
}

type webhookFeedFactory struct {
	enterpriseFeedFactory
	useSecureServer bool
}

var _ cdctest.TestFeedFactory = (*webhookFeedFactory)(nil)

// makeWebhookFeedFactory returns a TestFeedFactory implementation using the `webhook-webhooks` uri.
func makeWebhookFeedFactory(srvOrCluster interface{}, rootDB *gosql.DB) cdctest.TestFeedFactory {
	s, injectables := getInjectables(srvOrCluster)
	useSecure := rand.Float32() < 0.5
	return &webhookFeedFactory{
		enterpriseFeedFactory: enterpriseFeedFactory{
			s:      s,
			db:     rootDB,
			rootDB: rootDB,
			di:     newDepInjector(injectables...),
		},
		useSecureServer: useSecure,
	}
}

func (f *webhookFeedFactory) Feed(create string, args ...interface{}) (cdctest.TestFeed, error) {
	parsed, err := parser.ParseOne(create)
	if err != nil {
		return nil, err
	}
	createStmt := parsed.AST.(*tree.CreateChangefeed)

	// required value
	createStmt.Options = append(createStmt.Options, tree.KVOption{Key: changefeedbase.OptTopicInValue})
	if createStmt.Select != nil {
		// Normally, webhook requires key_in_value; but if we use bare envelope,
		// this option is not required.  However, we need it to make this
		// test feed work -- so, set it.
		createStmt.Options = append(createStmt.Options, tree.KVOption{Key: changefeedbase.OptKeyInValue})
	}
	var sinkDest *cdctest.MockWebhookSink

	cert, _, err := cdctest.NewCACertBase64Encoded()
	if err != nil {
		return nil, err
	}

	if f.useSecureServer {
		sinkDest, err = cdctest.StartMockWebhookSinkSecure(cert)
		if err != nil {
			return nil, err
		}

		clientCertPEM, clientKeyPEM, err := cdctest.GenerateClientCertAndKey(cert)
		if err != nil {
			return nil, err
		}

		uri := fmt.Sprintf(
			"webhook-%s?insecure_tls_skip_verify=true&client_cert=%s&client_key=%s",
			sinkDest.URL(), base64.StdEncoding.EncodeToString(clientCertPEM),
			base64.StdEncoding.EncodeToString(clientKeyPEM))

		if err := setURI(createStmt, uri, true, &args); err != nil {
			return nil, err
		}
	} else {
		sinkDest, err = cdctest.StartMockWebhookSink(cert)
		if err != nil {
			return nil, err
		}

		uri := fmt.Sprintf("webhook-%s?insecure_tls_skip_verify=true", sinkDest.URL())
		if err := setURI(createStmt, uri, true, &args); err != nil {
			return nil, err
		}
	}

	ss := &sinkSynchronizer{}
	wrapSink := func(s Sink) Sink {
		return &notifyFlushSink{Sink: s, sync: ss}
	}

	explicitEnvelope := false
	for _, opt := range createStmt.Options {
		if string(opt.Key) == changefeedbase.OptEnvelope {
			explicitEnvelope = true
		}
	}

	c := &webhookFeed{
		jobFeed:        newJobFeed(f.jobsTableConn(), wrapSink),
		seenTrackerMap: make(map[string]struct{}),
		ss:             ss,
		isBare:         createStmt.Select != nil && !explicitEnvelope,
		mockSink:       sinkDest,
	}
	if err := f.startFeedJob(c.jobFeed, createStmt.String(), args...); err != nil {
		sinkDest.Close()
		return nil, err
	}
	return c, nil
}

func (f *webhookFeedFactory) Server() serverutils.ApplicationLayerInterface {
	return f.s
}

type webhookFeed struct {
	*jobFeed
	seenTrackerMap
	ss       *sinkSynchronizer
	isBare   bool
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
func extractTopicFromJSONValue(
	isBare bool, wrapped []byte,
) (topic string, value []byte, err error) {
	var topicRaw gojson.RawMessage
	topicRaw, value, err = extractFieldFromJSONValue("topic", isBare, wrapped)
	if err != nil {
		return "", nil, err
	}
	if err := gojson.Unmarshal(topicRaw, &topic); err != nil {
		return "", nil, err
	}
	return topic, value, nil
}

type webhookSinkTestfeedPayload struct {
	Payload []gojson.RawMessage `json:"payload"`
	Length  int                 `json:"length"`
}

// extractValueFromJSONMessage extracts the value of the first element of
// the payload array from an webhook sink JSON message.
func extractValueFromJSONMessage(message []byte) (val []byte, err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "message was '%s'", message)
		}
	}()
	var parsed webhookSinkTestfeedPayload
	if err := gojson.Unmarshal(message, &parsed); err != nil {
		return nil, err
	}
	keyParsed := parsed.Payload
	if len(keyParsed) <= 0 {
		return nil, fmt.Errorf("payload value in json message contains no elements")
	}

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
				details, err := f.Details()
				if err != nil {
					return nil, err
				}
				switch v := changefeedbase.FormatType(details.Opts[changefeedbase.OptFormat]); v {
				case ``, changefeedbase.OptFormatJSON:
					resolved, err := isResolvedTimestamp([]byte(msg))
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
						if m.Key, m.Value, err = extractKeyFromJSONValue(f.isBare, wrappedValue); err != nil {
							return nil, err
						}
						if m.Topic, m.Value, err = extractTopicFromJSONValue(f.isBare, m.Value); err != nil {
							return nil, err
						}
						if isNew := f.markSeen(m); !isNew {
							continue
						}
					}
				case changefeedbase.OptFormatCSV:
					m.Value = []byte(msg)
					return m, nil
				default:
					return nil, errors.Errorf(`unknown %s: %s`, changefeedbase.OptFormat, v)
				}
				return m, nil
			}
			m.Key, m.Value = nil, nil
			return m, nil
		}

		if err := timeutil.RunWithTimeout(
			context.Background(), timeoutOp("webhook.Next", f.jobID), timeout(),
			func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-f.ss.eventReady():
					return nil
				case <-f.mockSink.NotifyMessage():
					return nil
				case <-f.shutdown:
					return f.terminalJobError()
				}
			},
		); err != nil {
			return nil, err
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

type mockPubsubMessage struct {
	data string
	// attributes are only populated for the non-deprecated pubsub sink.
	attributes map[string]string
	// topic is only populated for the non-deprecated pubsub sink.
	topic string
}

type deprecatedMockPubsubMessageBuffer struct {
	mu   syncutil.Mutex
	rows []mockPubsubMessage
}

func (p *deprecatedMockPubsubMessageBuffer) pop() *mockPubsubMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.rows) == 0 {
		return nil
	}
	var head mockPubsubMessage
	head, p.rows = p.rows[0], p.rows[1:]
	return &head
}

func (p *deprecatedMockPubsubMessageBuffer) push(m mockPubsubMessage) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rows = append(p.rows, m)
}

type deprecatedFakePubsubClient struct {
	buffer *deprecatedMockPubsubMessageBuffer
}

var _ deprecatedPubsubClient = (*deprecatedFakePubsubClient)(nil)

func (p *deprecatedFakePubsubClient) init() error {
	return nil
}

func (p *deprecatedFakePubsubClient) close() error {
	return nil
}

// sendMessage sends a message to the topic
func (p *deprecatedFakePubsubClient) sendMessage(m []byte, _ string, _ string) error {
	message := mockPubsubMessage{data: string(m)}
	p.buffer.push(message)
	return nil
}

func (p *deprecatedFakePubsubClient) sendMessageToAllTopics(m []byte) error {
	message := mockPubsubMessage{data: string(m)}
	p.buffer.push(message)
	return nil
}

func (p *deprecatedFakePubsubClient) flushTopics() {
}

type deprecatedFakePubsubSink struct {
	Sink
	client *deprecatedFakePubsubClient
	sync   *sinkSynchronizer
}

var _ Sink = (*deprecatedFakePubsubSink)(nil)

func (p *deprecatedFakePubsubSink) Dial() error {
	s := p.Sink.(*deprecatedPubsubSink)
	s.client = p.client
	s.setupWorkers()
	return nil
}

func (p *deprecatedFakePubsubSink) Flush(ctx context.Context) error {
	defer p.sync.addFlush()
	return p.Sink.Flush(ctx)
}

func (p *deprecatedFakePubsubClient) connectivityErrorLocked() error {
	return nil
}

type fakePubsubServer struct {
	srv *pstest.Server
	mu  struct {
		syncutil.Mutex
		buffer []mockPubsubMessage
		notify chan struct{}
	}
}

func makeFakePubsubServer() *fakePubsubServer {
	mockServer := fakePubsubServer{}
	mockServer.mu.buffer = make([]mockPubsubMessage, 0)
	mockServer.srv = pstest.NewServer(pstest.ServerReactorOption{
		FuncName: "Publish",
		Reactor:  &mockServer,
	})
	return &mockServer
}

var _ pstest.Reactor = (*fakePubsubServer)(nil)

func (ps *fakePubsubServer) React(req interface{}) (handled bool, ret interface{}, err error) {
	publishReq, ok := req.(*pb.PublishRequest)
	if ok {
		ps.mu.Lock()
		defer ps.mu.Unlock()

		for _, msg := range publishReq.Messages {
			ps.mu.buffer = append(ps.mu.buffer,
				mockPubsubMessage{data: string(msg.Data), topic: publishReq.Topic, attributes: msg.Attributes})
		}
		if ps.mu.notify != nil {
			notifyCh := ps.mu.notify
			ps.mu.notify = nil
			close(notifyCh)
		}
	}

	return false, nil, nil
}

func (s *fakePubsubServer) NotifyMessage() chan struct{} {
	c := make(chan struct{})
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.mu.buffer) > 0 {
		close(c)
	} else {
		s.mu.notify = c
	}
	return c
}

func (ps *fakePubsubServer) Dial() (*grpc.ClientConn, error) {
	return grpc.Dial(ps.srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func (ps *fakePubsubServer) Pop() *mockPubsubMessage {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if len(ps.mu.buffer) == 0 {
		return nil
	}
	var head mockPubsubMessage
	head, ps.mu.buffer = ps.mu.buffer[0], ps.mu.buffer[1:]
	return &head
}

func (ps *fakePubsubServer) Close() error {
	ps.srv.Wait()
	return ps.srv.Close()
}

type pubsubFeedFactory struct {
	enterpriseFeedFactory
}

var _ cdctest.TestFeedFactory = (*pubsubFeedFactory)(nil)

// makePubsubFeedFactory returns a TestFeedFactory implementation using the `pubsub` uri.
func makePubsubFeedFactory(srvOrCluster interface{}, rootDB *gosql.DB) cdctest.TestFeedFactory {
	s, injectables := getInjectables(srvOrCluster)

	switch t := srvOrCluster.(type) {
	case serverutils.ApplicationLayerInterface:
		t.DistSQLServer().(*distsql.ServerImpl).TestingKnobs.Changefeed.(*TestingKnobs).PubsubClientSkipClientCreation = true
	case serverutils.TestClusterInterface:
		servers := make([]feedInjectable, t.NumServers())
		for i := range servers {
			t.Server(i).DistSQLServer().(*distsql.ServerImpl).TestingKnobs.Changefeed.(*TestingKnobs).PubsubClientSkipClientCreation = true
		}
	}

	return &pubsubFeedFactory{
		enterpriseFeedFactory: enterpriseFeedFactory{
			s:      s,
			db:     rootDB,
			rootDB: rootDB,
			di:     newDepInjector(injectables...),
		},
	}
}

// Feed implements cdctest.TestFeedFactory
func (p *pubsubFeedFactory) Feed(create string, args ...interface{}) (cdctest.TestFeed, error) {
	parsed, err := parser.ParseOne(create)
	if err != nil {
		return nil, err
	}
	createStmt := parsed.AST.(*tree.CreateChangefeed)
	if createStmt.SinkURI == nil {
		err = setURI(createStmt, GcpScheme+"://testfeed?region=testfeedRegion", true, &args)
		if err != nil {
			return nil, err
		}
	}

	mockServer := makeFakePubsubServer()

	deprecatedClient := &deprecatedFakePubsubClient{
		buffer: &deprecatedMockPubsubMessageBuffer{
			rows: make([]mockPubsubMessage, 0),
		},
	}

	ss := &sinkSynchronizer{}
	var mu syncutil.Mutex
	wrapSink := func(s Sink) Sink {
		mu.Lock() // Called concurrently due to getEventSink and getResolvedTimestampSink
		defer mu.Unlock()
		if batchingSink, ok := s.(*batchingSink); ok {
			if sinkClient, ok := batchingSink.client.(*pubsubSinkClient); ok {
				conn, _ := mockServer.Dial()
				mockClient, _ := pubsubv1.NewPublisherClient(context.Background(), option.WithGRPCConn(conn))
				sinkClient.client = mockClient
			}
			return &notifyFlushSink{Sink: s, sync: ss}
		} else if _, ok := s.(*deprecatedPubsubSink); ok {
			return &deprecatedFakePubsubSink{
				Sink:   s,
				client: deprecatedClient,
				sync:   ss,
			}
		}
		return s
	}

	c := &pubsubFeed{
		jobFeed:          newJobFeed(p.jobsTableConn(), wrapSink),
		seenTrackerMap:   make(map[string]struct{}),
		ss:               ss,
		mockServer:       mockServer,
		deprecatedClient: deprecatedClient,
	}

	if err := p.startFeedJob(c.jobFeed, createStmt.String(), args...); err != nil {
		_ = mockServer.Close()
		return nil, err
	}
	return c, nil
}

// Server implements TestFeedFactory
func (p *pubsubFeedFactory) Server() serverutils.ApplicationLayerInterface {
	return p.s
}

type pubsubFeed struct {
	*jobFeed
	seenTrackerMap
	ss               *sinkSynchronizer
	mockServer       *fakePubsubServer
	deprecatedClient *deprecatedFakePubsubClient
}

var _ cdctest.TestFeed = (*pubsubFeed)(nil)

// Partitions implements TestFeed
func (p *pubsubFeed) Partitions() []string {
	return []string{``}
}

// extractJSONMessagePubsub extracts the value, key, and topic from a pubsub message
func extractJSONMessagePubsub(wrapped []byte) (value []byte, key []byte, topic string, err error) {
	parsed := jsonPayload{}
	err = gojson.Unmarshal(wrapped, &parsed)
	if err != nil {
		return
	}
	valueParsed := parsed.Value
	keyParsed := parsed.Key
	topic = parsed.Topic

	value, err = reformatJSON(valueParsed)
	if err != nil {
		return
	}
	key, err = reformatJSON(keyParsed)
	if err != nil {
		return
	}
	return
}

// Next implements TestFeed
func (p *pubsubFeed) Next() (*cdctest.TestFeedMessage, error) {
	for {
		deprecatedMessage := false
		msg := p.mockServer.Pop()
		if msg == nil {
			deprecatedMessage = true
			msg = p.deprecatedClient.buffer.pop()
		}
		if msg != nil {
			details, err := p.Details()
			if err != nil {
				return nil, err
			}

			m := &cdctest.TestFeedMessage{
				RawMessage: msg,
			}
			switch v := changefeedbase.FormatType(details.Opts[changefeedbase.OptFormat]); v {
			case ``, changefeedbase.OptFormatJSON:
				resolved, err := isResolvedTimestamp([]byte(msg.data))
				if err != nil {
					return nil, err
				}
				msgBytes := []byte(msg.data)
				if resolved {
					m.Resolved = msgBytes
					if !deprecatedMessage {
						m.Topic = msg.topic
					}
				} else {
					m.Value, m.Key, m.Topic, err = extractJSONMessagePubsub(msgBytes)
					if err != nil {
						return nil, err
					}
					if isNew := p.markSeen(m); !isNew {
						continue
					}
				}
			case changefeedbase.OptFormatCSV:
				m.Value = []byte(msg.data)
			default:
				return nil, errors.Errorf(`unknown %s: %s`, changefeedbase.OptFormat, v)
			}

			return m, nil
		}

		if err := p.waitForMessage(); err != nil {
			return nil, err
		}
	}
}

func (p *pubsubFeed) waitForMessage() error {
	return timeutil.RunWithTimeout(
		context.Background(), timeoutOp("pubsub.Next", p.jobID), timeout(),
		func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-p.ss.eventReady():
				return nil
			case <-p.mockServer.NotifyMessage():
				return nil
			case <-p.shutdown:
				return p.terminalJobError()
			}
		},
	)
}

// Close implements TestFeed
func (p *pubsubFeed) Close() error {
	err := p.jobFeed.Close()
	if err != nil {
		return err
	}
	_ = p.mockServer.Close()
	return nil
}

type mockPulsarServer struct {
	msgCh chan *pulsar.ProducerMessage
}

func makeMockPulsarServer() *mockPulsarServer {
	return &mockPulsarServer{
		msgCh: make(chan *pulsar.ProducerMessage, 2048),
	}
}

type mockPulsarClient struct {
	pulsarServer *mockPulsarServer
}

var _ PulsarClient = (*mockPulsarClient)(nil)

func (pc *mockPulsarClient) CreateProducer(opts pulsar.ProducerOptions) (pulsar.Producer, error) {
	if opts.BatcherBuilderType != pulsar.KeyBasedBatchBuilder {
		panic("unexpected batch builder type")
	}
	if opts.Topic == "" {
		panic("expected topic for producer")
	}
	return &mockPulsarProducer{
		topic:        opts.Topic,
		pulsarServer: pc.pulsarServer,
	}, nil
}

func (pc *mockPulsarClient) Close() {}

type mockPulsarProducer struct {
	topic        string
	pulsarServer *mockPulsarServer
}

func (p *mockPulsarProducer) Topic() string {
	return p.topic
}

func (p *mockPulsarProducer) Name() string {
	panic("unimplemented")
}

func (p *mockPulsarProducer) Send(
	context.Context, *pulsar.ProducerMessage,
) (pulsar.MessageID, error) {
	panic("unimplemented")
}

// TODO (#118899): for better testing, we should make this async and
// implement Flush(), to simulate a real scenario. It would also
// be ideal to make batches with the supplied ordering key.
// For now, this is sufficient. End-to-end correctness testing will be addressed
// by #118859.
func (p *mockPulsarProducer) SendAsync(
	ctx context.Context,
	m *pulsar.ProducerMessage,
	f func(pulsar.MessageID, *pulsar.ProducerMessage, error),
) {
	select {
	case <-ctx.Done():
		f(nil, m, ctx.Err())
	case p.pulsarServer.msgCh <- m:
		f(nil, m, nil)
	}
}

func (p *mockPulsarProducer) LastSequenceID() int64 {
	panic("unimplemented")
}

func (p *mockPulsarProducer) Flush() error {
	return nil
}

func (p *mockPulsarProducer) Close() {}

type pulsarFeedFactory struct {
	enterpriseFeedFactory
}

var _ cdctest.TestFeedFactory = (*pulsarFeedFactory)(nil)

// makePulsarFeedFactory returns a TestFeedFactory implementation using the `pulsar` uri.
func makePulsarFeedFactory(srvOrCluster interface{}, rootDB *gosql.DB) cdctest.TestFeedFactory {
	s, injectables := getInjectables(srvOrCluster)

	switch t := srvOrCluster.(type) {
	case serverutils.ApplicationLayerInterface:
		t.DistSQLServer().(*distsql.ServerImpl).TestingKnobs.Changefeed.(*TestingKnobs).PulsarClientSkipCreation = true
	case serverutils.TestClusterInterface:
		servers := make([]feedInjectable, t.NumServers())
		for i := range servers {
			t.Server(i).DistSQLServer().(*distsql.ServerImpl).TestingKnobs.Changefeed.(*TestingKnobs).PulsarClientSkipCreation = true
		}
	}

	return &pulsarFeedFactory{
		enterpriseFeedFactory: enterpriseFeedFactory{
			s:      s,
			db:     rootDB,
			rootDB: rootDB,
			di:     newDepInjector(injectables...),
		},
	}
}

// Feed implements cdctest.TestFeedFactory
func (p *pulsarFeedFactory) Feed(create string, args ...interface{}) (cdctest.TestFeed, error) {
	parsed, err := parser.ParseOne(create)
	if err != nil {
		return nil, err
	}
	createStmt := parsed.AST.(*tree.CreateChangefeed)
	if createStmt.SinkURI == nil {
		err = setURI(createStmt, changefeedbase.SinkSchemePulsar+"://testfeed?region=testfeedRegion", true, &args)
		if err != nil {
			return nil, err
		}
	}

	mockServer := makeMockPulsarServer()

	var mu syncutil.Mutex
	wrapSink := func(s Sink) Sink {
		mu.Lock() // Called concurrently due to getEventSink and getResolvedTimestampSink
		defer mu.Unlock()
		pulsarSink := s.(*pulsarSink)
		pulsarSink.client = &mockPulsarClient{
			pulsarServer: mockServer,
		}
		if err := pulsarSink.initTopicProducers(); err != nil {
			panic(err)
		}
		return s
	}

	c := &pulsarFeed{
		jobFeed:        newJobFeed(p.jobsTableConn(), wrapSink),
		seenTrackerMap: make(map[string]struct{}),
		pulsarServer:   mockServer,
	}

	if err := p.startFeedJob(c.jobFeed, createStmt.String(), args...); err != nil {
		return nil, err
	}
	return c, nil
}

// Server implements TestFeedFactory
func (p *pulsarFeedFactory) Server() serverutils.ApplicationLayerInterface {
	return p.s
}

type pulsarFeed struct {
	*jobFeed
	seenTrackerMap
	pulsarServer *mockPulsarServer
}

var _ cdctest.TestFeed = (*pulsarFeed)(nil)

// Partitions implements TestFeed
func (p *pulsarFeed) Partitions() []string {
	return []string{``}
}

// Next implements TestFeed
func (p *pulsarFeed) Next() (*cdctest.TestFeedMessage, error) {
	for {
		var msg *pulsar.ProducerMessage
		if err := timeutil.RunWithTimeout(
			context.Background(), timeoutOp("pulsar.Next", p.jobID), timeout(),
			func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-p.shutdown:
					return p.terminalJobError()
				case msg = <-p.pulsarServer.msgCh:
					return nil
				}
			},
		); err != nil {
			return nil, err
		}

		details, err := p.Details()
		if err != nil {
			return nil, err
		}

		m := &cdctest.TestFeedMessage{
			RawMessage: msg,
		}
		switch v := changefeedbase.FormatType(details.Opts[changefeedbase.OptFormat]); v {
		case ``, changefeedbase.OptFormatJSON:
			resolved, err := isResolvedTimestamp(msg.Payload)
			if err != nil {
				return nil, err
			}
			msgBytes := msg.Payload
			if resolved {
				m.Resolved = msgBytes
			} else {
				m.Value, m.Key, m.Topic, err = extractJSONMessagePubsub(msgBytes)
				if err != nil {
					return nil, err
				}
				if isNew := p.markSeen(m); !isNew {
					continue
				}
			}
		case changefeedbase.OptFormatCSV:
			m.Value = msg.Payload
		default:
			return nil, errors.Errorf(`unknown %s: %s`, changefeedbase.OptFormat, v)
		}

		return m, nil
	}

}

// Close implements TestFeed
func (p *pulsarFeed) Close() error {
	err := p.jobFeed.Close()
	if err != nil {
		return err
	}
	return nil
}

// stopFeedWhenDone arranges for feed to stop when passed in context
// is done. Returns cleanup function.
func stopFeedWhenDone(ctx context.Context, f cdctest.TestFeed) func() {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)

	whenDone := func(fn func()) {
		defer wg.Done()
		select {
		case <-done:
		case <-ctx.Done():
			fn()
		}
	}

	switch t := f.(type) {
	case *sinklessFeed:
		go whenDone(func() {
			_ = t.conn.Close(context.Background())
		})
	case jobFailedMarker:
		go whenDone(func() {
			t.jobFailed(errors.New("stopping job due to TestFeed timeout"))
		})
	}

	return func() {
		close(done)
		wg.Wait()
	}
}
