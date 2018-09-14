// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"bytes"
	"context"
	gosql "database/sql"
	gojson "encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"

	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
)

type benchSink struct {
	syncutil.Mutex
	cond      *sync.Cond
	emits     int
	emitBytes int64
}

func makeBenchSink() *benchSink {
	s := &benchSink{}
	s.cond = sync.NewCond(&s.Mutex)
	return s
}

func (s *benchSink) EmitRow(ctx context.Context, _ string, k, v []byte) error {
	return s.emit(int64(len(k) + len(v)))
}
func (s *benchSink) EmitResolvedTimestamp(_ context.Context, p []byte) error {
	return s.emit(int64(len(p)))
}
func (s *benchSink) Flush(_ context.Context) error { return nil }
func (s *benchSink) Close() error                  { return nil }
func (s *benchSink) emit(bytes int64) error {
	s.Lock()
	defer s.Unlock()
	s.emits++
	s.emitBytes += bytes
	s.cond.Broadcast()
	return nil
}

// WaitForEmit blocks until at least one thing is emitted by the sink. It
// returns the number of emitted messages and bytes since the last WaitForEmit.
func (s *benchSink) WaitForEmit() (int, int64) {
	s.Lock()
	defer s.Unlock()
	for s.emits == 0 {
		s.cond.Wait()
	}
	emits, emitBytes := s.emits, s.emitBytes
	s.emits, s.emitBytes = 0, 0
	return emits, emitBytes
}

// createBenchmarkChangefeed starts a stripped down changefeed. It watches
// `database.table` and outputs to `sinkURI`. The given `feedClock` is only used
// for the internal ExportRequest polling, so a benchmark can write data with
// different timestamps beforehand and simulate the changefeed going through
// them in steps.
//
// The returned sink can be used to count emits and the closure handed back
// cancels the changefeed (blocking until it's shut down) and returns an error
// if the changefeed had failed before the closure was called.
//
// This intentionally skips the distsql and sink parts to keep the benchmark
// focused on the core changefeed work, but it does include the poller.
func createBenchmarkChangefeed(
	ctx context.Context,
	s serverutils.TestServerInterface,
	feedClock *hlc.Clock,
	database, table string,
) (*benchSink, func() error) {
	tableDesc := sqlbase.GetTableDescriptor(s.DB(), database, table)
	spans := []roachpb.Span{tableDesc.PrimaryIndexSpan()}
	details := jobspb.ChangefeedDetails{
		Targets: jobspb.ChangefeedTargets{tableDesc.ID: jobspb.ChangefeedTarget{
			StatementTimeName: tableDesc.Name,
		}},
		Opts: map[string]string{
			optEnvelope: string(optEnvelopeRow),
		},
	}
	initialHighWater := hlc.Timestamp{}
	sink := makeBenchSink()

	buf := makeBuffer()
	poller := makePoller(
		s.ClusterSettings(), s.DB(), feedClock, s.Gossip(), spans, details, initialHighWater, buf)

	th := makeTableHistory(func(*sqlbase.TableDescriptor) error { return nil }, initialHighWater)
	thUpdater := &tableHistoryUpdater{
		settings: s.ClusterSettings(),
		db:       s.DB(),
		targets:  details.Targets,
		m:        th,
	}
	rowsFn := kvsToRows(s.LeaseManager().(*sql.LeaseManager), th, details, buf.Get)
	tickFn := emitEntries(details, sink, rowsFn, TestingKnobs{})

	ctx, cancel := context.WithCancel(ctx)
	go func() { _ = poller.Run(ctx) }()
	go func() { _ = thUpdater.PollTableDescs(ctx) }()

	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := func() error {
			sf := makeSpanFrontier(spans...)
			for {
				// This is basically the ChangeAggregator processor.
				resolvedSpans, err := tickFn(ctx)
				if err != nil {
					return err
				}
				// This is basically the ChangeFrontier processor, the resolved
				// spans are normally sent using distsql, so we're missing a bit
				// of overhead here.
				for _, rs := range resolvedSpans {
					if sf.Forward(rs.Span, rs.Timestamp) {
						if err := emitResolvedTimestamp(ctx, details, sink, nil, sf); err != nil {
							return err
						}
					}
				}
			}
		}()
		errCh <- err
	}()
	cancelFn := func() error {
		select {
		case err := <-errCh:
			return err
		default:
		}
		cancel()
		wg.Wait()
		return nil
	}
	return sink, cancelFn
}

// loadWorkloadBatches inserts a workload.Table's row batches, each in one
// transaction. It returns the timestamps of these transactions and the byte
// size for use with b.SetBytes.
func loadWorkloadBatches(sqlDB *gosql.DB, table workload.Table) ([]time.Time, int64, error) {
	if _, err := sqlDB.Exec(`CREATE TABLE "` + table.Name + `" ` + table.Schema); err != nil {
		return nil, 0, err
	}

	var now time.Time
	var timestamps []time.Time
	var benchBytes int64

	var insertStmtBuf bytes.Buffer
	var params []interface{}
	for batchIdx := 0; batchIdx < table.InitialRows.NumBatches; batchIdx++ {
		if _, err := sqlDB.Exec(`BEGIN`); err != nil {
			return nil, 0, err
		}

		params = params[:0]
		insertStmtBuf.Reset()
		insertStmtBuf.WriteString(`INSERT INTO "` + table.Name + `" VALUES `)
		for _, row := range table.InitialRows.Batch(batchIdx) {
			if len(params) != 0 {
				insertStmtBuf.WriteString(`,`)
			}
			insertStmtBuf.WriteString(`(`)
			for colIdx, datum := range row {
				if colIdx != 0 {
					insertStmtBuf.WriteString(`,`)
				}
				benchBytes += workload.ApproxDatumSize(datum)
				params = append(params, datum)
				fmt.Fprintf(&insertStmtBuf, `$%d`, len(params))
			}
			insertStmtBuf.WriteString(`)`)
		}
		if _, err := sqlDB.Exec(insertStmtBuf.String(), params...); err != nil {
			return nil, 0, err
		}

		if err := sqlDB.QueryRow(`SELECT transaction_timestamp(); COMMIT;`).Scan(&now); err != nil {
			return nil, 0, err
		}
		timestamps = append(timestamps, now)
	}

	if table.InitialRows.NumTotal != 0 {
		var totalRows int
		if err := sqlDB.QueryRow(
			`SELECT count(*) FROM "` + table.Name + `"`,
		).Scan(&totalRows); err != nil {
			return nil, 0, err
		}
		if table.InitialRows.NumTotal != totalRows {
			return nil, 0, errors.Errorf(`sanity check failed: expected %d rows got %d`,
				table.InitialRows.NumTotal, totalRows)
		}
	}

	return timestamps, benchBytes, nil
}

type testfeedFactory interface {
	Feed(t testing.TB, create string, args ...interface{}) testfeed
	Server() serverutils.TestServerInterface
}

type testfeed interface {
	Partitions() []string
	Next(t testing.TB) (topic, partition string, key, value, payload []byte, ok bool)
	Err() error
	Close(t testing.TB)
}

type sinklessFeedFactory struct {
	s  serverutils.TestServerInterface
	db *gosql.DB
}

func makeSinkless(s serverutils.TestServerInterface, db *gosql.DB) *sinklessFeedFactory {
	return &sinklessFeedFactory{s: s, db: db}
}

func (f *sinklessFeedFactory) Feed(t testing.TB, create string, args ...interface{}) testfeed {
	t.Helper()

	if _, err := f.db.Exec(
		`SET CLUSTER SETTING changefeed.experimental_poll_interval = '0ns'`,
	); err != nil {
		t.Fatal(err)
	}

	s := &sinklessFeed{db: f.db}
	now := timeutil.Now()
	var err error
	s.rows, err = s.db.Query(create, args...)
	if err != nil {
		t.Fatal(err)
	}
	queryIDRows, err := s.db.Query(
		`SELECT query_id FROM [SHOW QUERIES] WHERE query LIKE 'CREATE CHANGEFEED%' AND start > $1`,
		now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !queryIDRows.Next() {
		t.Fatalf(`could not find query id`)
	}
	if err := queryIDRows.Scan(&s.queryID); err != nil {
		t.Fatal(err)
	}
	if queryIDRows.Next() {
		t.Fatalf(`found too many query ids`)
	}
	return s
}

func (f *sinklessFeedFactory) Server() serverutils.TestServerInterface {
	return f.s
}

type sinklessFeed struct {
	db      *gosql.DB
	rows    *gosql.Rows
	queryID string
}

func (c *sinklessFeed) Partitions() []string { return []string{`sinkless`} }

func (c *sinklessFeed) Next(
	t testing.TB,
) (topic, partition string, key, value, resolved []byte, ok bool) {
	t.Helper()
	partition = `sinkless`
	var noKey, noValue, noResolved []byte
	if !c.rows.Next() {
		return ``, ``, nil, nil, nil, false
	}
	var maybeTopic gosql.NullString
	if err := c.rows.Scan(&maybeTopic, &key, &value); err != nil {
		t.Fatal(err)
	}
	if maybeTopic.Valid {
		return maybeTopic.String, partition, key, value, noResolved, true
	}
	resolvedPayload := value
	return ``, partition, noKey, noValue, resolvedPayload, true
}

func (c *sinklessFeed) Err() error {
	if c.rows != nil {
		return c.rows.Err()
	}
	return nil
}

func (c *sinklessFeed) Close(t testing.TB) {
	t.Helper()
	// TODO(dan): We should just be able to close the `gosql.Rows` but that
	// currently blocks forever without this.
	if _, err := c.db.Exec(`CANCEL QUERY IF EXISTS $1`, c.queryID); err != nil {
		t.Error(err)
	}
	// Ignore the error because we just force canceled the feed.
	_ = c.rows.Close()
}

type tableFeedFactory struct {
	s       serverutils.TestServerInterface
	db      *gosql.DB
	flushCh chan struct{}
}

func makeTable(
	s serverutils.TestServerInterface, db *gosql.DB, flushCh chan struct{},
) *tableFeedFactory {
	return &tableFeedFactory{s: s, db: db, flushCh: flushCh}
}

func (f *tableFeedFactory) Feed(t testing.TB, create string, args ...interface{}) testfeed {
	t.Helper()

	sink, cleanup := sqlutils.PGUrl(t, f.s.ServingAddr(), t.Name(), url.User(security.RootUser))
	sink.Path = fmt.Sprintf(`table_%d`, timeutil.Now().UnixNano())

	db, err := gosql.Open("postgres", sink.String())
	if err != nil {
		t.Fatal(err)
	}

	sink.Scheme = sinkSchemeExperimentalSQL
	c := &tableFeed{db: db, urlCleanup: cleanup, sinkURI: sink.String(), flushCh: f.flushCh}
	if _, err := c.db.Exec(
		`SET CLUSTER SETTING changefeed.experimental_poll_interval = '0ns'`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := c.db.Exec(`CREATE DATABASE ` + sink.Path); err != nil {
		t.Fatal(err)
	}

	parsed, err := parser.ParseOne(create)
	if err != nil {
		t.Fatal(err)
	}
	createStmt := parsed.(*tree.CreateChangefeed)
	if createStmt.SinkURI != nil {
		t.Fatalf(`unexpected sink provided: "INTO %s"`, tree.AsString(createStmt.SinkURI))
	}
	createStmt.SinkURI = tree.NewStrVal(c.sinkURI)

	if err := f.db.QueryRow(createStmt.String(), args...).Scan(&c.jobID); err != nil {
		t.Fatal(err)
	}
	return c
}

func (f *tableFeedFactory) Server() serverutils.TestServerInterface {
	return f.s
}

type tableFeed struct {
	db         *gosql.DB
	sinkURI    string
	urlCleanup func()
	jobID      int64
	flushCh    chan struct{}

	rows   *gosql.Rows
	jobErr error
}

func (c *tableFeed) Partitions() []string {
	// The sqlSink hardcodes these.
	return []string{`0`, `1`, `2`}
}

func (c *tableFeed) Next(
	t testing.TB,
) (topic, partition string, key, value, payload []byte, ok bool) {
	// sinkSink writes all changes to a table with primary key of topic,
	// partition, message_id. To simulate the semantics of kafka, message_ids
	// are only comparable within a given (topic, partition). Internally the
	// message ids are generated as a 64 bit int with a timestamp in bits 1-49
	// and a hash of the partition in 50-64. This tableFeed.Next function works
	// by repeatedly fetching and deleting all rows in the table. Then it pages
	// through the results until they are empty and repeats.
	//
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
	for {
		if c.rows != nil && c.rows.Next() {
			var msgID int64
			if err := c.rows.Scan(&topic, &partition, &msgID, &key, &value, &payload); err != nil {
				t.Fatal(err)
			}
			// Scan turns NULL bytes columns into a 0-length, non-nil byte
			// array, which is pretty unexpected. Nil them out before returning.
			// Either key+value or payload will be set, but not both.
			if len(key) > 0 {
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
		if c.jobErr != nil {
			return ``, ``, nil, nil, nil, false
		}

		// We're not guaranteed to get a flush notification if the feed exits,
		// so bound how long we wait.
		select {
		case <-c.flushCh:
		case <-time.After(30 * time.Millisecond):
		}

		// If the error was set, save it, but do one more poll as described
		// above.
		var errorStr gosql.NullString
		if err := c.db.QueryRow(
			`SELECT error FROM [SHOW JOBS] WHERE job_id=$1`, c.jobID,
		).Scan(&errorStr); err != nil {
			t.Fatal(err)
		}
		if len(errorStr.String) > 0 {
			c.jobErr = errors.New(errorStr.String)
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
			`DELETE FROM sqlsink ORDER BY PRIMARY KEY sqlsink RETURNING *`)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func (c *tableFeed) Err() error {
	return c.jobErr
}

func (c *tableFeed) Close(t testing.TB) {
	if c.rows != nil {
		if err := c.rows.Close(); err != nil {
			t.Errorf(`could not close rows: %v`, err)
		}
	}
	if _, err := c.db.Exec(`CANCEL JOB $1`, c.jobID); err != nil {
		log.Infof(context.Background(), `could not cancel feed %d: %v`, c.jobID, err)
	}
	if err := c.db.Close(); err != nil {
		t.Error(err)
	}
	c.urlCleanup()
}

func assertPayloads(t testing.TB, f testfeed, expected []string) {
	t.Helper()

	var actual []string
	for len(actual) < len(expected) {
		topic, _, key, value, resolved, ok := f.Next(t)
		if !ok {
			break
		} else if key != nil {
			actual = append(actual, fmt.Sprintf(`%s: %s->%s`, topic, key, value))
		} else if resolved != nil {
			continue
		}
	}

	// The tests that use this aren't concerned with order, just that these are
	// the next len(expected) messages.
	sort.Strings(expected)
	sort.Strings(actual)
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected\n  %s\ngot\n  %s",
			strings.Join(expected, "\n  "), strings.Join(actual, "\n  "))
	}
}

func skipResolvedTimestamps(t *testing.T, f testfeed) {
	for {
		table, _, key, value, _, ok := f.Next(t)
		if !ok {
			break
		}
		if key != nil {
			t.Errorf(`unexpected row %s: %s->%s`, table, key, value)
		}
	}
}

func expectResolvedTimestampGreaterThan(t testing.TB, f testfeed, ts string) {
	t.Helper()
	for {
		topic, _, key, value, resolved, _ := f.Next(t)
		if key != nil {
			t.Fatalf(`unexpected row %s: %s -> %s`, topic, key, value)
		}
		if resolved == nil {
			t.Fatal(`expected a resolved timestamp notification`)
		}

		var valueRaw struct {
			CRDB struct {
				Resolved string `json:"resolved"`
			} `json:"__crdb__"`
		}
		if err := gojson.Unmarshal(resolved, &valueRaw); err != nil {
			t.Fatal(err)
		}
		parseTimeToDecimal := func(s string) *apd.Decimal {
			t.Helper()
			d, _, err := apd.NewFromString(s)
			if err != nil {
				t.Fatal(err)
			}
			return d
		}
		if parseTimeToDecimal(valueRaw.CRDB.Resolved).Cmp(parseTimeToDecimal(ts)) > 0 {
			break
		}
	}
}

func sinklessTest(testFn func(*testing.T, *gosql.DB, testfeedFactory)) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		knobs := base.TestingKnobs{DistSQL: &distsqlrun.TestingKnobs{Changefeed: &TestingKnobs{}}}
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			UseDatabase: "d",
			Knobs:       knobs,
			// TODO(dan): HACK until the changefeed can control pgwire flushing.
			ConnResultsBufferBytes: 1,
		})
		defer s.Stopper().Stop(ctx)
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '0ns'`)
		sqlDB.Exec(t, `CREATE DATABASE d`)
		f := makeSinkless(s, db)
		testFn(t, db, f)
	}
}

func enterpriseTest(testFn func(*testing.T, *gosql.DB, testfeedFactory)) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()

		flushCh := make(chan struct{}, 1)
		defer close(flushCh)
		knobs := base.TestingKnobs{DistSQL: &distsqlrun.TestingKnobs{Changefeed: &TestingKnobs{
			AfterSinkFlush: func() error {
				select {
				case flushCh <- struct{}{}:
				default:
				}
				return nil
			},
		}}}

		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			UseDatabase: "d",
			Knobs:       knobs,
		})
		defer s.Stopper().Stop(ctx)
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '0ns'`)
		sqlDB.Exec(t, `CREATE DATABASE d`)
		f := makeTable(s, db, flushCh)

		testFn(t, db, f)
	}
}
