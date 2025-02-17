// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type fakeResumer struct {
	done chan struct{}
}

var _ jobs.Resumer = (*fakeResumer)(nil)

func (d *fakeResumer) Resume(ctx context.Context, execCtx interface{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-d.done:
		return nil
	}
}

func (d *fakeResumer) OnFailOrCancel(context.Context, interface{}, error) error {
	return nil
}

func (d *fakeResumer) CollectProfile(context.Context, interface{}) error {
	return nil
}

func TestShowChangefeedJobsBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH format='json'`)
		defer closeFeed(t, foo)

		type row struct {
			id             jobspb.JobID
			SinkURI        string
			FullTableNames []uint8
			format         string
			topics         string
		}

		var out row

		query := `SELECT job_id, sink_uri, full_table_names, format, IFNULL(topics, '') FROM [SHOW CHANGEFEED JOBS] ORDER BY sink_uri`
		rowResults := sqlDB.Query(t, query)

		if !rowResults.Next() {
			err := rowResults.Err()
			if err != nil {
				t.Fatalf("Error encountered while querying the next row: %v", err)
			} else {
				t.Fatalf("Expected more rows when querying and none found for query: %s", query)
			}
		}
		err := rowResults.Scan(&out.id, &out.SinkURI, &out.FullTableNames, &out.format, &out.topics)
		if err != nil {
			t.Fatal(err)
		}

		if testFeed, ok := foo.(cdctest.EnterpriseTestFeed); ok {
			details, err := testFeed.Details()
			require.NoError(t, err)
			sinkURI := details.SinkURI
			jobID := testFeed.JobID()

			require.Equal(t, jobID, out.id, "Expected id:%d but found id:%d", jobID, out.id)
			require.Equal(t, sinkURI, out.SinkURI, "Expected sinkUri:%s but found sinkUri:%s", sinkURI, out.SinkURI)

			u, err := url.Parse(sinkURI)
			require.NoError(t, err)
			if u.Scheme == changefeedbase.SinkSchemeKafka {
				require.Equal(t, "foo", out.topics, "Expected topics:%s but found topics:%s", "foo", out.topics)
			}
		}
		require.Equal(t, "{d.public.foo}", string(out.FullTableNames), "Expected fullTableNames:%s but found fullTableNames:%s", "{d.public.foo}", string(out.FullTableNames))
		require.Equal(t, "json", out.format, "Expected format:%s but found format:%s", "json", out.format)
	}

	// TODO: Webhook disabled since the query parameters on the sinkURI are
	// correct but out of order
	cdcTest(t, testFn, feedTestOmitSinks("webhook", "sinkless"), feedTestNoExternalConnection)
}

// TestShowChangefeedJobsShowsHighWaterTimestamp verifies that SHOW CHANGEFEED
// JOBS includes a readable_high_water_timestamp which is a readable timestamp
// but otherwise corresponds to the HLC time in high_water_timestamp.
func TestShowChangefeedJobsShowsHighWaterTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo(a INT PRIMARY KEY)`)
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH resolved='0s',min_checkpoint_frequency='0s'`)

		defer closeFeed(t, foo)

		var highWaterHLC gosql.NullFloat64
		var readableHighWater gosql.NullTime

		// Wait for the high water timestamp to be non-null.
		testutils.SucceedsSoon(t, func() error {
			stmt := `SELECT high_water_timestamp, readable_high_water_timestamptz from [SHOW CHANGEFEED JOBS]`
			sqlDB.QueryRow(t, stmt).Scan(&highWaterHLC, &readableHighWater)

			if !highWaterHLC.Valid {
				return errors.Errorf("high water timestamp not populated: %v", highWaterHLC)
			}

			return nil
		})

		highWaterTimestamp := time.Unix(0, int64(highWaterHLC.Float64)).UTC()
		// Timestamps in CockroachDB have microsecond precision by default.
		roundedHighWaterTimestamp := highWaterTimestamp.Round(time.Microsecond)

		differenceDuration := roundedHighWaterTimestamp.Sub(readableHighWater.Time).Abs()
		require.True(t, differenceDuration < 5*time.Microsecond)
	}

	cdcTest(t, testFn, feedTestOmitSinks("sinkless"))
}

// TestShowChangefeedJobsRedacted verifies that SHOW CHANGEFEED JOB, SHOW
// CHANGEFEED JOBS, and SHOW JOBS redact sensitive information (including keys
// and secrets) for its output. Regression for #113503.
func TestShowChangefeedJobsRedacted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, stopServer := makeServer(t)
	defer stopServer()

	knobs := s.TestingKnobs.
		DistSQL.(*execinfra.TestingKnobs).
		Changefeed.(*TestingKnobs)
	knobs.WrapSink = func(s Sink, _ jobspb.JobID) Sink {
		if _, ok := s.(*externalConnectionKafkaSink); ok {
			return s
		}
		return &externalConnectionKafkaSink{sink: s, ignoreDialError: true}
	}

	sqlDB := sqlutils.MakeSQLRunner(s.DB)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

	const apiSecret = "bar"
	const certSecret = "Zm9v"
	for _, tc := range []struct {
		name                string
		uri                 string
		expectedSinkURI     string
		expectedDescription string
	}{
		{
			name: "api_secret",
			uri:  fmt.Sprintf("confluent-cloud://nope?api_key=fee&api_secret=%s", apiSecret),
		},
		{
			name: "sasl_password",
			uri:  fmt.Sprintf("kafka://nope/?sasl_enabled=true&sasl_handshake=false&sasl_password=%s&sasl_user=aa", apiSecret),
		},
		{
			name: "ca_cert",
			uri:  fmt.Sprintf("kafka://nope?ca_cert=%s&tls_enabled=true", certSecret),
		},
		{
			name: "shared_access_key",
			uri:  fmt.Sprintf("azure-event-hub://nope?shared_access_key=%s&shared_access_key_name=plain", apiSecret),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			createStmt := fmt.Sprintf(`CREATE CHANGEFEED FOR TABLE foo INTO '%s'`, tc.uri)
			var jobID jobspb.JobID
			sqlDB.QueryRow(t, createStmt).Scan(&jobID)
			var sinkURI, description string
			sqlDB.QueryRow(t, "SELECT sink_uri, description from [SHOW CHANGEFEED JOB $1]", jobID).Scan(&sinkURI, &description)
			replacer := strings.NewReplacer(apiSecret, "redacted", certSecret, "redacted")
			expectedSinkURI := replacer.Replace(tc.uri)
			expectedDescription := replacer.Replace(createStmt)
			require.Equal(t, expectedSinkURI, sinkURI)
			require.Equal(t, expectedDescription, description)
		})
	}

	t.Run("jobs", func(t *testing.T) {
		queryStr := sqlDB.QueryStr(t, "SELECT description from [SHOW JOBS]")
		require.NotContains(t, queryStr, apiSecret)
		require.NotContains(t, queryStr, certSecret)
		queryStr = sqlDB.QueryStr(t, "SELECT sink_uri, description from [SHOW CHANGEFEED JOBS]")
		require.NotContains(t, queryStr, apiSecret)
		require.NotContains(t, queryStr, certSecret)
	})
}

func TestShowChangefeedJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	bucket, accessKey, secretKey := checkS3Credentials(t)

	s, rawSQLDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	sqlDB := sqlutils.MakeSQLRunner(rawSQLDB)
	registry := s.ApplicationLayer().JobRegistry().(*jobs.Registry)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	// row represents a row returned from crdb_internal.jobs, but
	// *not* a row in system.jobs.
	type row struct {
		id             jobspb.JobID
		SinkURI        string
		FullTableNames []uint8
		format         string
		description    string
		topics         string
		DescriptorIDs  []descpb.ID
	}

	query := `CREATE TABLE foo (a string)`
	sqlDB.Exec(t, query)

	query = `CREATE TABLE bar (b string)`
	sqlDB.Exec(t, query)

	var fooDescriptorID, barDescriptorID int

	query = `SELECT table_id from crdb_internal.tables WHERE name = 'foo'`
	sqlDB.QueryRow(t, query).Scan(&fooDescriptorID)

	query = `SELECT table_id from crdb_internal.tables WHERE name = 'bar'`
	sqlDB.QueryRow(t, query).Scan(&barDescriptorID)

	doneCh := make(chan struct{})
	defer close(doneCh)

	registry.TestingWrapResumerConstructor(jobspb.TypeChangefeed,
		func(raw jobs.Resumer) jobs.Resumer {
			r := fakeResumer{
				done: doneCh,
			}
			return &r
		})

	query = `SET CLUSTER SETTING kv.rangefeed.enabled = true`
	sqlDB.Exec(t, query)

	var singleChangefeedID, multiChangefeedID jobspb.JobID

	query = `CREATE CHANGEFEED FOR TABLE foo INTO
		'webhook-https://fake-http-sink:8081' WITH webhook_auth_header='Basic Zm9v'`
	sqlDB.QueryRow(t, query).Scan(&singleChangefeedID)

	// Cannot use kafka for tests right now because of leaked goroutine issue
	query = fmt.Sprintf(`CREATE CHANGEFEED FOR TABLE foo, bar INTO
		'experimental-s3://%s/fake/path?AWS_ACCESS_KEY_ID=%s&AWS_SECRET_ACCESS_KEY=%s'`, bucket, accessKey, secretKey)
	sqlDB.QueryRow(t, query).Scan(&multiChangefeedID)

	var out row

	query = `SELECT job_id, sink_uri, full_table_names, format, IFNULL(topics, '') FROM [SHOW CHANGEFEED JOB $1]`
	sqlDB.QueryRow(t, query, multiChangefeedID).Scan(&out.id, &out.SinkURI, &out.FullTableNames, &out.format, &out.topics)

	expectedURI := fmt.Sprintf("experimental-s3://%s/fake/path?AWS_ACCESS_KEY_ID=%s&AWS_SECRET_ACCESS_KEY=redacted", bucket, accessKey)
	require.Equal(t, multiChangefeedID, out.id, "Expected id:%d but found id:%d", multiChangefeedID, out.id)
	require.Equal(t, expectedURI, out.SinkURI, "Expected sinkUri:%s but found sinkUri:%s", expectedURI, out.SinkURI)
	require.Equal(t, "{defaultdb.public.foo,defaultdb.public.bar}", string(out.FullTableNames), "Expected fullTableNames:%s but found fullTableNames:%s", "{defaultdb.public.foo,defaultdb.public.bar}", string(out.FullTableNames))
	require.Equal(t, "json", out.format, "Expected format:%s but found format:%s", "json", out.format)
	require.Equal(t, "", out.topics, "Expected topics to be empty")

	query = `SELECT job_id, description, sink_uri, full_table_names, format, IFNULL(topics, '') FROM [SHOW CHANGEFEED JOBS] ORDER BY sink_uri`
	rowResults := sqlDB.Query(t, query)

	if !rowResults.Next() {
		err := rowResults.Err()
		if err != nil {
			t.Fatalf("Error encountered while querying the next row: %v", err)
		} else {
			t.Fatalf("Expected more rows when querying and none found for query: %s", query)
		}
	}
	err := rowResults.Scan(&out.id, &out.description, &out.SinkURI, &out.FullTableNames, &out.format, &out.topics)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, multiChangefeedID, out.id, "Expected id:%d but found id:%d", multiChangefeedID, out.id)
	require.Equal(t, expectedURI, out.SinkURI, "Expected sinkUri:%s but found sinkUri:%s", expectedURI, out.SinkURI)
	require.Equal(t, "{defaultdb.public.foo,defaultdb.public.bar}", string(out.FullTableNames), "Expected fullTableNames:%s but found fullTableNames:%s", "{defaultdb.public.foo,defaultdb.public.bar}", string(out.FullTableNames))
	require.Equal(t, "json", out.format, "Expected format:%s but found format:%s", "json", out.format)
	require.Equal(t, "", out.topics, "Expected topics to be empty")

	if !rowResults.Next() {
		err := rowResults.Err()
		if err != nil {
			t.Fatalf("Error encountered while querying the next row: %v", err)
		} else {
			t.Fatalf("Expected more rows when querying and none found for query: %s", query)
		}
	}
	err = rowResults.Scan(&out.id, &out.description, &out.SinkURI, &out.FullTableNames, &out.format, &out.topics)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, singleChangefeedID, out.id, "Expected id:%d but found id:%d", singleChangefeedID, out.id)
	require.Equal(t, "CREATE CHANGEFEED FOR TABLE foo INTO 'webhook-https://fake-http-sink:8081' WITH webhook_auth_header = 'redacted'", out.description, "Expected description:%s but found description:%s", "CREATE CHANGEFEED FOR TABLE foo INTO 'webhook-https://fake-http-sink:8081' WITH webhook_auth_header = 'redacted'", out.description)
	require.Equal(t, "webhook-https://fake-http-sink:8081", out.SinkURI, "Expected sinkUri:%s but found sinkUri:%s", "webhook-https://fake-http-sink:8081", out.SinkURI)
	require.Equal(t, "{defaultdb.public.foo}", string(out.FullTableNames), "Expected fullTableNames:%s but found fullTableNames:%s", "{defaultdb.public.foo}", string(out.FullTableNames))
	require.Equal(t, "json", out.format, "Expected format:%s but found format:%s", "json", out.format)
	require.Equal(t, "", out.topics, "Expected topics to be empty")

	hasNext := rowResults.Next()
	require.Equal(t, false, hasNext, "Expected no more rows for query:%s", query)

	err = rowResults.Err()
	require.Equal(t, nil, err, "Expected no error for query:%s but got error %v", query, err)
}

func TestShowChangefeedJobsStatusChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var params base.TestServerArgs
	params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
	srv, rawSQLDB, _ := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()

	for _, l := range []serverutils.ApplicationLayerInterface{s, srv.SystemLayer()} {
		kvserver.RangefeedEnabled.Override(ctx, &l.ClusterSettings().SV, true)
	}

	registry := s.JobRegistry().(*jobs.Registry)
	sqlDB := sqlutils.MakeSQLRunner(rawSQLDB)

	query := `CREATE TABLE foo (a string)`
	sqlDB.Exec(t, query)

	doneCh := make(chan struct{})
	defer close(doneCh)

	registry.TestingWrapResumerConstructor(jobspb.TypeChangefeed,
		func(raw jobs.Resumer) jobs.Resumer {
			r := fakeResumer{
				done: doneCh,
			}
			return &r
		})

	var changefeedID jobspb.JobID

	query = `CREATE CHANGEFEED FOR TABLE foo INTO
		'experimental-http://fake-bucket-name/fake/path?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456'`
	sqlDB.QueryRow(t, query).Scan(&changefeedID)

	waitForJobState(sqlDB, t, changefeedID, "running")

	query = `PAUSE JOB $1`
	sqlDB.Exec(t, query, changefeedID)

	waitForJobState(sqlDB, t, changefeedID, "paused")

	query = `RESUME JOB $1`
	sqlDB.Exec(t, query, changefeedID)

	waitForJobState(sqlDB, t, changefeedID, "running")
}

func TestShowChangefeedJobsNoResults(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, rawSQLDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	sqlDB := sqlutils.MakeSQLRunner(rawSQLDB)
	defer s.Stopper().Stop(context.Background())

	query := `SELECT job_id, sink_uri, full_table_names, format FROM [SHOW CHANGEFEED JOB 999999999999]`
	rowResults := sqlDB.Query(t, query)

	if rowResults.Next() || rowResults.Err() != nil {
		t.Fatalf("Expected no results for query:%s", query)
	}

	query = `CREATE TABLE foo (a string)`
	sqlDB.Exec(t, query)

	query = `ALTER TABLE foo ADD COLUMN b BOOL DEFAULT true`
	sqlDB.Exec(t, query)

	var jobID int

	query = `SELECT job_id FROM [SHOW JOBS] WHERE description LIKE '%ALTER TABLE defaultdb.public.foo%'`
	rowResults = sqlDB.Query(t, query)

	if !rowResults.Next() {
		err := rowResults.Err()
		if err != nil {
			t.Fatalf("Error encountered while querying the next row: %v", err)
		} else {
			t.Fatalf("Expected more rows when querying and none found for query: %s", query)
		}
	}

	err := rowResults.Scan(&jobID)
	if err != nil {
		t.Fatal(err)
	}

	query = `SELECT job_id, sink_uri, full_table_names, format FROM [SHOW CHANGEFEED JOB $1]`
	rowResults = sqlDB.Query(t, query, jobID)

	if rowResults.Next() || rowResults.Err() != nil {
		t.Fatalf("Expected no results for query:%s", query)
	}
}

func TestShowChangefeedJobsAlterChangefeed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY)`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, foo)

		feed, ok := foo.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		jobID := feed.JobID()
		details, err := feed.Details()
		require.NoError(t, err)
		sinkURI := details.SinkURI

		type row struct {
			id             jobspb.JobID
			description    string
			SinkURI        string
			FullTableNames []uint8
			format         string
			topics         string
		}

		obtainJobRowFn := func() row {
			var out row

			query := fmt.Sprintf(
				`SELECT job_id, description, sink_uri, full_table_names, format, IFNULL(topics, '') FROM [SHOW CHANGEFEED JOB %d]`,
				jobID,
			)

			rowResults := sqlDB.Query(t, query)
			if !rowResults.Next() {
				err := rowResults.Err()
				if err != nil {
					t.Fatalf("Error encountered while querying the next row: %v", err)
				} else {
					t.Fatalf("Expected more rows when querying and none found for query: %s", query)
				}
			}
			err := rowResults.Scan(&out.id, &out.description, &out.SinkURI, &out.FullTableNames, &out.format, &out.topics)
			if err != nil {
				t.Fatal(err)
			}

			return out
		}

		sqlDB.Exec(t, `PAUSE JOB $1`, jobID)
		waitForJobState(sqlDB, t, jobID, `paused`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d ADD bar`, jobID))

		out := obtainJobRowFn()

		topicsArr := strings.Split(out.topics, ",")
		sort.Strings(topicsArr)
		sortedTopics := strings.Join(topicsArr, ",")
		require.Equal(t, jobID, out.id, "Expected id:%d but found id:%d", jobID, out.id)
		require.Equal(t, sinkURI, out.SinkURI, "Expected sinkUri:%s but found sinkUri:%s", sinkURI, out.SinkURI)
		require.Equal(t, "bar,foo", sortedTopics, "Expected topics:%s but found topics:%s", "bar,foo", sortedTopics)
		require.Equal(t, "{d.public.foo,d.public.bar}", string(out.FullTableNames), "Expected fullTableNames:%s but found fullTableNames:%s", "{d.public.foo,d.public.bar}", string(out.FullTableNames))
		require.Equal(t, "json", out.format, "Expected format:%s but found format:%s", "json", out.format)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d DROP foo`, feed.JobID()))

		out = obtainJobRowFn()

		require.Equal(t, jobID, out.id, "Expected id:%d but found id:%d", jobID, out.id)
		require.Equal(t, "CREATE CHANGEFEED FOR TABLE d.public.bar INTO 'kafka://does.not.matter/'", out.description, "Expected description:%s but found description:%s", "CREATE CHANGEFEED FOR TABLE bar INTO 'kafka://does.not.matter/'", out.description)
		require.Equal(t, sinkURI, out.SinkURI, "Expected sinkUri:%s but found sinkUri:%s", sinkURI, out.SinkURI)
		require.Equal(t, "bar", out.topics, "Expected topics:%s but found topics:%s", "bar", sortedTopics)
		require.Equal(t, "{d.public.bar}", string(out.FullTableNames), "Expected fullTableNames:%s but found fullTableNames:%s", "{d.public.bar}", string(out.FullTableNames))
		require.Equal(t, "json", out.format, "Expected format:%s but found format:%s", "json", out.format)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d SET resolved = '5s'`, feed.JobID()))

		out = obtainJobRowFn()

		require.Equal(t, jobID, out.id, "Expected id:%d but found id:%d", jobID, out.id)
		require.Equal(t, "CREATE CHANGEFEED FOR TABLE d.public.bar INTO 'kafka://does.not.matter/' WITH OPTIONS (resolved = '5s')", out.description, "Expected description:%s but found description:%s", "CREATE CHANGEFEED FOR TABLE bar INTO 'kafka://does.not.matter/ WITH resolved = '5s''", out.description)
		require.Equal(t, sinkURI, out.SinkURI, "Expected sinkUri:%s but found sinkUri:%s", sinkURI, out.SinkURI)
		require.Equal(t, "bar", out.topics, "Expected topics:%s but found topics:%s", "bar", sortedTopics)
		require.Equal(t, "{d.public.bar}", string(out.FullTableNames), "Expected fullTableNames:%s but found fullTableNames:%s", "{d.public.bar}", string(out.FullTableNames))
		require.Equal(t, "json", out.format, "Expected format:%s but found format:%s", "json", out.format)
	}

	// Force kafka to validate topics
	cdcTest(t, testFn, feedTestForceSink("kafka"), feedTestNoExternalConnection)
}

func TestShowChangefeedJobsAuthorization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		ChangefeedJobPermissionsTestSetup(t, s)

		var jobID jobspb.JobID
		createFeed := func(stmt string) {
			successfulFeed := feed(t, f, stmt)
			defer closeFeed(t, successfulFeed)
			_, err := successfulFeed.Next()
			require.NoError(t, err)
			jobID = successfulFeed.(cdctest.EnterpriseTestFeed).JobID()
		}
		rootDB := sqlutils.MakeSQLRunner(s.DB)

		// Create a changefeed and assert who can see it.
		asUser(t, f, `feedCreator`, func(userDB *sqlutils.SQLRunner) {
			createFeed(`CREATE CHANGEFEED FOR table_a, table_b`)
		})
		expectedJobIDStr := strconv.Itoa(int(jobID))
		asUser(t, f, `adminUser`, func(userDB *sqlutils.SQLRunner) {
			userDB.CheckQueryResults(t, `SELECT job_id FROM [SHOW CHANGEFEED JOBS]`, [][]string{{expectedJobIDStr}})
		})
		asUser(t, f, `userWithAllGrants`, func(userDB *sqlutils.SQLRunner) {
			userDB.CheckQueryResults(t, `SELECT job_id FROM [SHOW CHANGEFEED JOBS]`, [][]string{{expectedJobIDStr}})
		})
		asUser(t, f, `userWithSomeGrants`, func(userDB *sqlutils.SQLRunner) {
			userDB.CheckQueryResults(t, `SELECT job_id FROM [SHOW CHANGEFEED JOBS]`, [][]string{})
		})
		asUser(t, f, `jobController`, func(userDB *sqlutils.SQLRunner) {
			userDB.CheckQueryResults(t, `SELECT job_id FROM [SHOW CHANGEFEED JOBS]`, [][]string{{expectedJobIDStr}})
		})
		asUser(t, f, `regularUser`, func(userDB *sqlutils.SQLRunner) {
			userDB.CheckQueryResults(t, `SELECT job_id FROM [SHOW CHANGEFEED JOBS]`, [][]string{})
		})

		// Assert behavior when one of the tables is dropped.
		rootDB.Exec(t, "DROP TABLE table_b")
		// Having CHANGEFEED on only table_a is now sufficient.
		asUser(t, f, `userWithSomeGrants`, func(userDB *sqlutils.SQLRunner) {
			userDB.CheckQueryResults(t, `SELECT job_id FROM [SHOW CHANGEFEED JOBS]`, [][]string{{expectedJobIDStr}})
		})
		asUser(t, f, `regularUser`, func(userDB *sqlutils.SQLRunner) {
			userDB.CheckQueryResults(t, `SELECT job_id FROM [SHOW CHANGEFEED JOBS]`, [][]string{})
		})
	}

	// Only enterprise sinks create jobs.
	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

// TestShowChangefeedJobsDefaultFilter verifies that "SHOW JOBS" AND "SHOW CHANGEFEED JOBS"
// use the same age filter (12 hours).
func TestShowChangefeedJobsDefaultFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		countChangefeedJobs := func() (count int) {
			query := `select count(*) from [SHOW CHANGEFEED JOBS]`
			sqlDB.QueryRow(t, query).Scan(&count)
			return count
		}
		changefeedJobExists := func(id catpb.JobID) bool {
			rows := sqlDB.Query(t, `SHOW CHANGEFEED JOB $1`, id)
			defer rows.Close()
			return rows.Next()
		}

		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		waitForJobState(sqlDB, t, foo.(cdctest.EnterpriseTestFeed).JobID(), jobs.StateRunning)
		require.Equal(t, 1, countChangefeedJobs())

		// The job is not visible after closed (and its finished time is older than 12 hours).
		closeFeed(t, foo)
		require.Equal(t, 0, countChangefeedJobs())

		// We can still see the job if we explicitly ask for it.
		jobID := foo.(cdctest.EnterpriseTestFeed).JobID()
		require.True(t, changefeedJobExists(jobID))
	}

	updateKnobs := func(opts *feedTestOptions) {
		opts.knobsFn = func(knobs *base.TestingKnobs) {
			knobs.JobsTestingKnobs.(*jobs.TestingKnobs).StubTimeNow = func() time.Time {
				return timeutil.Now().Add(-13 * time.Hour)
			}
		}
	}

	cdcTest(t, testFn, feedTestForceSink("kafka"), updateKnobs)
}
