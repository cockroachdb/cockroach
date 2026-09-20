// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestChangefeedExternalIOImplicitAccess tests that users cannot create
// changefeeds into external sinks without the EXTERNALIOIMPLICITACCESS
// permission but can once it is granted.
func TestChangefeedExternalIOImplicitAccess(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		rootDB := sqlutils.MakeSQLRunner(s.DB)
		rootDB.Exec(t, `CREATE TABLE t (x INT PRIMARY KEY)`)
		rootDB.Exec(t, `INSERT INTO t VALUES (1)`)
		rootDB.Exec(t, `CREATE USER user1`)
		rootDB.Exec(t, `GRANT SELECT, CHANGEFEED ON TABLE t TO user1`)

		rootDB.Exec(t, `SET CLUSTER SETTING changefeed.permissions.require_external_io_implicit_access.enabled = false`)
		asUser(t, f, `user1`, func(_ *sqlutils.SQLRunner) {
			foo := feed(t, f, `CREATE CHANGEFEED FOR t`)
			defer closeFeed(t, foo)
			assertPayloads(t, foo, []string{`t: [1]->{"after": {"x": 1}}`})
		})

		rootDB.Exec(t, `SET CLUSTER SETTING changefeed.permissions.require_external_io_implicit_access.enabled = true`)

		asUser(t, f, `user1`, func(_ *sqlutils.SQLRunner) {
			// Ensure the error message includes a pointer to the right setting.
			expectErrCreatingFeed(t, f, `CREATE CHANGEFEED FOR t`, `EXTERNALIOIMPLICITACCESS`)
		})

		rootDB.Exec(t, `GRANT SYSTEM EXTERNALIOIMPLICITACCESS TO user1`)
		rootDB.Exec(t, `INSERT INTO t VALUES (2)`)
		asUser(t, f, `user1`, func(_ *sqlutils.SQLRunner) {
			foo := feed(t, f, `CREATE CHANGEFEED FOR t`)
			defer closeFeed(t, foo)
			assertPayloads(t, foo, []string{
				`t: [1]->{"after": {"x": 1}}`,
				`t: [2]->{"after": {"x": 2}}`,
			})
		})
	}

	cdcTest(t, testFn, feedTestOmitSinks("sinkless"), feedTestNoExternalConnection)
}

// TestChangefeedExternalIOImplicitAccessRunningFeed verifies that
// feeds that were created before the setting is turned on are not
// affected by the change until an alter.
func TestChangefeedExternalIOImplicitAccessRunningFeed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		rootDB := sqlutils.MakeSQLRunner(s.DB)
		rootDB.Exec(t, `CREATE TABLE t (x INT PRIMARY KEY)`)
		rootDB.Exec(t, `INSERT INTO t VALUES (1)`)
		rootDB.Exec(t, `CREATE USER user1`)
		rootDB.Exec(t, `GRANT SELECT, CHANGEFEED ON TABLE t TO user1`)

		rootDB.Exec(t, `SET CLUSTER SETTING changefeed.permissions.require_external_io_implicit_access.enabled = false`)

		var foo cdctest.TestFeed
		asUser(t, f, `user1`, func(_ *sqlutils.SQLRunner) {
			foo = feed(t, f, `CREATE CHANGEFEED FOR t`)
		})
		defer closeFeed(t, foo)
		assertPayloads(t, foo, []string{`t: [1]->{"after": {"x": 1}}`})

		rootDB.Exec(t, `SET CLUSTER SETTING changefeed.permissions.require_external_io_implicit_access.enabled = true`)
		rootDB.Exec(t, `INSERT INTO t VALUES (2)`)
		assertPayloads(t, foo, []string{`t: [2]->{"after": {"x": 2}}`})

		jobFeed := foo.(cdctest.EnterpriseTestFeed)
		rootDB.Exec(t, `PAUSE JOB $1`, jobFeed.JobID())
		waitForJobState(rootDB, t, jobFeed.JobID(), `paused`)
		asUser(t, f, `user1`, func(userDB *sqlutils.SQLRunner) {
			userDB.ExpectErr(t, `EXTERNALIOIMPLICITACCESS`,
				fmt.Sprintf(`ALTER CHANGEFEED %d SET resolved = '10s'`, jobFeed.JobID()))
		})
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks, feedTestNoExternalConnection)
}

type blockedSinkURI struct {
	name             string
	uri              string
	errorDestination string
}

// TestChangefeedExternalIOImplicitAccessSinkURIs verifies that, with the
// requirement on, CREATE CHANGEFEED is rejected for using implicit access
// when they lack EXTERNALIOIMPLICITACCESS permission. It covers both the
// changefeed-only sink schemes (kafka://, pulsar://, ...) and the external
// storage schemes (s3://, gs://, ...).
func TestChangefeedExternalIOImplicitAccessSinkURIs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	sqlutils.MakeSQLRunner(srv.SystemLayer().SQLConn(t)).Exec(
		t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	app := srv.ApplicationLayer()
	rootDB := sqlutils.MakeSQLRunner(app.SQLConn(t))
	provisionImplicitAccessUser(t, rootDB)

	userDB := sqlutils.MakeSQLRunner(app.SQLConn(t, serverutils.User("testuser")))
	userDB.Exec(t, `SET statement_timeout = '10s'`)

	implicitAccessBlockedURIs := []blockedSinkURI{
		{name: "kafka", uri: "kafka://nope", errorDestination: "kafka"},
		{name: "pulsar", uri: "pulsar://nope", errorDestination: "pulsar"},
		{name: "webhook-https", uri: "webhook-https://nope", errorDestination: "webhook-https"},
		{
			name:             "confluent-cloud",
			uri:              "confluent-cloud://nope?api_key=key&api_secret=secret",
			errorDestination: "confluent-cloud",
		},
		{
			name:             "azure-event-hub",
			uri:              "azure-event-hub://nope?shared_access_key_name=name&shared_access_key=key",
			errorDestination: "azure-event-hub",
		},
		{
			name:             "experimental-sql",
			uri:              fmt.Sprintf("experimental-sql://root@%s/defaultdb?sslmode=disable", app.AdvSQLAddr()),
			errorDestination: "experimental-sql",
		},
		{
			name: "kafka-aws-msk-iam",
			uri: "kafka://nope?tls_enabled=true&sasl_enabled=true&sasl_mechanism=AWS_MSK_IAM" +
				"&sasl_aws_region=us-east-1&sasl_aws_iam_role_arn=arn%3Aaws%3Aiam%3A%3A123%3Arole%2Fr" +
				"&sasl_aws_iam_session_name=cdc",
			errorDestination: "kafka",
		},
		{
			// sasl_token_url points the node at an OAuth token endpoint it fetches
			// with implicit credentials. It needs no dedicated gate handling: the
			// kafka sink it belongs to is already blocked.
			name: "kafka-sasl-oauth",
			uri: "kafka://nope?tls_enabled=true&sasl_enabled=true&sasl_mechanism=OAUTHBEARER" +
				"&sasl_client_id=id&sasl_client_secret=secret" +
				"&sasl_token_url=https%3A%2F%2Fauth.example.com%2Ftoken",
			errorDestination: "kafka",
		},
		{
			name:             "gcpubsub-implicit",
			uri:              "gcpubsub://nope?region=us-east1&AUTH=implicit",
			errorDestination: "gcpubsub",
		},
		{
			name:             "s3-implicit",
			uri:              "s3://bucket/path?AUTH=implicit&AWS_REGION=us-east-1",
			errorDestination: "s3",
		},
		{name: "gs-implicit", uri: "gs://bucket/path?AUTH=implicit", errorDestination: "gs"},
		{
			// file_size and partition_format are changefeed-specific params that
			// resolveSinkDestination must strip before handing the URI to
			// ExternalStorageConfFromURI.
			name:             "gs-implicit-with-changefeed-params",
			uri:              "gs://bucket/path?AUTH=implicit&file_size=32MB&partition_format=daily",
			errorDestination: "gs",
		},
		{
			name:             "azure-implicit",
			uri:              "azure://container/path?AUTH=implicit&AZURE_ACCOUNT_NAME=acct",
			errorDestination: "azure",
		},
		{name: "nodelocal", uri: "nodelocal://1/foo", errorDestination: "nodelocal"},
		// A file-https sink is backed by the http external storage provider, so the
		// error names the provider, not the changefeed-specific scheme.
		{name: "file-https", uri: "file-https://nope/path", errorDestination: "http"},
		{name: "experimental-nodelocal", uri: "experimental-nodelocal://1/foo", errorDestination: "nodelocal"},
	}

	for _, tc := range implicitAccessBlockedURIs {
		t.Run(tc.name, func(t *testing.T) {
			_, err := userDB.DB.ExecContext(ctx, `CREATE CHANGEFEED FOR t INTO $1`, tc.uri)
			require.Error(t, err)
			require.Equal(t, pgcode.InsufficientPrivilege.String(), pgErrCode(err), "got: %v", err)
			require.ErrorContains(t, err, `EXTERNALIOIMPLICITACCESS`)
			require.ErrorContains(t, err, fmt.Sprintf("specified %s URI", tc.errorDestination))
		})
	}
}

// noopSink is a Sink that drops everything it is handed. It is installed via the
// WrapSink testing knob so that a changefeed which clears the authorization gate
// never performs real network I/O when its job runs.
type noopSink struct {
	testSink
}

var _ Sink = noopSink{}

func (noopSink) Dial() error  { return nil }
func (noopSink) Close() error { return nil }

func (noopSink) EmitRow(
	context.Context,
	TopicDescriptor,
	[]byte,
	[]byte,
	hlc.Timestamp,
	hlc.Timestamp,
	kvevent.Alloc,
	rowHeaders,
) error {
	return nil
}

func (noopSink) Flush(context.Context) error { return nil }

func (noopSink) EmitResolvedTimestamp(context.Context, Encoder, hlc.Timestamp) error {
	return nil
}

// TestChangefeedExternalIOImplicitAccessAlter tests that with the setting on,
// ALTERS succeed for sink changes TO a sink that uses explicit auth but fails
// when altering to a sink that uses implicit auth.
// ALTER cannot change the sink type, so the only sinks that reach
// the gate on this path are the storage schemes whose auth is data-dependent
// (s3, azure).
func TestChangefeedExternalIOImplicitAccessAlter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			DistSQL: &execinfra.TestingKnobs{
				Changefeed: &TestingKnobs{
					WrapSink: func(Sink, jobspb.JobID) Sink { return noopSink{} },
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	sqlutils.MakeSQLRunner(srv.SystemLayer().SQLConn(t)).Exec(
		t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	rootDB := sqlutils.MakeSQLRunner(db)
	provisionImplicitAccessUser(t, rootDB)

	userDB := sqlutils.MakeSQLRunner(srv.SQLConn(t, serverutils.User("testuser")))
	userDB.Exec(t, `SET statement_timeout = '30s'`)

	testCases := []struct {
		name      string
		createURI string
		alterURI  string
		// blockedDest, when set, means the ALTER must be rejected and name this
		// destination; when empty the ALTER must clear the gate.
		blockedDest string
	}{
		{name: "null", createURI: "null://", alterURI: "null://?delay=1s"},
		{
			name:      "s3-explicit",
			createURI: "s3://bucket/path?AWS_ACCESS_KEY_ID=k&AWS_SECRET_ACCESS_KEY=s&AWS_REGION=us-east-1",
			alterURI:  "s3://bucket/path2?AWS_ACCESS_KEY_ID=k&AWS_SECRET_ACCESS_KEY=s&AWS_REGION=us-east-1",
		},
		{
			name: "s3-explicit-with-changefeed-params",
			// We successfully parse sink URIs with additional params.
			createURI: "s3://bucket/path?AWS_ACCESS_KEY_ID=k&AWS_SECRET_ACCESS_KEY=s&AWS_REGION=us-east-1&file_size=32MB&partition_format=daily",
			alterURI:  "s3://bucket/path2?AWS_ACCESS_KEY_ID=k&AWS_SECRET_ACCESS_KEY=s&AWS_REGION=us-east-1&file_size=32MB&partition_format=daily",
		},
		{
			name: "gs-explicit",
			// GCS parses CREDENTIALS at sink construction, so it must base64-decode
			// to a service-account JSON. The key itself is never used because the
			// sink is replaced with a noop via the WrapSink knob.
			createURI: "gs://bucket/path?AUTH=specified&CREDENTIALS=eyJ0eXBlIjoic2VydmljZV9hY2NvdW50In0%3D",
			alterURI:  "gs://bucket/path2?AUTH=specified&CREDENTIALS=eyJ0eXBlIjoic2VydmljZV9hY2NvdW50In0%3D",
		},
		{
			name:      "azure-explicit",
			createURI: "azure://container/path?AZURE_ACCOUNT_NAME=acct&AZURE_ACCOUNT_KEY=Zm9v",
			alterURI:  "azure://container/path2?AZURE_ACCOUNT_NAME=acct&AZURE_ACCOUNT_KEY=Zm9v",
		},
		{
			name:        "s3-to-implicit",
			createURI:   "s3://bucket/path?AWS_ACCESS_KEY_ID=k&AWS_SECRET_ACCESS_KEY=s&AWS_REGION=us-east-1",
			alterURI:    "s3://bucket/path?AUTH=implicit&AWS_REGION=us-east-1",
			blockedDest: "s3",
		},
		{
			name:        "azure-to-implicit",
			createURI:   "azure://container/path?AZURE_ACCOUNT_NAME=acct&AZURE_ACCOUNT_KEY=Zm9v",
			alterURI:    "azure://container/path?AUTH=implicit&AZURE_ACCOUNT_NAME=acct",
			blockedDest: "azure",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer rootDB.Exec(t,
				`CANCEL JOBS (SELECT job_id FROM [SHOW CHANGEFEED JOBS] WHERE status IN ('running', 'pending', 'paused'))`)

			var jobID int64
			require.NoError(t, userDB.DB.QueryRowContext(
				ctx, `CREATE CHANGEFEED FOR t INTO $1`, tc.createURI).Scan(&jobID))

			userDB.Exec(t, `PAUSE JOB $1`, jobID)
			waitForJobState(userDB, t, jobspb.JobID(jobID), `paused`)
			_, err := userDB.DB.ExecContext(
				ctx, fmt.Sprintf(`ALTER CHANGEFEED %d SET sink = '%s'`, jobID, tc.alterURI))
			if tc.blockedDest == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Equal(t, pgcode.InsufficientPrivilege.String(), pgErrCode(err), "got: %v", err)
			require.ErrorContains(t, err, `EXTERNALIOIMPLICITACCESS`)
			require.ErrorContains(t, err, fmt.Sprintf("specified %s URI", tc.blockedDest))
		})
	}
}

func provisionImplicitAccessUser(t *testing.T, rootDB *sqlutils.SQLRunner) {
	rootDB.Exec(t, `CREATE TABLE t (x INT PRIMARY KEY)`)
	rootDB.Exec(t, `CREATE USER testuser`)
	rootDB.Exec(t, `GRANT SELECT, CHANGEFEED ON TABLE t TO testuser`)
	rootDB.Exec(t,
		`SET CLUSTER SETTING changefeed.permissions.require_external_io_implicit_access.enabled = true`)
}
