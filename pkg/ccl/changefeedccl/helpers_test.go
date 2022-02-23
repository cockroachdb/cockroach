// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	gosql "database/sql"
	gojson "encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	apd "github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	// Imported to allow locality-related table mutations
	_ "github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/partitionccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

var testSinkFlushFrequency = 100 * time.Millisecond

func waitForSchemaChange(
	t testing.TB, sqlDB *sqlutils.SQLRunner, stmt string, arguments ...interface{},
) {
	sqlDB.Exec(t, stmt, arguments...)
	row := sqlDB.QueryRow(t, "SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'SCHEMA CHANGE' ORDER BY created DESC LIMIT 1")
	var jobID string
	row.Scan(&jobID)

	testutils.SucceedsSoon(t, func() error {
		row := sqlDB.QueryRow(t, "SELECT status FROM [SHOW JOBS] WHERE job_id = $1", jobID)
		var status string
		row.Scan(&status)
		if status != "succeeded" {
			return fmt.Errorf("Job %s had status %s, wanted 'succeeded'", jobID, status)
		}
		return nil
	})
}

func readNextMessages(f cdctest.TestFeed, numMessages int) ([]cdctest.TestFeedMessage, error) {
	var actual []cdctest.TestFeedMessage
	for len(actual) < numMessages {
		m, err := f.Next()
		if log.V(1) {
			if m != nil {
				log.Infof(context.Background(), `msg %s: %s->%s (%s)`, m.Topic, m.Key, m.Value, m.Resolved)
			} else {
				log.Infof(context.Background(), `err %v`, err)
			}
		}
		if err != nil {
			return nil, err
		}
		if m == nil {
			return nil, errors.AssertionFailedf(`expected message`)
		}
		if len(m.Key) > 0 || len(m.Value) > 0 {
			actual = append(actual,
				cdctest.TestFeedMessage{
					Topic: m.Topic,
					Key:   m.Key,
					Value: m.Value,
				},
			)
		}
	}
	return actual, nil
}

func stripTsFromPayloads(payloads []cdctest.TestFeedMessage) ([]string, error) {
	var actual []string
	for _, m := range payloads {
		var value []byte
		var message map[string]interface{}
		if err := gojson.Unmarshal(m.Value, &message); err != nil {
			return nil, errors.Wrapf(err, `unmarshal: %s`, m.Value)
		}
		delete(message, "updated")
		value, err := reformatJSON(message)
		if err != nil {
			return nil, err
		}
		actual = append(actual, fmt.Sprintf(`%s: %s->%s`, m.Topic, m.Key, value))
	}
	return actual, nil
}

func extractUpdatedFromValue(value []byte) (float64, error) {
	var updatedRaw struct {
		Updated string `json:"updated"`
	}
	if err := gojson.Unmarshal(value, &updatedRaw); err != nil {
		return -1, errors.Wrapf(err, `unmarshal: %s`, value)
	}
	updatedVal, err := strconv.ParseFloat(updatedRaw.Updated, 64)
	if err != nil {
		return -1, errors.Wrapf(err, "error parsing updated timestamp: %s", updatedRaw.Updated)
	}
	return updatedVal, nil

}

func checkPerKeyOrdering(payloads []cdctest.TestFeedMessage) (bool, error) {
	// map key to list of timestamp, ensure each list is ordered
	keysToTimestamps := make(map[string][]float64)
	for _, msg := range payloads {
		key := string(msg.Key)
		updatedTimestamp, err := extractUpdatedFromValue(msg.Value)
		if err != nil {
			return false, err
		}
		if _, ok := keysToTimestamps[key]; !ok {
			keysToTimestamps[key] = []float64{}
		}
		if len(keysToTimestamps[key]) > 0 {
			if updatedTimestamp < keysToTimestamps[key][len(keysToTimestamps[key])-1] {
				return false, nil
			}
		}
		keysToTimestamps[key] = append(keysToTimestamps[key], updatedTimestamp)
	}
	return true, nil
}

func assertPayloadsBase(
	t testing.TB, f cdctest.TestFeed, expected []string, stripTs bool, perKeyOrdered bool,
) {
	t.Helper()
	require.NoError(t, assertPayloadsBaseErr(f, expected, stripTs, perKeyOrdered))
}

func assertPayloadsBaseErr(
	f cdctest.TestFeed, expected []string, stripTs bool, perKeyOrdered bool,
) error {
	actual, err := readNextMessages(f, len(expected))
	if err != nil {
		return err
	}

	var actualFormatted []string
	for _, m := range actual {
		actualFormatted = append(actualFormatted, fmt.Sprintf(`%s: %s->%s`, m.Topic, m.Key, m.Value))
	}

	if perKeyOrdered {
		ordered, err := checkPerKeyOrdering(actual)
		if err != nil {
			return err
		}
		if !ordered {
			return errors.Newf("payloads violate CDC per-key ordering guarantees:\n  %s",
				strings.Join(actualFormatted, "\n  "))
		}
	}

	// strip timestamps after checking per-key ordering since check uses timestamps
	if stripTs {
		// format again with timestamps stripped
		actualFormatted, err = stripTsFromPayloads(actual)
		if err != nil {
			return err
		}
	}

	sort.Strings(expected)
	sort.Strings(actualFormatted)
	if !reflect.DeepEqual(expected, actualFormatted) {
		return errors.Newf("expected\n  %s\ngot\n  %s",
			strings.Join(expected, "\n  "), strings.Join(actualFormatted, "\n  "))
	}
	return nil
}

func assertPayloads(t testing.TB, f cdctest.TestFeed, expected []string) {
	t.Helper()
	assertPayloadsBase(t, f, expected, false, false)
}

func assertPayloadsStripTs(t testing.TB, f cdctest.TestFeed, expected []string) {
	t.Helper()
	assertPayloadsBase(t, f, expected, true, false)
}

// assert that the messages received by the sink maintain per-key ordering guarantees. then,
// strip the timestamp from the messages and compare them to the expected payloads.
func assertPayloadsPerKeyOrderedStripTs(t testing.TB, f cdctest.TestFeed, expected []string) {
	t.Helper()
	assertPayloadsBase(t, f, expected, true, true)
}

func avroToJSON(t testing.TB, reg *cdctest.SchemaRegistry, avroBytes []byte) []byte {
	json, err := reg.AvroToJSON(avroBytes)
	require.NoError(t, err)
	return json
}

func assertRegisteredSubjects(t testing.TB, reg *cdctest.SchemaRegistry, expected []string) {
	t.Helper()

	actual := reg.Subjects()
	sort.Strings(expected)
	sort.Strings(actual)
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected\n  %s\ngot\n  %s",
			strings.Join(expected, "\n  "), strings.Join(actual, "\n  "))
	}
}

func parseTimeToHLC(t testing.TB, s string) hlc.Timestamp {
	t.Helper()
	d, _, err := apd.NewFromString(s)
	if err != nil {
		t.Fatal(err)
	}
	ts, err := tree.DecimalToHLC(d)
	if err != nil {
		t.Fatal(err)
	}
	return ts
}

// Expect to receive a resolved timestamp and the partition it belongs to from
// a test changefeed.
func expectResolvedTimestamp(t testing.TB, f cdctest.TestFeed) (hlc.Timestamp, string) {
	t.Helper()
	m, err := f.Next()
	if err != nil {
		t.Fatal(err)
	} else if m == nil {
		t.Fatal(`expected message`)
	}
	return extractResolvedTimestamp(t, m), m.Partition
}

func extractResolvedTimestamp(t testing.TB, m *cdctest.TestFeedMessage) hlc.Timestamp {
	t.Helper()
	if m.Key != nil {
		t.Fatalf(`unexpected row %s: %s -> %s`, m.Topic, m.Key, m.Value)
	}
	if m.Resolved == nil {
		t.Fatal(`expected a resolved timestamp notification`)
	}

	var resolvedRaw struct {
		Resolved string `json:"resolved"`
	}
	if err := gojson.Unmarshal(m.Resolved, &resolvedRaw); err != nil {
		t.Fatal(err)
	}

	return parseTimeToHLC(t, resolvedRaw.Resolved)
}

func expectResolvedTimestampAvro(t testing.TB, f cdctest.TestFeed) hlc.Timestamp {
	t.Helper()
	m, err := f.Next()
	if err != nil {
		t.Fatal(err)
	} else if m == nil {
		t.Fatal(`expected message`)
	}
	if m.Key != nil {
		t.Fatalf(`unexpected row %s: %s -> %s`, m.Topic, m.Key, m.Value)
	}
	if m.Resolved == nil {
		t.Fatal(`expected a resolved timestamp notification`)
	}

	var resolvedNative interface{}
	err = gojson.Unmarshal(m.Resolved, &resolvedNative)
	if err != nil {
		t.Fatal(err)
	}
	resolved := resolvedNative.(map[string]interface{})[`resolved`]
	return parseTimeToHLC(t, resolved.(map[string]interface{})[`string`].(string))
}

var serverSetupStatements = `
SET CLUSTER SETTING kv.rangefeed.enabled = true;
SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1s';
SET CLUSTER SETTING changefeed.experimental_poll_interval = '10ms';
SET CLUSTER SETTING sql.defaults.vectorize=on;
CREATE DATABASE d;
`

func startTestServer(
	t testing.TB, options feedTestOptions,
) (serverutils.TestServerInterface, *gosql.DB, func()) {
	if options.useTenant {
		return startTestTenant(t, options)
	}
	return startTestFullServer(t, options)
}

func startTestFullServer(
	t testing.TB, options feedTestOptions,
) (serverutils.TestServerInterface, *gosql.DB, func()) {
	knobs := base.TestingKnobs{
		DistSQL:          &execinfra.TestingKnobs{Changefeed: &TestingKnobs{}},
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		Server:           &server.TestingKnobs{},
	}
	if options.knobsFn != nil {
		options.knobsFn(&knobs)
	}
	args := base.TestServerArgs{
		Knobs:         knobs,
		UseDatabase:   `d`,
		ExternalIODir: options.externalIODir,
	}

	if options.argsFn != nil {
		options.argsFn(&args)
	}

	ctx := context.Background()
	resetFlushFrequency := changefeedbase.TestingSetDefaultMinCheckpointFrequency(testSinkFlushFrequency)
	s, db, _ := serverutils.StartServer(t, args)

	cleanup := func() {
		s.Stopper().Stop(ctx)
		resetFlushFrequency()
	}
	var err error
	defer func() {
		if err != nil {
			cleanup()
			require.NoError(t, err)
		}
	}()

	_, err = db.ExecContext(ctx, serverSetupStatements)
	require.NoError(t, err)

	if region := serverArgsRegion(args); region != "" {
		_, err = db.ExecContext(ctx, fmt.Sprintf(`ALTER DATABASE d PRIMARY REGION "%s"`, region))
		require.NoError(t, err)
	}

	return s, db, cleanup
}

// startTestCluster starts a 3 node cluster.
//
// Note, if a testfeed depends on particular testing knobs, those may
// need to be applied to each of the servers in the test cluster
// returned from this function.
func startTestCluster(t testing.TB) (serverutils.TestClusterInterface, *gosql.DB, func()) {
	ctx := context.Background()
	knobs := base.TestingKnobs{
		DistSQL:          &execinfra.TestingKnobs{Changefeed: &TestingKnobs{}},
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}

	resetFlushFrequency := changefeedbase.TestingSetDefaultMinCheckpointFrequency(testSinkFlushFrequency)
	cluster, db, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 3 /* numServers */, knobs,
		multiregionccltestutils.WithUseDatabase("d"),
	)
	cleanupAndReset := func() {
		cleanup()
		resetFlushFrequency()
	}

	var err error
	defer func() {
		if err != nil {
			cleanupAndReset()
			require.NoError(t, err)
		}
	}()
	_, err = db.ExecContext(ctx, serverSetupStatements)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `ALTER DATABASE d PRIMARY REGION "us-east1"`)
	return cluster, db, cleanupAndReset
}

func startTestTenant(
	t testing.TB, options feedTestOptions,
) (serverutils.TestServerInterface, *gosql.DB, func()) {
	ctx := context.Background()

	kvServer, _, cleanupCluster := startTestFullServer(t, options)
	knobs := base.TestingKnobs{
		DistSQL:          &execinfra.TestingKnobs{Changefeed: &TestingKnobs{}},
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		Server:           &server.TestingKnobs{},
	}
	if options.knobsFn != nil {
		options.knobsFn(&knobs)
	}

	tenantID := serverutils.TestTenantID()
	tenantArgs := base.TestTenantArgs{
		// crdb_internal.create_tenant called by StartTenant
		TenantID:      tenantID,
		UseDatabase:   `d`,
		TestingKnobs:  knobs,
		ExternalIODir: options.externalIODir,
	}

	tenantServer, tenantDB := serverutils.StartTenant(t, kvServer, tenantArgs)
	// Re-run setup on the tenant as well
	_, err := tenantDB.ExecContext(ctx, serverSetupStatements)
	require.NoError(t, err)

	server := &testServerShim{tenantServer, kvServer}
	// Log so that it is clear if a failed test happened
	// to run on a tenant.
	t.Logf("Running test using tenant %s", tenantID)
	return server, tenantDB, func() {
		tenantServer.Stopper().Stop(context.Background())
		log.Infof(context.Background(), "tenant server stopped")
		cleanupCluster()
		log.Infof(context.Background(), "cluster shut down")
	}
}

type cdcTestFn func(*testing.T, *gosql.DB, cdctest.TestFeedFactory)
type updateArgsFn func(args *base.TestServerArgs)
type updateKnobsFn func(knobs *base.TestingKnobs)

type feedTestOptions struct {
	useTenant     bool
	argsFn        updateArgsFn
	knobsFn       updateKnobsFn
	externalIODir string
}

type feedTestOption func(opts *feedTestOptions)

// feedTestNoTenants is a feedTestOption that will prohibit this tests
// from randomly running on a tenant.
var feedTestNoTenants = func(opts *feedTestOptions) { opts.useTenant = false }

// withArgsFn is a feedTestOption that allow the caller to modify the
// TestServerArgs before they are used to create the test server. Note
// that in multi-tenant tests, these will only apply to the kvServer
// and not the sqlServer.
func withArgsFn(fn updateArgsFn) feedTestOption {
	return func(opts *feedTestOptions) { opts.argsFn = fn }
}

// withKnobsFn is a feedTestOption that allows the caller to modify
// the testing knobs used by the test server.  For multi-tenant
// testing, these knobs are applied to both the kv and sql nodes.
func withKnobsFn(fn updateKnobsFn) feedTestOption {
	return func(opts *feedTestOptions) { opts.knobsFn = fn }
}

// Silence the linter.
var _ = withKnobsFn(nil /* fn */)

func newTestOptions() feedTestOptions {
	// percentTenant is the percentange of tests that will be run against
	// a SQL-node in a multi-tenant server. 1 for all tests to be run on a
	// tenant.
	const percentTenant = 0.25
	return feedTestOptions{
		useTenant: rand.Float32() < percentTenant,
	}
}

func makeOptions(opts ...feedTestOption) feedTestOptions {
	options := newTestOptions()
	for _, o := range opts {
		o(&options)
	}
	return options
}

func sinklessTest(testFn cdcTestFn, testOpts ...feedTestOption) func(*testing.T) {
	return sinklessTestWithOptions(testFn, makeOptions(testOpts...))
}

func sinklessTestWithOptions(testFn cdcTestFn, opts feedTestOptions) func(*testing.T) {
	return func(t *testing.T) {
		s, db, stopServer := startTestServer(t, opts)
		defer stopServer()

		sink, cleanup := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
		defer cleanup()
		f := makeSinklessFeedFactory(s, sink)
		testFn(t, db, f)
	}
}

// RunRandomSink runs the testFn against one of a number of possible
// sinks. Sinkless is not included in the possible sinks.
func RunRandomSinkTest(t *testing.T, desc string, testFn cdcTestFn, testOpts ...feedTestOption) {
	// TODO(ssd): It would be nice if explicitly selecting a test
	// via -run/TESTS= would force it to always run.
	switch p := rand.Float32(); {
	case p < 0.20:
		t.Run(fmt.Sprintf("enterprise/%s", desc), enterpriseTest(testFn, testOpts...))
	case p < 0.40:
		t.Run(fmt.Sprintf("cloudstorage/%s", desc), cloudStorageTest(testFn, testOpts...))
	case p < 0.60:
		t.Run(fmt.Sprintf("webhook/%s", desc), webhookTest(testFn, testOpts...))
	default: // Run kafka a bit more often
		t.Run(fmt.Sprintf("kafka/%s", desc), kafkaTest(testFn, testOpts...))
	}
}

func enterpriseTest(testFn cdcTestFn, testOpts ...feedTestOption) func(*testing.T) {
	return enterpriseTestWithOptions(testFn, makeOptions(testOpts...))
}

func enterpriseTestWithOptions(testFn cdcTestFn, options feedTestOptions) func(*testing.T) {
	return func(t *testing.T) {
		s, db, stopServer := startTestServer(t, options)
		defer stopServer()

		sink, cleanup := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
		defer cleanup()
		f := makeTableFeedFactory(s, db, sink)

		testFn(t, db, f)
	}
}

func cloudStorageTest(testFn cdcTestFn, testOpts ...feedTestOption) func(*testing.T) {
	return cloudStorageTestWithOptions(testFn, makeOptions(testOpts...))
}

func cloudStorageTestWithOptions(testFn cdcTestFn, options feedTestOptions) func(*testing.T) {
	return func(t *testing.T) {
		if options.externalIODir == "" {
			dir, dirCleanupFn := testutils.TempDir(t)
			defer dirCleanupFn()
			options.externalIODir = dir
		}
		oldKnobsFn := options.knobsFn
		options.knobsFn = func(knobs *base.TestingKnobs) {
			if oldKnobsFn != nil {
				oldKnobsFn(knobs)
			}
			blobClientFactory := blobs.NewLocalOnlyBlobClientFactory(options.externalIODir)
			if serverKnobs, ok := knobs.Server.(*server.TestingKnobs); ok {
				serverKnobs.BlobClientFactory = blobClientFactory
			} else {
				knobs.Server = &server.TestingKnobs{
					BlobClientFactory: blobClientFactory,
				}
			}
		}
		s, db, stopServer := startTestServer(t, options)
		defer stopServer()

		f := makeCloudFeedFactory(s, db, options.externalIODir)
		testFn(t, db, f)
	}
}

func kafkaTest(testFn cdcTestFn, testOpts ...feedTestOption) func(t *testing.T) {
	return kafkaTestWithOptions(testFn, makeOptions(testOpts...))
}

func kafkaTestWithOptions(testFn cdcTestFn, options feedTestOptions) func(*testing.T) {
	return func(t *testing.T) {
		s, db, stopServer := startTestServer(t, options)
		defer stopServer()
		f := makeKafkaFeedFactory(s, db)
		testFn(t, db, f)
	}
}

func webhookTest(testFn cdcTestFn, testOpts ...feedTestOption) func(t *testing.T) {
	return webhookTestWithOptions(testFn, makeOptions(testOpts...))
}

func webhookTestWithOptions(testFn cdcTestFn, options feedTestOptions) func(*testing.T) {
	return func(t *testing.T) {
		s, db, stopServer := startTestServer(t, options)
		defer stopServer()
		f := makeWebhookFeedFactory(s, db)
		testFn(t, db, f)
	}
}

func pubsubTest(testFn cdcTestFn, testOpts ...feedTestOption) func(t *testing.T) {
	return pubsubTestWithOptions(testFn, makeOptions(testOpts...))
}

func pubsubTestWithOptions(testFn cdcTestFn, options feedTestOptions) func(*testing.T) {
	return func(t *testing.T) {
		s, db, stopServer := startTestServer(t, options)
		defer stopServer()
		f := makePubsubFeedFactory(s, db)
		testFn(t, db, f)
	}
}

func serverArgsRegion(args base.TestServerArgs) string {
	for _, tier := range args.Locality.Tiers {
		if tier.Key == "region" {
			return tier.Value
		}
	}
	return ""
}

// expectNotice creates a pretty crude database connection that doesn't involve
// a lot of cdc test framework, use with caution. Driver-agnostic tools don't
// have clean ways of inspecting incoming notices.
func expectNotice(t *testing.T, s serverutils.TestServerInterface, sql string, expected string) {
	url, cleanup := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanup()
	base, err := pq.NewConnector(url.String())
	if err != nil {
		t.Fatal(err)
	}
	actual := "(no notice)"
	connector := pq.ConnectorWithNoticeHandler(base, func(n *pq.Error) {
		actual = n.Message
	})

	dbWithHandler := gosql.OpenDB(connector)
	defer dbWithHandler.Close()
	sqlDB := sqlutils.MakeSQLRunner(dbWithHandler)

	sqlDB.Exec(t, sql)

	require.Equal(t, expected, actual)
}

func feed(
	t testing.TB, f cdctest.TestFeedFactory, create string, args ...interface{},
) cdctest.TestFeed {
	t.Helper()
	feed, err := f.Feed(create, args...)
	if err != nil {
		t.Fatal(err)
	}
	return feed
}

func closeFeed(t testing.TB, f cdctest.TestFeed) {
	t.Helper()
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func forceTableGC(
	t testing.TB,
	tsi serverutils.TestServerInterface,
	sqlDB *sqlutils.SQLRunner,
	database, table string,
) {
	t.Helper()
	if err := tsi.ForceTableGC(context.Background(), database, table, tsi.Clock().Now()); err != nil {
		t.Fatal(err)
	}
}
