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
	"math"
	"math/rand"
	"net/url"
	"os"
	"reflect"
	"regexp"
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
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

var testSinkFlushFrequency = 100 * time.Millisecond

// disableDeclarativeSchemaChangesForTest tests that are disabled due to differences
// in changefeed behaviour and are tracked by issue #80545.
func disableDeclarativeSchemaChangesForTest(t testing.TB, sqlDB *sqlutils.SQLRunner) {
	sqlDB.Exec(t, "SET use_declarative_schema_changer='off'")
	sqlDB.Exec(t, "SET CLUSTER SETTING  sql.defaults.use_declarative_schema_changer='off'")
}

func waitForSchemaChange(
	t testing.TB, sqlDB *sqlutils.SQLRunner, stmt string, arguments ...interface{},
) {
	sqlDB.Exec(t, stmt, arguments...)
	row := sqlDB.QueryRow(t, "SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'NEW SCHEMA CHANGE' OR job_type ='SCHEMA CHANGE' ORDER BY created DESC LIMIT 1")
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

func readNextMessages(
	ctx context.Context, f cdctest.TestFeed, numMessages int,
) ([]cdctest.TestFeedMessage, error) {
	var actual []cdctest.TestFeedMessage
	lastMessage := timeutil.Now()
	for len(actual) < numMessages {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		m, err := f.Next()
		if log.V(1) {
			if m != nil {
				log.Infof(context.Background(), `msg %s: %s->%s (%s) (%s)`,
					m.Topic, m.Key, m.Value, m.Resolved, timeutil.Since(lastMessage))
			} else {
				log.Infof(context.Background(), `err %v`, err)
			}
		}
		lastMessage = timeutil.Now()
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
	timeout := assertPayloadsTimeout()
	if len(expected) > 100 {
		// Webhook sink is very slow; We have few tests that read 1000 messages.
		timeout += 5 * time.Minute
	}

	require.NoError(t,
		withTimeout(f, timeout,
			func(ctx context.Context) error {
				return assertPayloadsBaseErr(ctx, f, expected, stripTs, perKeyOrdered)
			},
		))
}

func assertPayloadsBaseErr(
	ctx context.Context, f cdctest.TestFeed, expected []string, stripTs bool, perKeyOrdered bool,
) error {
	actual, err := readNextMessages(ctx, f, len(expected))
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

func assertPayloadsTimeout() time.Duration {
	if util.RaceEnabled {
		return 5 * time.Minute
	}
	return 30 * time.Second
}

func withTimeout(
	f cdctest.TestFeed, timeout time.Duration, fn func(ctx context.Context) error,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	defer stopFeedWhenDone(ctx, f)()
	return fn(ctx)
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
	ts, err := hlc.DecimalToHLC(d)
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
		Knobs: knobs,
		// This test suite is already probabilistically running with
		// tenants. No need for the test tenant.
		DisableDefaultTestTenant: true,
		UseDatabase:              `d`,
		ExternalIODir:            options.externalIODir,
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

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.ExecMultiple(t, strings.Split(serverSetupStatements, ";")...)

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
	skip.UnderStressRace(t, "multinode setup doesn't work under testrace")
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
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.ExecMultiple(t, strings.Split(serverSetupStatements, ";")...)

	_, err = db.ExecContext(ctx, `ALTER DATABASE d PRIMARY REGION "us-east1"`)
	return cluster, db, cleanupAndReset
}

func waitForTenantPodsActive(
	t testing.TB, tenantServer serverutils.TestTenantInterface, numPods int,
) {
	testutils.SucceedsWithin(t, func() error {
		status := tenantServer.StatusServer().(serverpb.SQLStatusServer)
		var nodes *serverpb.NodesListResponse
		var err error
		for nodes == nil || len(nodes.Nodes) != numPods {
			nodes, err = status.NodesList(context.Background(), nil)
			if err != nil {
				return err
			}
		}
		return nil
	}, 10*time.Second)
}

func startTestTenant(
	t testing.TB, systemServer serverutils.TestServerInterface, options feedTestOptions,
) (roachpb.TenantID, serverutils.TestTenantInterface, *gosql.DB, func()) {
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

	tenantServer, tenantDB := serverutils.StartTenant(t, systemServer, tenantArgs)
	// Re-run setup on the tenant as well
	tenantRunner := sqlutils.MakeSQLRunner(tenantDB)
	tenantRunner.ExecMultiple(t, strings.Split(serverSetupStatements, ";")...)

	waitForTenantPodsActive(t, tenantServer, 1)

	return tenantID, tenantServer, tenantDB, func() {
		tenantServer.Stopper().Stop(context.Background())
	}
}

type cdcTestFn func(*testing.T, TestServer, cdctest.TestFeedFactory)
type cdcTestWithSystemFn func(*testing.T, TestServerWithSystem, cdctest.TestFeedFactory)
type updateArgsFn func(args *base.TestServerArgs)
type updateKnobsFn func(knobs *base.TestingKnobs)

type feedTestOptions struct {
	useTenant         bool
	argsFn            updateArgsFn
	knobsFn           updateKnobsFn
	externalIODir     string
	allowedSinkTypes  []string
	disabledSinkTypes []string
}

type feedTestOption func(opts *feedTestOptions)

// feedTestNoTenants is a feedTestOption that will prohibit this tests
// from randomly running on a tenant.
var feedTestNoTenants = func(opts *feedTestOptions) { opts.useTenant = false }

var feedTestForceSink = func(sinkType string) feedTestOption {
	return feedTestRestrictSinks(sinkType)
}

var feedTestRestrictSinks = func(sinkTypes ...string) feedTestOption {
	return func(opts *feedTestOptions) { opts.allowedSinkTypes = append(opts.allowedSinkTypes, sinkTypes...) }
}

var feedTestEnterpriseSinks = func(opts *feedTestOptions) {
	feedTestOmitSinks("sinkless")(opts)
}

var feedTestOmitSinks = func(sinkTypes ...string) feedTestOption {
	return func(opts *feedTestOptions) { opts.disabledSinkTypes = append(opts.disabledSinkTypes, sinkTypes...) }
}

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
	// percentTenant is the percentage of tests that will be run against
	// a SQL-node in a multi-tenant server. 1 for all tests to be run on a
	// tenant.
	const percentTenant = 0.5
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
func expectNotice(t *testing.T, s serverutils.TestTenantInterface, sql string, expected string) {
	url, cleanup := sqlutils.PGUrl(t, s.SQLAddr(), t.Name(), url.User(username.RootUser))
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

func asUser(t testing.TB, f cdctest.TestFeedFactory, user string, fn func()) {
	t.Helper()
	require.NoError(t, f.AsUser(user, fn))
}

func expectErrCreatingFeed(
	t testing.TB, f cdctest.TestFeedFactory, create string, errSubstring string,
) {
	t.Helper()
	t.Logf("expecting %s to error", create)
	feed, err := f.Feed(create)
	if feed != nil {
		defer func() { _ = feed.Close() }()
	}
	if err == nil {
		// Sinkless test feeds don't error until you try to read the first row.
		if _, sinkless := feed.(*sinklessFeed); sinkless {
			_, err = feed.Next()
		}
	}
	if err == nil {
		t.Errorf("No error from %s", create)
	} else {
		require.Contains(t, err.Error(), errSubstring)
	}
}

func closeFeed(t testing.TB, f cdctest.TestFeed) {
	t.Helper()
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestServer is a struct to allow tests to operate on a shared API regardless
// of a test running as the system tenant or a secondary tenant
type TestServer struct {
	DB           *gosql.DB
	Server       serverutils.TestTenantInterface
	Codec        keys.SQLCodec
	TestingKnobs base.TestingKnobs
}

// TestServerWithSystem provides access to the system db and server for a
// TestServer.  This is useful for some tests that explicitly require access to
// the system tenant, for example if
// desctestutils.TestingGetPublicTableDescriptor is being called.
type TestServerWithSystem struct {
	TestServer
	SystemDB     *gosql.DB
	SystemServer serverutils.TestServerInterface
}

func makeSystemServer(
	t *testing.T, opts ...feedTestOption,
) (testServer TestServerWithSystem, cleanup func()) {
	options := makeOptions(opts...)
	return makeSystemServerWithOptions(t, options)
}

var _ = makeSystemServer // silence unused warning

func makeSystemServerWithOptions(
	t *testing.T, options feedTestOptions,
) (testServer TestServerWithSystem, cleanup func()) {
	systemServer, systemDB, clusterCleanup := startTestFullServer(t, options)
	return TestServerWithSystem{
			TestServer: TestServer{
				DB:           systemDB,
				Server:       systemServer,
				TestingKnobs: systemServer.(*server.TestServer).Cfg.TestingKnobs,
				Codec:        keys.SystemSQLCodec,
			},
			SystemServer: systemServer,
			SystemDB:     systemDB,
		}, func() {
			clusterCleanup()
		}
}

func makeTenantServer(
	t *testing.T, opts ...feedTestOption,
) (testServer TestServerWithSystem, cleanup func()) {
	options := makeOptions(opts...)
	return makeTenantServerWithOptions(t, options)
}
func makeTenantServerWithOptions(
	t *testing.T, options feedTestOptions,
) (testServer TestServerWithSystem, cleanup func()) {
	systemServer, systemDB, clusterCleanup := startTestFullServer(t, options)
	tenantID, tenantServer, tenantDB, tenantCleanup := startTestTenant(t, systemServer, options)

	return TestServerWithSystem{
			TestServer: TestServer{
				DB:           tenantDB,
				Server:       tenantServer,
				TestingKnobs: tenantServer.(*server.TestTenant).Cfg.TestingKnobs,
				Codec:        keys.MakeSQLCodec(tenantID),
			},
			SystemDB:     systemDB,
			SystemServer: systemServer,
		}, func() {
			tenantCleanup()
			clusterCleanup()
		}
}

func makeServer(
	t *testing.T, opts ...feedTestOption,
) (testServer TestServerWithSystem, cleanup func()) {
	options := makeOptions(opts...)
	return makeServerWithOptions(t, options)
}

func makeServerWithOptions(
	t *testing.T, options feedTestOptions,
) (server TestServerWithSystem, cleanup func()) {
	if options.useTenant {
		t.Logf("making server as secondary tenant")
		return makeTenantServerWithOptions(t, options)
	}
	t.Logf("making server as system tenant")
	return makeSystemServerWithOptions(t, options)
}

func randomSinkType(opts ...feedTestOption) string {
	options := makeOptions(opts...)
	return randomSinkTypeWithOptions(options)
}

func randomSinkTypeWithOptions(options feedTestOptions) string {
	sinkWeights := map[string]int{
		"kafka":        2, // run kafka a bit more often
		"enterprise":   1,
		"webhook":      1,
		"pubsub":       1,
		"sinkless":     1,
		"cloudstorage": 0, // requires externalIODir set
	}
	if options.externalIODir != "" {
		sinkWeights["cloudstorage"] = 1
	}
	if options.allowedSinkTypes != nil {
		sinkWeights = map[string]int{}
		for _, sinkType := range options.allowedSinkTypes {
			sinkWeights[sinkType] = 1
		}
	}
	if options.disabledSinkTypes != nil {
		for _, sinkType := range options.disabledSinkTypes {
			sinkWeights[sinkType] = 0
		}
	}
	weightTotal := 0
	for _, weight := range sinkWeights {
		weightTotal += weight
	}
	p := rand.Float32() * float32(weightTotal)
	var sum float32 = 0
	for sink, weight := range sinkWeights {
		sum += float32(weight)
		if p <= sum {
			return sink
		}
	}
	return "kafka" // unreachable
}

// addCloudStorageOptions adds the options necessary to enable a server to run a
// cloudstorage changefeed on it
func addCloudStorageOptions(t *testing.T, options *feedTestOptions) (cleanup func()) {
	dir, dirCleanupFn := testutils.TempDir(t)
	options.externalIODir = dir
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
	return dirCleanupFn
}

func makeFeedFactory(
	t *testing.T,
	sinkType string,
	s serverutils.TestTenantInterface,
	db *gosql.DB,
	testOpts ...feedTestOption,
) (factory cdctest.TestFeedFactory, sinkCleanup func()) {
	options := makeOptions(testOpts...)
	return makeFeedFactoryWithOptions(t, sinkType, s, db, options)
}

func makeFeedFactoryWithOptions(
	t *testing.T,
	sinkType string,
	s serverutils.TestTenantInterface,
	db *gosql.DB,
	options feedTestOptions,
) (factory cdctest.TestFeedFactory, sinkCleanup func()) {
	t.Logf("making %s feed factory", sinkType)
	pgURLForUser := func(u string, pass ...string) (url.URL, func()) {
		t.Logf("pgURL %s %s", sinkType, u)
		if len(pass) < 1 {
			return sqlutils.PGUrl(t, s.SQLAddr(), t.Name(), url.User(u))
		}
		return url.URL{
			Scheme: "postgres",
			User:   url.UserPassword(u, pass[0]),
			Host:   s.SQLAddr()}, func() {}
	}
	switch sinkType {
	case "kafka":
		f := makeKafkaFeedFactory(s, db)
		return f, func() {}
	case "cloudstorage":
		if options.externalIODir == "" {
			t.Fatalf("expected externalIODir option to be set")
		}
		f := makeCloudFeedFactory(s, db, options.externalIODir)
		return f, func() {}
	case "enterprise":
		sink, cleanup := pgURLForUser(username.RootUser)
		f := makeTableFeedFactory(s, db, sink)
		return f, cleanup
	case "webhook":
		f := makeWebhookFeedFactory(s, db)
		return f, func() {}
	case "pubsub":
		f := makePubsubFeedFactory(s, db)
		return f, func() {}
	case "sinkless":
		sink, cleanup := pgURLForUser(username.RootUser)
		f := makeSinklessFeedFactory(s, sink, pgURLForUser)
		return f, cleanup
	}
	t.Fatalf("unhandled sink type %s", sinkType)
	return nil, nil
}

func cdcTest(t *testing.T, testFn cdcTestFn, testOpts ...feedTestOption) {
	cdcTestNamed(t, "", testFn, testOpts...)
}

func cdcTestNamed(t *testing.T, name string, testFn cdcTestFn, testOpts ...feedTestOption) {
	testFnWithSystem := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		testFn(t, s.TestServer, f)
	}
	cdcTestNamedWithSystem(t, "", testFnWithSystem, testOpts...)
}

func cdcTestWithSystem(t *testing.T, testFn cdcTestWithSystemFn, testOpts ...feedTestOption) {
	cdcTestNamedWithSystem(t, "", testFn, testOpts...)
}

func cdcTestNamedWithSystem(
	t *testing.T, name string, testFn cdcTestWithSystemFn, testOpts ...feedTestOption,
) {
	t.Helper()
	options := makeOptions(testOpts...)
	cleanupCloudStorage := addCloudStorageOptions(t, &options)

	sinkType := randomSinkTypeWithOptions(options)
	testLabel := sinkType
	if name != "" {
		testLabel = fmt.Sprintf("%s/%s", sinkType, name)
	}
	t.Run(testLabel, func(t *testing.T) {
		testServer, cleanupServer := makeServerWithOptions(t, options)
		feedFactory, cleanupSink := makeFeedFactoryWithOptions(t, sinkType, testServer.Server, testServer.DB, options)
		defer cleanupServer()
		defer cleanupSink()
		defer cleanupCloudStorage()

		testFn(t, testServer, feedFactory)
	})
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

// All structured logs should contain this property which stores the snake_cased
// version of the name of the message struct
type BaseEventStruct struct {
	EventType string
}

var cmLogRe = regexp.MustCompile(`event_log\.go`)

func checkStructuredLogs(t *testing.T, eventType string, startTime int64) []string {
	var matchingEntries []string
	testutils.SucceedsSoon(t, func() error {
		log.Flush()
		entries, err := log.FetchEntriesFromFiles(startTime,
			math.MaxInt64, 10000, cmLogRe, log.WithMarkedSensitiveData)
		if err != nil {
			t.Fatal(err)
		}

		for _, e := range entries {
			jsonPayload := []byte(e.Message)
			var baseStruct BaseEventStruct
			if err := gojson.Unmarshal(jsonPayload, &baseStruct); err != nil {
				continue
			}
			if baseStruct.EventType != eventType {
				continue
			}

			matchingEntries = append(matchingEntries, e.Message)
		}

		return nil
	})

	return matchingEntries
}

func checkCreateChangefeedLogs(t *testing.T, startTime int64) []eventpb.CreateChangefeed {
	var matchingEntries []eventpb.CreateChangefeed

	for _, m := range checkStructuredLogs(t, "create_changefeed", startTime) {
		jsonPayload := []byte(m)
		var event eventpb.CreateChangefeed
		if err := gojson.Unmarshal(jsonPayload, &event); err != nil {
			t.Errorf("unmarshalling %q: %v", m, err)
		}
		matchingEntries = append(matchingEntries, event)
	}

	return matchingEntries
}

func checkChangefeedFailedLogs(t *testing.T, startTime int64) []eventpb.ChangefeedFailed {
	var matchingEntries []eventpb.ChangefeedFailed

	for _, m := range checkStructuredLogs(t, "changefeed_failed", startTime) {
		jsonPayload := []byte(m)
		var event eventpb.ChangefeedFailed
		if err := gojson.Unmarshal(jsonPayload, &event); err != nil {
			t.Errorf("unmarshalling %q: %v", m, err)
		}
		matchingEntries = append(matchingEntries, event)
	}

	return matchingEntries
}

func checkS3Credentials(t *testing.T) (bucket string, accessKey string, secretKey string) {
	accessKey = os.Getenv("AWS_ACCESS_KEY_ID")
	if accessKey == "" {
		skip.IgnoreLint(t, "AWS_ACCESS_KEY_ID env var must be set")
	}
	secretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	if secretKey == "" {
		skip.IgnoreLint(t, "AWS_SECRET_ACCESS_KEY env var must be set")
	}
	bucket = os.Getenv("AWS_S3_BUCKET")
	if bucket == "" {
		skip.IgnoreLint(t, "AWS_S3_BUCKET env var must be set")
	}

	return bucket, accessKey, secretKey
}

func waitForJobStatus(
	runner *sqlutils.SQLRunner, t *testing.T, id jobspb.JobID, targetStatus jobs.Status,
) {
	testutils.SucceedsSoon(t, func() error {
		var jobStatus string
		query := `SELECT status FROM [SHOW CHANGEFEED JOB $1]`
		runner.QueryRow(t, query, id).Scan(&jobStatus)
		if targetStatus != jobs.Status(jobStatus) {
			return errors.Errorf("Expected status:%s but found status:%s", targetStatus, jobStatus)
		}
		return nil
	})
}
