// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
// This file provides generic interfaces that allow tests to set up test servers
// without importing the server package (avoiding circular dependencies).
// To be used, the binary needs to call
// InitTestServerFactory(server.TestServerFactory), generally from a TestMain()
// in an "foo_test" package (which can import server and is linked together with
// the other tests in package "foo").

package serverutils

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvadmission"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

const defaultTestTenantName = roachpb.TenantName("test-tenant")

// defaultTestTenantMessage is a message that is printed when a test is run
// under cluster virtualization. This is useful for debugging test failures.
//
// If you see this message, the test server was configured to route SQL queries
// to a virtual cluster (secondary tenant). If you are only seeing a test
// failure when this message appears, there may be a problem specific to cluster
// virtualization or multi-tenancy. A virtual cluster can be started either as a
// shared process or as an external process. The problem may be specific to one
// of these modes, so it is important to note which mode was used from the log
// message.
//
// To investigate, consider using "COCKROACH_TEST_TENANT=true" to force-enable
// just the virtual cluster in all runs (or, alternatively, "false" to
// force-disable), or use "COCKROACH_INTERNAL_DISABLE_METAMORPHIC_TESTING=true"
// to disable all random test variables altogether.`

var defaultTestTenantMessage = func(sharedProcess bool) string {
	processModeMessage := "an external process"
	if sharedProcess {
		processModeMessage = "a shared process"
	}
	return fmt.Sprintf(
		`automatically injected %s virtual cluster under test; see comment at top of test_server_shim.go for details.`,
		processModeMessage,
	)
}

var PreventStartTenantError = errors.New("attempting to manually start a virtual cluster while " +
	"DefaultTestTenant is set to TestTenantProbabilisticOnly")

// ShouldStartDefaultTestTenant determines whether a default test tenant
// should be started for test servers or clusters, to serve SQL traffic by
// default. It returns a new base.DefaultTestTenantOptions that reflects
// the decision that was taken.
//
// The decision can be overridden either via the build tag `metamorphic_disable`
// or just for test tenants via COCKROACH_TEST_TENANT.
//
// This function is included in package 'serverutils' instead of 'server.testServer'
// directly so that it only gets linked into test code (and to avoid a linter
// error that 'skip' must only be used in test code).
func ShouldStartDefaultTestTenant(
	t TestLogger, baseArg base.DefaultTestTenantOptions,
) (retval base.DefaultTestTenantOptions) {
	// Explicit case for disabling the default test tenant.
	if baseArg.TestTenantAlwaysDisabled() {
		if issueNum, label := baseArg.IssueRef(); issueNum != 0 {
			if t != nil {
				t.Logf("cluster virtualization disabled due to issue: #%d (expected label: %s)", issueNum, label)
			}
		}
		return baseArg
	}

	if skip.UnderBench() {
		// Until #83461 is resolved, we want to make sure that we don't use the
		// multi-tenant setup so that the comparison against old single-tenant
		// SHAs in the benchmarks is fair.
		return base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(83461)
	}

	// If the test tenant is explicitly enabled and a process mode selected, then
	// we are done.
	if !baseArg.TestTenantNoDecisionMade() {
		if t != nil {
			t.Log("using test tenant configuration from test explicit setting")
		}
		return baseArg
	}

	if globalDefaultSelectionOverride.isSet {
		override := globalDefaultSelectionOverride.value
		if override.TestTenantNoDecisionMade() {
			panic("programming error: global override does not contain a final decision")
		}
		if override.TestTenantAlwaysDisabled() {
			if issueNum, label := override.IssueRef(); issueNum != 0 && t != nil {
				t.Logf("cluster virtualization disabled in global scope due to issue: #%d (expected label: %s)", issueNum, label)
			}
		} else {
			if t != nil {
				t.Log(defaultTestTenantMessage(override.SharedProcessMode()) + "\n(via override from TestingSetDefaultTenantSelectionOverride)")
			}
		}
		return override
	}

	if factoryDefaultTenant != nil {
		defaultArg := *factoryDefaultTenant
		// If factory default made a decision, return it.
		if !defaultArg.TestTenantNoDecisionMade() {
			if t != nil {
				t.Log("using test tenant configuration from testserver factory defaults")
			}
			if defaultArg.TestTenantAlwaysDisabled() {
				if issueNum, label := defaultArg.IssueRef(); issueNum != 0 && t != nil {
					t.Logf("cluster virtualization disabled due to issue: #%d (expected label: %s)", issueNum, label)
				}
			}
			return defaultArg
		}
	}

	// Determine if the default test tenant should be run as a shared process.
	var shared bool
	switch {
	case baseArg.SharedProcessMode():
		shared = true
	case baseArg.ExternalProcessMode():
		shared = false
	default:
		// If no explicit process mode was selected, then randomly select one.
		rng, _ := randutil.NewTestRand()
		shared = rng.Intn(2) == 0
	}

	// Explicit case for enabling the default test tenant, but with a
	// probabilistic selection made for running as a shared or external process.
	if baseArg.TestTenantAlwaysEnabled() {
		return base.InternalNonDefaultDecision(baseArg, true /* enabled */, shared /* shared */)
	}

	if decision, override := testTenantDecisionFromEnvironment(baseArg, shared); override {
		if decision.TestTenantAlwaysEnabled() {
			if t != nil {
				t.Log(defaultTestTenantMessage(decision.SharedProcessMode()) + "\n(override via COCKROACH_TEST_TENANT)")
			}
		}
		return decision
	}

	// Note: we ask the metamorphic framework for a "disable" value, instead
	// of an "enable" value, because it probabilistically returns its default value
	// more often than not and that is what we want.
	enabled := !metamorphic.ConstantWithTestBoolWithoutLogging("disable-test-tenant", false)
	if enabled && t != nil {
		t.Log(defaultTestTenantMessage(shared))
	}
	if enabled {
		return base.InternalNonDefaultDecision(baseArg, true /* enable */, shared /* shared */)
	}
	return base.InternalNonDefaultDecision(baseArg, false /* enable */, false /* shared */)
}

const (
	// COCKROACH_TEST_TENANT controls whether a secondary tenant
	// is used by a TestServer-based test.
	//
	// - false disables the use of tenants;
	//
	// - true forces the use of tenants, randomly deciding between
	//   an external or shared process tenant;
	//
	// - shared forces the use of a tenant, always starting a
	//   shared process tenant;
	//
	// - external forces the use of a tenant, always starting a
	//   separate process tenant.
	testTenantEnabledEnvVar = "COCKROACH_TEST_TENANT"

	testTenantModeEnabledShared   = "shared"
	testTenantModeEnabledExternal = "external"

	// COCKROACH_TEST_DRPC controls the DRPC enablement mode for test servers.
	//
	// - disabled: disables DRPC; all inter-node connectivity will use gRPC only
	//
	// - enabled: enables DRPC for inter-node connectivity
	testDRPCEnabledEnvVar = "COCKROACH_TEST_DRPC"
)

func testTenantDecisionFromEnvironment(
	baseArg base.DefaultTestTenantOptions, shared bool,
) (base.DefaultTestTenantOptions, bool) {
	if str, present := envutil.EnvString(testTenantEnabledEnvVar, 0); present {
		v, err := strconv.ParseBool(str)
		if err == nil {
			if v {
				return base.InternalNonDefaultDecision(baseArg, true /* enabled */, shared /* shared */), true
			}
			return base.InternalNonDefaultDecision(baseArg, false /* enabled */, false /* shared */), true
		}

		switch str {
		case testTenantModeEnabledShared:
			return base.InternalNonDefaultDecision(baseArg, true /* enabled */, true /* shared */), true
		case testTenantModeEnabledExternal:
			return base.InternalNonDefaultDecision(baseArg, true /* enabled */, false /* shared */), true
		default:
			panic(fmt.Sprintf("invalid value for %s: %s", testTenantEnabledEnvVar, str))
		}
	}
	return baseArg, false
}

var globalDefaultDRPCOptionOverride struct {
	isSet bool
	value base.DefaultTestDRPCOption
}

// TestingGlobalDRPCOption sets the package-level DefaultTestDRPCOption.
func TestingGlobalDRPCOption(v base.DefaultTestDRPCOption) func() {
	globalDefaultDRPCOptionOverride.isSet = true
	globalDefaultDRPCOptionOverride.value = v
	return func() {
		globalDefaultDRPCOptionOverride.isSet = false
	}
}

// globalDefaultSelectionOverride is used when an entire package needs
// to override the probabilistic behavior.
var globalDefaultSelectionOverride struct {
	isSet bool
	value base.DefaultTestTenantOptions
}

// TestingSetDefaultTenantSelectionOverride changes the global selection override.
func TestingSetDefaultTenantSelectionOverride(v base.DefaultTestTenantOptions) func() {
	globalDefaultSelectionOverride.isSet = true
	globalDefaultSelectionOverride.value = v
	return func() {
		globalDefaultSelectionOverride.isSet = false
	}
}

var srvFactoryImpl TestServerFactory
var factoryDefaultDRPC *base.DefaultTestDRPCOption
var factoryDefaultTenant *base.DefaultTestTenantOptions

type TestServerFactoryOption func()

func WithTenantOption(opt base.DefaultTestTenantOptions) TestServerFactoryOption {
	return func() {
		factoryDefaultTenant = &opt
	}
}
func WithDRPCOption(opt base.DefaultTestDRPCOption) TestServerFactoryOption {
	return func() {
		factoryDefaultDRPC = &opt
	}
}

// InitTestServerFactory should be called once to provide the implementation
// of the service. It will be called from a xx_test package that can import the
// server package. Optional parameters can be passed to set default options:
// - WithDRPCOption: Sets the default DRPC mode for test servers
// - WithTenantOption: Sets the default tenant configuration for test servers
func InitTestServerFactory(impl TestServerFactory, opts ...TestServerFactoryOption) {
	srvFactoryImpl = impl
	for _, opt := range opts {
		opt()
	}
}

// TestLogger is the minimal interface of testing.T that is used by
// StartServerOnlyE.
type TestLogger interface {
	Helper()
	Log(args ...interface{})
	Logf(format string, args ...interface{})
}

// TestFataler is the minimal interface of testing.T that is used by
// StartServer.
type TestFataler interface {
	TestLogger
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	FailNow()
}

// StartServerOnlyE is like StartServerOnly() but it lets
// the test decide what to do with the error.
//
// The first argument is optional. If non-nil; it is used for logging
// server configuration messages.
func StartServerOnlyE(t TestLogger, params base.TestServerArgs) (TestServerInterface, error) {
	ctx := context.Background()
	allowAdditionalTenants := params.DefaultTestTenant.AllowAdditionalTenants()

	// Update the flags with the actual decisions for test configuration.
	// Priority of these Should* functions:
	// Test explicit value > global override > factory defaults > env vars > metamorphic.
	params.DefaultTestTenant = ShouldStartDefaultTestTenant(t, params.DefaultTestTenant)
	params.DefaultDRPCOption = ShouldEnableDRPC(ctx, t, params.DefaultDRPCOption)

	s, err := NewServer(params)
	if err != nil {
		return nil, err
	}

	if t != nil {
		if w, ok := s.(*wrap); ok {
			// Redirect the info/warning messages to the test logs.
			w.loggerFn = t.Logf
		}
	}

	if err := s.Start(ctx); err != nil {
		return nil, err
	}

	if !allowAdditionalTenants {
		s.TenantController().DisableStartTenant(PreventStartTenantError)
	}

	return s, nil
}

// StartServerOnly creates and starts a test server.
// The returned server should be stopped by calling
// server.Stopper().Stop().
func StartServerOnly(t TestFataler, params base.TestServerArgs) TestServerInterface {
	s, err := StartServerOnlyE(t, params)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

var ConfigureSlimTestServer func(params base.TestServerArgs) base.TestServerArgs

func StartSlimServerOnly(
	t TestFataler, params base.TestServerArgs, slimOpts ...base.SlimServerOption,
) TestServerInterface {
	params.SlimServerConfig(slimOpts...)
	return StartServerOnly(t, params)
}

func StartSlimServer(
	t TestFataler, params base.TestServerArgs, slimOpts ...base.SlimServerOption,
) (TestServerInterface, *gosql.DB, *kv.DB) {
	s := StartSlimServerOnly(t, params, slimOpts...)
	goDB := s.ApplicationLayer().SQLConn(t, DBName(params.UseDatabase))
	kvDB := s.ApplicationLayer().DB()
	return s, goDB, kvDB
}

// StartServer creates and starts a test server.
// The returned server should be stopped by calling
// server.Stopper().Stop().
//
// The second and third return values are equivalent to
// .ApplicationLayer().SQLConn() and .ApplicationLayer().DB(),
// respectively. If your test does not need them, consider
// using StartServerOnly() instead.
func StartServer(
	t TestFataler, params base.TestServerArgs,
) (TestServerInterface, *gosql.DB, *kv.DB) {
	s := StartServerOnly(t, params)
	goDB := s.ApplicationLayer().SQLConn(t, DBName(params.UseDatabase))
	kvDB := s.ApplicationLayer().DB()
	return s, goDB, kvDB
}

// NewServer creates a test server.
func NewServer(params base.TestServerArgs) (TestServerInterface, error) {
	if srvFactoryImpl == nil {
		return nil, errors.AssertionFailedf("TestServerFactory not initialized. One needs to be injected " +
			"from the package's TestMain()")
	}
	tcfg := params.DefaultTestTenant
	if tcfg.TestTenantNoDecisionMade() {
		return nil, errors.AssertionFailedf("programming error: DefaultTestTenant does not contain a decision\n(maybe call ShouldStartDefaultTestTenant?)")
	}

	if params.DefaultTenantName == "" {
		params.DefaultTenantName = defaultTestTenantName
	}

	// Allow access to unsafe internals for this server.
	SetUnsafeOverride(&params.Knobs)

	srv, err := srvFactoryImpl.New(params)
	if err != nil {
		return nil, err
	}
	// TestServers run in CI often are running under sustained overload conditions
	// causing background work, normally expected to yield until spare capacity is
	// available, to be starved out indefinitely -- just running the various
	// system components/rangefeeds/etc of several testservers/tenants is enough
	// to have a perpetually non-empty runnable queue in CIs for 10+ minutes. As
	// such, we have to disable yield AC if we want background work to run at all.
	if params.DisableElasticCPUAdmission {
		kvadmission.ElasticAdmission.Override(context.Background(), &srv.(TestServerInterfaceRaw).ClusterSettings().SV, false)
		// Disable elastic CPU control settings that can cause bulk operations and
		// reads to be throttled. We use Lookup to avoid import cycles.
		for _, key := range []string{
			"sqladmission.low_pri_read_response_elastic_control.enabled",
			"bulkio.index_backfill.elastic_control.enabled",
			"bulkio.ingest.sst_batcher_elastic_control.enabled",
			"bulkio.import.elastic_control.enabled",
			"bulkio.backup.file_sst_sink_elastic_control.enabled",
		} {
			if s, ok := settings.LookupForLocalAccessByKey(
				settings.InternalKey(key), true, /* forSystemTenant */
			); ok {
				s.(*settings.BoolSetting).Override(context.Background(), &srv.(TestServerInterfaceRaw).ClusterSettings().SV, false)
			}
		}
	}
	admission.YieldForElasticCPU.Override(context.Background(), &srv.(TestServerInterfaceRaw).ClusterSettings().SV, false)
	srv = wrapTestServer(srv.(TestServerInterfaceRaw), tcfg)
	return srv.(TestServerInterface), nil
}

// OpenDBConnE is like OpenDBConn, but returns an error.
// Note: consider using the .SQLConnE() method on the test server instead.
func OpenDBConnE(
	sqlAddr string, useDatabase string, insecure bool, stopper *stop.Stopper,
) (*gosql.DB, error) {
	pgURL, cleanupGoDB, err := pgurlutils.PGUrlE(
		sqlAddr, "StartServer" /* prefix */, url.User(username.RootUser))
	if err != nil {
		return nil, err
	}

	pgURL.Path = useDatabase
	if insecure {
		pgURL.RawQuery = "sslmode=disable"
	}
	goDB, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		return nil, err
	}

	stopper.AddCloser(
		stop.CloserFn(func() {
			_ = goDB.Close()
			cleanupGoDB()
		}))
	return goDB, nil
}

// OpenDBConn sets up a gosql DB connection to the given server.
// Note: consider using the .SQLConn() method on the test server instead.
func OpenDBConn(
	t TestFataler, sqlAddr string, useDatabase string, insecure bool, stopper *stop.Stopper,
) *gosql.DB {
	conn, err := OpenDBConnE(sqlAddr, useDatabase, insecure, stopper)
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

// StartTenant starts a tenant SQL server connecting to the supplied test
// server. It uses the server's stopper to shut down automatically. However,
// the returned DB is for the caller to close.
//
// Note: log.Scope() should always be used in tests that start a tenant
// (otherwise, having more than one test in a package which uses StartTenant
// without log.Scope() will cause a a "clusterID already set" panic).
func StartTenant(
	t TestFataler, ts TestServerInterface, params base.TestTenantArgs,
) (ApplicationLayerInterface, *gosql.DB) {
	SetUnsafeOverride(&params.TestingKnobs)
	tenant, err := ts.TenantController().StartTenant(context.Background(), params)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	goDB := tenant.SQLConn(t, DBName(params.UseDatabase))
	return tenant, goDB
}

func StartSharedProcessTenant(
	t TestFataler, ts TestServerInterface, params base.TestSharedProcessTenantArgs,
) (ApplicationLayerInterface, *gosql.DB) {
	SetUnsafeOverride(&params.Knobs)
	tenant, goDB, err := ts.TenantController().StartSharedProcessTenant(context.Background(), params)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	return tenant, goDB
}

// TestTenantID returns a roachpb.TenantID that can be used when
// starting a test Tenant. The returned tenant IDs match those built
// into the test certificates.
func TestTenantID() roachpb.TenantID {
	return roachpb.MustMakeTenantID(securitytest.EmbeddedTenantIDs()[0])
}

// TestTenantID2 returns another roachpb.TenantID that can be used when
// starting a test Tenant. The returned tenant IDs match those built
// into the test certificates.
func TestTenantID2() roachpb.TenantID {
	return roachpb.MustMakeTenantID(securitytest.EmbeddedTenantIDs()[1])
}

// TestTenantID3 returns another roachpb.TenantID that can be used when
// starting a test Tenant. The returned tenant IDs match those built
// into the test certificates.
func TestTenantID3() roachpb.TenantID {
	return roachpb.MustMakeTenantID(securitytest.EmbeddedTenantIDs()[2])
}

// GetJSONProto uses the supplied client to GET the URL specified by the parameters
// and unmarshals the result into response.
func GetJSONProto(ts ApplicationLayerInterface, path string, response protoutil.Message) error {
	return GetJSONProtoWithAdminOption(ts, path, response, true)
}

// GetJSONProtoWithAdminOption is like GetJSONProto but the caller can customize
// whether the request is performed with admin privilege
func GetJSONProtoWithAdminOption(
	ts ApplicationLayerInterface, path string, response protoutil.Message, isAdmin bool,
) error {
	httpClient, err := ts.GetAuthenticatedHTTPClient(isAdmin, SingleTenantSession)
	if err != nil {
		return err
	}
	u := ts.AdminURL()
	fullURL := u.WithPath(path).String()
	log.Dev.Infof(context.Background(), "test retrieving protobuf over HTTP: %s", fullURL)
	return httputil.GetJSON(httpClient, fullURL, response)
}

// GetJSONProtoWithAdminAndTimeoutOption is like GetJSONProtoWithAdminOption but
// the caller can specify an additional timeout duration for the request.
func GetJSONProtoWithAdminAndTimeoutOption(
	ts ApplicationLayerInterface,
	path string,
	response protoutil.Message,
	isAdmin bool,
	additionalTimeout time.Duration,
) error {
	httpClient, err := ts.GetAuthenticatedHTTPClient(isAdmin, SingleTenantSession)
	if err != nil {
		return err
	}
	httpClient.Timeout += additionalTimeout
	u := ts.AdminURL()
	fullURL := u.WithPath(path).String()
	log.Dev.Infof(context.Background(), "test retrieving protobuf over HTTP: %s", fullURL)
	log.Dev.Infof(context.Background(), "set HTTP client timeout to: %s", httpClient.Timeout)
	return httputil.GetJSON(httpClient, fullURL, response)
}

// PostJSONProto uses the supplied client to POST the URL specified by the parameters
// and unmarshals the result into response.
func PostJSONProto(
	ts ApplicationLayerInterface, path string, request, response protoutil.Message,
) error {
	return PostJSONProtoWithAdminOption(ts, path, request, response, true)
}

// PostJSONProtoWithAdminOption is like PostJSONProto but the caller
// can customize whether the request is performed with admin
// privilege.
func PostJSONProtoWithAdminOption(
	ts ApplicationLayerInterface, path string, request, response protoutil.Message, isAdmin bool,
) error {
	httpClient, err := ts.GetAuthenticatedHTTPClient(isAdmin, SingleTenantSession)
	if err != nil {
		return err
	}
	fullURL := ts.AdminURL().WithPath(path).String()
	log.Dev.Infof(context.Background(), "test retrieving protobuf over HTTP: %s", fullURL)
	return httputil.PostJSON(httpClient, fullURL, request, response)
}

// WaitForTenantCapabilities waits until the given set of capabilities have been cached.
func WaitForTenantCapabilities(
	t TestFataler,
	s TestServerInterface,
	tenID roachpb.TenantID,
	targetCaps map[tenantcapabilitiespb.ID]string,
	errPrefix string,
) {
	err := s.TenantController().WaitForTenantCapabilities(context.Background(), tenID, targetCaps, errPrefix)
	if err != nil {
		t.Fatal(err)
	}
}

// parseDefaultTestDRPCOptionFromEnv parses the COCKROACH_TEST_DRPC environment
// variable and returns the corresponding DefaultTestDRPCOption. If the
// environment variable is not set it returns TestDRPCUnset. For invalid value,
// it panic.
func parseDefaultTestDRPCOptionFromEnv() base.DefaultTestDRPCOption {
	if str, present := envutil.EnvString(testDRPCEnabledEnvVar, 0); present {
		switch str {
		case "disabled", "false":
			return base.TestDRPCDisabled
		case "enabled", "true":
			return base.TestDRPCEnabled
		default:
			panic(fmt.Sprintf("invalid value for %s: %s", testDRPCEnabledEnvVar, str))
		}
	}
	return base.TestDRPCUnset
}

// ShouldEnableDRPC determines the final DRPC option based on the input
// option, resolving random choices to a concrete enabled/disabled state.
func ShouldEnableDRPC(
	ctx context.Context, t TestLogger, option base.DefaultTestDRPCOption,
) base.DefaultTestDRPCOption {
	if skip.UnderBench() {
		// Microbenchmarks exercise specific parts of the database and we
		// want to remove any non-deterministic factors that could affect the
		// numbers, so we disable the dRPC option until it becomes the default.
		return base.TestDRPCDisabled
	}
	var logSuffix string

	if option == base.TestDRPCUnset {
		if envOption := parseDefaultTestDRPCOptionFromEnv(); envOption != base.TestDRPCUnset {
			option = envOption
			logSuffix = " (via COCKROACH_TEST_DRPC environment variable)"
		} else if globalDefaultDRPCOptionOverride.isSet {
			option = globalDefaultDRPCOptionOverride.value
			logSuffix = " (via override by TestingGlobalDRPCOption)"
		} else if factoryDefaultDRPC != nil {
			option = *factoryDefaultDRPC
			logSuffix = " (via testserver factory defaults)"
		} else {
			return base.TestDRPCUnset
		}
	} else {
		logSuffix = " (via test explicit setting)"
	}

	enableDRPC := false
	switch option {
	case base.TestDRPCEnabled:
		enableDRPC = true
	case base.TestDRPCEnabledRandomly:
		rng, _ := randutil.NewTestRand()
		enableDRPC = rng.Intn(2) == 0
	case base.TestDRPCUnset:
		return base.TestDRPCUnset
	}

	if enableDRPC {
		if t != nil {
			t.Log("DRPC is enabled" + logSuffix)
		}
		return base.TestDRPCEnabled
	}
	return base.TestDRPCDisabled
}

// SetUnsafeOverride sets an unsafe override for eval.TestingKnobs.UnsafeOverride on
// the given TestingKnobs.
func SetUnsafeOverride(knobs *base.TestingKnobs) {
	var evalTestingKnobs *eval.TestingKnobs
	if knobs.SQLEvalContext != nil {
		evalTestingKnobs = knobs.SQLEvalContext.(*eval.TestingKnobs)
	} else {
		evalTestingKnobs = &eval.TestingKnobs{}
		knobs.SQLEvalContext = evalTestingKnobs
	}

	if evalTestingKnobs.UnsafeOverride == nil {
		v := true
		evalTestingKnobs.UnsafeOverride = func() *bool {
			return &v
		}
	}
}
