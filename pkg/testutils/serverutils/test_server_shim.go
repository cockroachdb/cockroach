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
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
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
			t.Logf("cluster virtualization disabled due to issue: #%d (expected label: %s)", issueNum, label)
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
		return baseArg
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
			t.Log(defaultTestTenantMessage(decision.SharedProcessMode()) + "\n(override via COCKROACH_TEST_TENANT)")
		}
		return decision
	}

	if globalDefaultSelectionOverride.isSet {
		override := globalDefaultSelectionOverride.value
		if override.TestTenantNoDecisionMade() {
			panic("programming error: global override does not contain a final decision")
		}
		if override.TestTenantAlwaysDisabled() {
			if issueNum, label := override.IssueRef(); issueNum != 0 {
				t.Logf("cluster virtualization disabled in global scope due to issue: #%d (expected label: %s)", issueNum, label)
			}
		} else {
			t.Log(defaultTestTenantMessage(shared) + "\n(override via TestingSetDefaultTenantSelectionOverride)")
		}
		return override
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

// InitTestServerFactory should be called once to provide the implementation
// of the service. It will be called from a xx_test package that can import the
// server package.
func InitTestServerFactory(impl TestServerFactory) {
	srvFactoryImpl = impl
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
	allowAdditionalTenants := params.DefaultTestTenant.AllowAdditionalTenants()
	// Update the flags with the actual decision as to whether we should
	// start the service for a default test tenant.
	params.DefaultTestTenant = ShouldStartDefaultTestTenant(t, params.DefaultTestTenant)

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

	ctx := context.Background()

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

	srv, err := srvFactoryImpl.New(params)
	if err != nil {
		return nil, err
	}
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
	log.Infof(context.Background(), "test retrieving protobuf over HTTP: %s", fullURL)
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
	log.Infof(context.Background(), "test retrieving protobuf over HTTP: %s", fullURL)
	log.Infof(context.Background(), "set HTTP client timeout to: %s", httpClient.Timeout)
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
	log.Infof(context.Background(), "test retrieving protobuf over HTTP: %s", fullURL)
	return httputil.PostJSON(httpClient, fullURL, request, response)
}

// WaitForTenantCapabilities waits until the given set of capabilities have been cached.
func WaitForTenantCapabilities(
	t TestFataler,
	s TestServerInterface,
	tenID roachpb.TenantID,
	targetCaps map[tenantcapabilities.ID]string,
	errPrefix string,
) {
	err := s.TenantController().WaitForTenantCapabilities(context.Background(), tenID, targetCaps, errPrefix)
	if err != nil {
		t.Fatal(err)
	}
}
