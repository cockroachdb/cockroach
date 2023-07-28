// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
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
	"net/url"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

// DefaultTestTenantMessage is a message that is printed when a test is run
// with the default test tenant. This is useful for debugging test failures.
const DefaultTestTenantMessage = `
Test server was configured to route SQL queries to a secondary tenant (virtual cluster).
If you are only seeing a test failure when this message appears, there may be a problem
specific to cluster virtualization or multi-tenancy.

To investigate, consider using "COCKROACH_TEST_TENANT=true" to force-enable just
the secondary tenant in all runs (or, alternatively, "false" to force-disable), or use
"COCKROACH_INTERNAL_DISABLE_METAMORPHIC_TESTING=false" to disable all random test variables altogether.`

var PreventStartTenantError = errors.New("attempting to manually start a server for a secondary tenant while " +
	"DefaultTestTenant is set to TestTenantProbabilisticOnly")

// ShouldStartDefaultTestTenant determines whether a default test tenant
// should be started for test servers or clusters, to serve SQL traffic by
// default.
// This can be overridden either via the build tag `metamorphic_disable`
// or just for test tenants via COCKROACH_TEST_TENANT.
func ShouldStartDefaultTestTenant(t TestLogger, serverArgs base.TestServerArgs) bool {
	// Explicit cases for enabling or disabling the default test tenant.
	if serverArgs.DefaultTestTenant.TestTenantAlwaysEnabled() {
		return true
	}
	if serverArgs.DefaultTestTenant.TestTenantAlwaysDisabled() {
		if issueNum, label := serverArgs.DefaultTestTenant.IssueRef(); issueNum != 0 {
			t.Logf("cluster virtualization disabled due to issue: #%d (expected label: %s)", issueNum, label)
		}
		return false
	}

	if skip.UnderBench() {
		// Until #83461 is resolved, we want to make sure that we don't use the
		// multi-tenant setup so that the comparison against old single-tenant
		// SHAs in the benchmarks is fair.
		return false
	}

	// Obey the env override if present.
	if str, present := envutil.EnvString("COCKROACH_TEST_TENANT", 0); present {
		v, err := strconv.ParseBool(str)
		if err != nil {
			panic(err)
		}
		return v
	}

	// Note: we ask the metamorphic framework for a "disable" value, instead
	// of an "enable" value, because it probabilistically returns its default value
	// more often than not and that is what we want.
	enabled := !util.ConstantWithMetamorphicTestBoolWithoutLogging("disable-test-tenant", false)
	if enabled && t != nil {
		t.Log(DefaultTestTenantMessage)
	}
	return enabled
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
	// Determine if we should probabilistically start a test tenant
	// for this server.
	startDefaultSQLServer := ShouldStartDefaultTestTenant(t, params)
	if !startDefaultSQLServer {
		// If we're told not to start a test tenant, set the
		// disable flag explicitly.
		//
		// TODO(#76378): review the definition of params.DefaultTestTenant
		// so we do not need this weird sentinel value.
		params.DefaultTestTenant = base.InternalNonDefaultDecision
	}

	s, err := NewServer(params)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	if err := s.Start(ctx); err != nil {
		s.Stopper().Stop(ctx)
		return nil, err
	}

	if s.StartedDefaultTestTenant() && t != nil {
		t.Log(DefaultTestTenantMessage)
	}

	if !allowAdditionalTenants {
		s.DisableStartTenant(PreventStartTenantError)
	}

	// Now that we have started the server on the bootstrap version, let us run
	// the migrations up to the overridden BinaryVersion.
	if v := s.BinaryVersionOverride(); v != (roachpb.Version{}) {
		for _, layer := range []ApplicationLayerInterface{s.SystemLayer(), s.ApplicationLayer()} {
			ie := layer.InternalExecutor().(isql.Executor)
			if _, err := ie.Exec(ctx, "set-version", nil, /* kv.Txn */
				`SET CLUSTER SETTING version = $1`, v.String()); err != nil {
				s.Stopper().Stop(ctx)
				return nil, err
			}
		}
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
	goDB := s.ApplicationLayer().SQLConn(t, params.UseDatabase)
	kvDB := s.ApplicationLayer().DB()
	return s, goDB, kvDB
}

// NewServer creates a test server.
func NewServer(params base.TestServerArgs) (TestServerInterface, error) {
	if srvFactoryImpl == nil {
		return nil, errors.AssertionFailedf("TestServerFactory not initialized. One needs to be injected " +
			"from the package's TestMain()")
	}

	srv, err := srvFactoryImpl.New(params)
	if err != nil {
		return nil, err
	}
	return srv.(TestServerInterface), nil
}

// OpenDBConnE is like OpenDBConn, but returns an error.
// Note: consider using the .SQLConnE() method on the test server instead.
func OpenDBConnE(
	sqlAddr string, useDatabase string, insecure bool, stopper *stop.Stopper,
) (*gosql.DB, error) {
	pgURL, cleanupGoDB, err := sqlutils.PGUrlE(
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
	tenant, err := ts.StartTenant(context.Background(), params)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	goDB := tenant.SQLConn(t, params.UseDatabase)
	return tenant, goDB
}

func StartSharedProcessTenant(
	t TestFataler, ts TestServerInterface, params base.TestSharedProcessTenantArgs,
) (ApplicationLayerInterface, *gosql.DB) {
	tenant, goDB, err := ts.StartSharedProcessTenant(context.Background(), params)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	return tenant, goDB
}

// TestTenantID returns a roachpb.TenantID that can be used when
// starting a test Tenant. The returned tenant IDs match those built
// into the test certificates.
func TestTenantID() roachpb.TenantID {
	return roachpb.MustMakeTenantID(security.EmbeddedTenantIDs()[0])
}

// TestTenantID2 returns another roachpb.TenantID that can be used when
// starting a test Tenant. The returned tenant IDs match those built
// into the test certificates.
func TestTenantID2() roachpb.TenantID {
	return roachpb.MustMakeTenantID(security.EmbeddedTenantIDs()[1])
}

// TestTenantID3 returns another roachpb.TenantID that can be used when
// starting a test Tenant. The returned tenant IDs match those built
// into the test certificates.
func TestTenantID3() roachpb.TenantID {
	return roachpb.MustMakeTenantID(security.EmbeddedTenantIDs()[2])
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
	u := ts.AdminURL().String()
	fullURL := u + path
	log.Infof(context.Background(), "test retrieving protobuf over HTTP: %s", fullURL)
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
	if errPrefix != "" && !strings.HasSuffix(errPrefix, ": ") {
		errPrefix += ": "
	}
	testutils.SucceedsSoon(t, func() error {
		if tenID.IsSystem() {
			return nil
		}
		if len(targetCaps) == 0 {
			return nil
		}

		missingCapabilityError := func(capID tenantcapabilities.ID) error {
			return errors.Newf("%stenant %s cap %q not at expected value", errPrefix, tenID, capID)
		}
		capabilities, found := s.TenantCapabilitiesReader().GetCapabilities(tenID)
		if !found {
			return errors.Newf("%scapabilities not ready for tenant %v", errPrefix, tenID)
		}

		for capID, expectedValue := range targetCaps {
			curVal := tenantcapabilities.MustGetValueByID(capabilities, capID).String()
			if curVal != expectedValue {
				return missingCapabilityError(capID)
			}
		}

		return nil
	})
}
