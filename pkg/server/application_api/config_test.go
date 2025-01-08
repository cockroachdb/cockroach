// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package application_api_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl"
	// To ensure the streaming replication cluster setting is defined.
	_ "github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAdminAPISettings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// Test setup
	settingName := "some.sensitive.setting"
	settings.RegisterStringSetting(
		settings.SystemVisible,
		settings.InternalKey(settingName),
		"sensitiveSetting",
		"Im sensitive!",
		settings.WithPublic,
		settings.WithReportable(false),
		settings.Sensitive,
	)
	testUsers := map[string]struct {
		userName           string
		consoleOnly        bool
		redactableSettings bool
		grantRole          string
	}{
		// Admin users should be able to se all cluster settings without redaction.
		"admin_user": {userName: "admin_user", redactableSettings: false, grantRole: "ADMIN"},
		// Users with MODIFYCLUSTERSETTING should be able to see all cluster
		// settings without redaction
		"modify_settings_user": {userName: "modify_settings_user", redactableSettings: false, grantRole: "SYSTEM MODIFYCLUSTERSETTING"},
		// Users with VIEWCLUSTERSETTING should be able to see all cluster
		// settings, but sensitive settings are redacted
		"view_settings_user": {userName: "view_settings_user", redactableSettings: true, grantRole: "SYSTEM VIEWCLUSTERSETTING"},
		// Users with VIEWACTIVITY and VIEWACTIVITYREDACTED should only be able to
		// see console specific settings.
		"view_activity_user":          {userName: "view_activity_user", redactableSettings: true, grantRole: "SYSTEM VIEWACTIVITY", consoleOnly: true},
		"view_activity_redacted_user": {userName: "view_activity_redacted_user", redactableSettings: true, grantRole: "SYSTEM VIEWACTIVITYREDACTED", consoleOnly: true},
	}
	ts := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer ts.Stopper().Stop(ctx)
	forSystemTenant := ts.ApplicationLayer().Codec().ForSystemTenant()
	conn := sqlutils.MakeSQLRunner(ts.ApplicationLayer().SQLConn(t))
	settingsLastUpdated := make(map[string]*time.Time)
	rows := conn.Query(t, `SELECT name, "lastUpdated" FROM system.settings`)
	defer rows.Close()
	for rows.Next() {
		var name string
		var lastUpdated *time.Time

		if err := rows.Scan(&name, &lastUpdated); err != nil {
			t.Fatal(err)
		}
		settingsLastUpdated[name] = lastUpdated
	}

	for _, u := range testUsers {
		conn.Exec(t, fmt.Sprintf("CREATE USER IF NOT EXISTS %s", u.userName))
		conn.Exec(t, fmt.Sprintf("GRANT %s TO %s", u.grantRole, u.userName))
	}

	// Runs test cases on each user in testUsers twice, once with redact_sensitive_settings
	// enabled and once with it disabled.
	testutils.RunTrueAndFalse(t, "redact sensitive", func(t *testing.T, redactSensitive bool) {
		if redactSensitive {
			conn.Exec(t, "SET CLUSTER SETTING server.redact_sensitive_settings.enabled=t")
		} else {
			conn.Exec(t, "SET CLUSTER SETTING server.redact_sensitive_settings.enabled=f")
		}
		for _, u := range testUsers {
			t.Run(u.userName, func(t *testing.T) {
				authCtx := authserver.ForwardHTTPAuthInfoToRPCCalls(authserver.ContextWithHTTPAuthInfo(ctx, u.userName, 1), nil)
				resp, err := ts.GetAdminClient(t).Settings(authCtx, &serverpb.SettingsRequest{})
				require.NoError(t, err)
				var keys []settings.InternalKey
				// users with consoleOnly flag are expected to only be able to see
				// console specific settings
				if u.consoleOnly {
					keys = settings.ConsoleKeys()
				} else {
					keys = settings.Keys(forSystemTenant)
					require.Contains(t, resp.KeyValues, settingName)
				}
				for _, internalKey := range keys {
					keyAsString := string(internalKey)
					setting, ok := settings.LookupForLocalAccessByKey(internalKey, forSystemTenant)
					require.True(t, ok)
					settingResponse := resp.KeyValues[keyAsString]
					settingVal := settingResponse.Value
					// If the setting is "sensitive", the setting is not empty, the
					// redact_sensitive_settings is true, and the user being tested
					// is not allowed to see sensitive settings, the value should
					// be redacted
					if settings.TestingIsSensitive(setting) && settingVal != "" && redactSensitive && u.redactableSettings {
						require.Equalf(t, "<redacted>", settingVal, "Expected %s to be <redacted>, but got %s", keyAsString, settingVal)
					} else {
						require.NotEqualf(t, "<redacted>", settingVal, "Expected %s to be %s, but got <redacted>", keyAsString, settingVal)
					}
					require.Equal(t, setting.Description(), settingResponse.Description)
					require.Equal(t, setting.Typ(), settingResponse.Type)
					require.Equal(t, string(setting.Name()), settingResponse.Name)
					if lu, ok := settingsLastUpdated[keyAsString]; ok {
						require.Equal(t, lu.UTC(), *settingResponse.LastUpdated)
					}
				}
			})
		}

		t.Run("no permission", func(t *testing.T) {
			userName := "no_permission"
			conn.Exec(t, fmt.Sprintf("CREATE USER IF NOT EXISTS %s", userName))
			authCtx := authserver.ForwardHTTPAuthInfoToRPCCalls(authserver.ContextWithHTTPAuthInfo(ctx, userName, 1), nil)
			_, err := ts.GetAdminClient(t).Settings(authCtx, &serverpb.SettingsRequest{})
			require.Error(t, err)
			grpcStatus, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, codes.PermissionDenied, grpcStatus.Code())

		})

		t.Run("filter keys", func(t *testing.T) {
			resp, err := ts.GetAdminClient(t).Settings(ctx, &serverpb.SettingsRequest{Keys: []string{settingName}})
			require.NoError(t, err)
			require.Contains(t, resp.KeyValues, settingName)
			require.Len(t, resp.KeyValues, 1)

			resp, err = ts.GetAdminClient(t).Settings(ctx, &serverpb.SettingsRequest{Keys: []string{"random.key"}})
			require.NoError(t, err)
			require.Empty(t, resp.KeyValues)
		})
	})
}

func TestClusterAPI(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails
		// with it enabled. Tracked with #81590.
		DefaultTestTenant: base.TODOTestTenantDisabled,
	})
	defer s.Stopper().Stop(context.Background())

	testutils.RunTrueAndFalse(t, "reportingOn", func(t *testing.T, reportingOn bool) {
		testutils.RunTrueAndFalse(t, "enterpriseOn", func(t *testing.T, enterpriseOn bool) {
			// Override server license check.
			if enterpriseOn {
				defer ccl.TestingEnableEnterprise()()
			}

			if _, err := db.Exec(`SET CLUSTER SETTING diagnostics.reporting.enabled = $1`, reportingOn); err != nil {
				t.Fatal(err)
			}

			// We need to retry, because the cluster ID isn't set until after
			// bootstrapping and because setting a cluster setting isn't necessarily
			// instantaneous.
			//
			// Also note that there's a migration that affects `diagnostics.reporting.enabled`,
			// so manipulating the cluster setting var directly is a bad idea.
			testutils.SucceedsSoon(t, func() error {
				var resp serverpb.ClusterResponse
				if err := srvtestutils.GetAdminJSONProto(s, "cluster", &resp); err != nil {
					return err
				}
				if a, e := resp.ClusterID, s.RPCContext().StorageClusterID.String(); a != e {
					return errors.Errorf("cluster ID %s != expected %s", a, e)
				}
				if a, e := resp.ReportingEnabled, reportingOn; a != e {
					return errors.Errorf("reportingEnabled = %t, wanted %t", a, e)
				}
				if a := resp.EnterpriseEnabled; !a {
					return errors.Errorf("enterpriseEnabled = %t, wanted true", a)
				}
				return nil
			})
		})
	})
}

func TestAdminAPILocations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails
		// with it enabled. Tracked with #81590.
		DefaultTestTenant: base.TODOTestTenantDisabled,
	})
	defer s.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(conn)

	testLocations := []struct {
		localityKey   string
		localityValue string
		latitude      float64
		longitude     float64
	}{
		{"city", "Des Moines", 41.60054, -93.60911},
		{"city", "New York City", 40.71427, -74.00597},
		{"city", "Seattle", 47.60621, -122.33207},
	}
	for _, loc := range testLocations {
		sqlDB.Exec(t,
			`INSERT INTO system.locations ("localityKey", "localityValue", latitude, longitude) VALUES ($1, $2, $3, $4)`,
			loc.localityKey, loc.localityValue, loc.latitude, loc.longitude,
		)
	}
	var res serverpb.LocationsResponse
	if err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, "locations", &res, false /* isAdmin */); err != nil {
		t.Fatal(err)
	}
	for i, loc := range testLocations {
		expLoc := serverpb.LocationsResponse_Location{
			LocalityKey:   loc.localityKey,
			LocalityValue: loc.localityValue,
			Latitude:      loc.latitude,
			Longitude:     loc.longitude,
		}
		if !reflect.DeepEqual(res.Locations[i], expLoc) {
			t.Errorf("%d: expected location %v, but got %v", i, expLoc, res.Locations[i])
		}
	}
}
