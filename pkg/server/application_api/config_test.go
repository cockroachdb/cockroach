// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package application_api_test

import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl"
	// To ensure the streaming replication cluster setting is defined.
	_ "github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/safesql"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func TestAdminAPISettings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

	// Any bool that defaults to true will work here.
	const settingKey = "sql.metrics.statement_details.enabled"
	st := s.ClusterSettings()
	target := settings.ForSystemTenant
	if srv.TenantController().StartedDefaultTestTenant() {
		target = settings.ForVirtualCluster
	}
	allKeys := settings.Keys(target)

	checkSetting := func(t *testing.T, k settings.InternalKey, v serverpb.SettingsResponse_Value) {
		ref, ok := settings.LookupForReportingByKey(k, target)
		if !ok {
			t.Fatalf("%s: not found after initial lookup", k)
		}
		typ := ref.Typ()

		if !settings.TestingIsReportable(ref) {
			if v.Value != "<redacted>" && v.Value != "" {
				t.Errorf("%s: expected redacted value for %v, got %s", k, ref, v.Value)
			}
		} else {
			if ref.String(&st.SV) != v.Value {
				t.Errorf("%s: expected value %v, got %s", k, ref, v.Value)
			}
		}

		if expectedPublic := ref.Visibility() == settings.Public; expectedPublic != v.Public {
			t.Errorf("%s: expected public %v, got %v", k, expectedPublic, v.Public)
		}

		if desc := ref.Description(); desc != v.Description {
			t.Errorf("%s: expected description %s, got %s", k, desc, v.Description)
		}
		if typ != v.Type {
			t.Errorf("%s: expected type %s, got %s", k, typ, v.Type)
		}
		if v.LastUpdated != nil {
			db := sqlutils.MakeSQLRunner(conn)
			q := safesql.NewQuery()
			q.Append(`SELECT name, "lastUpdated" FROM system.settings WHERE name=$`, k)
			rows := db.Query(
				t,
				q.String(),
				q.QueryArguments()...,
			)
			defer rows.Close()
			if rows.Next() == false {
				t.Errorf("missing sql row for %s", k)
			}
		}
	}

	t.Run("all", func(t *testing.T) {
		var resp serverpb.SettingsResponse

		if err := srvtestutils.GetAdminJSONProto(s, "settings", &resp); err != nil {
			t.Fatal(err)
		}

		// Check that all expected keys were returned.
		if len(allKeys) != len(resp.KeyValues) {
			t.Fatalf("expected %d keys, got %d", len(allKeys), len(resp.KeyValues))
		}
		for _, k := range allKeys {
			if _, ok := resp.KeyValues[string(k)]; !ok {
				t.Fatalf("expected key %s not found in response", k)
			}
		}

		// Check that the test key is listed and the values come indeed
		// from the settings package unchanged.
		seenRef := false
		for k, v := range resp.KeyValues {
			if k == settingKey {
				seenRef = true
				if v.Value != "true" {
					t.Errorf("%s: expected true, got %s", k, v.Value)
				}
			}

			checkSetting(t, settings.InternalKey(k), v)
		}

		if !seenRef {
			t.Fatalf("failed to observe test setting %s, got %+v", settingKey, resp.KeyValues)
		}
	})

	t.Run("one-by-one", func(t *testing.T) {
		var resp serverpb.SettingsResponse

		// All the settings keys must be retrievable, and their
		// type and description must match.
		for _, k := range allKeys {
			q := make(url.Values)
			q.Add("keys", string(k))
			url := "settings?" + q.Encode()
			if err := srvtestutils.GetAdminJSONProto(s, url, &resp); err != nil {
				t.Fatalf("%s: %v", k, err)
			}
			if len(resp.KeyValues) != 1 {
				t.Fatalf("%s: expected 1 response, got %d", k, len(resp.KeyValues))
			}
			v, ok := resp.KeyValues[string(k)]
			if !ok {
				t.Fatalf("%s: response does not contain key", k)
			}

			checkSetting(t, k, v)
		}
	})

	t.Run("different-permissions", func(t *testing.T) {
		var resp serverpb.SettingsResponse
		nonAdminUser := apiconstants.TestingUserNameNoAdmin().Normalized()
		var consoleKeys []settings.InternalKey
		for _, k := range settings.ConsoleKeys() {
			if _, ok := settings.LookupForLocalAccessByKey(k, target); !ok {
				continue
			}
			consoleKeys = append(consoleKeys, k)
		}
		sort.Slice(consoleKeys, func(i, j int) bool { return consoleKeys[i] < consoleKeys[j] })

		// Admin should return all cluster settings.
		if err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, "settings", &resp, true); err != nil {
			t.Fatal(err)
		}
		require.True(t, len(resp.KeyValues) == len(allKeys))

		// Admin requesting specific cluster setting should return that cluster setting.
		if err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, "settings?keys=sql.stats.persisted_rows.max",
			&resp, true); err != nil {
			t.Fatal(err)
		}
		require.NotNil(t, resp.KeyValues["sql.stats.persisted_rows.max"])
		require.True(t, len(resp.KeyValues) == 1)

		// Non-admin with no permission should return error message.
		err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, "settings", &resp, false)
		require.Error(t, err, "this operation requires the VIEWCLUSTERSETTING or MODIFYCLUSTERSETTING system privileges")

		// Non-admin with VIEWCLUSTERSETTING permission should return all cluster settings.
		_, err = conn.Exec(fmt.Sprintf("ALTER USER %s VIEWCLUSTERSETTING", nonAdminUser))
		require.NoError(t, err)
		if err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, "settings", &resp, false); err != nil {
			t.Fatal(err)
		}
		require.True(t, len(resp.KeyValues) == len(allKeys))

		// Non-admin with VIEWCLUSTERSETTING permission requesting specific cluster setting should return that cluster setting.
		if err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, "settings?keys=sql.stats.persisted_rows.max",
			&resp, false); err != nil {
			t.Fatal(err)
		}
		require.NotNil(t, resp.KeyValues["sql.stats.persisted_rows.max"])
		require.True(t, len(resp.KeyValues) == 1)

		// Non-admin with VIEWCLUSTERSETTING and VIEWACTIVITY permission should return all cluster settings.
		_, err = conn.Exec(fmt.Sprintf("ALTER USER %s VIEWACTIVITY", nonAdminUser))
		require.NoError(t, err)
		if err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, "settings", &resp, false); err != nil {
			t.Fatal(err)
		}
		require.True(t, len(resp.KeyValues) == len(allKeys))

		// Non-admin with VIEWCLUSTERSETTING and VIEWACTIVITY permission requesting specific cluster setting
		// should return that cluster setting
		if err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, "settings?keys=sql.stats.persisted_rows.max",
			&resp, false); err != nil {
			t.Fatal(err)
		}
		require.NotNil(t, resp.KeyValues["sql.stats.persisted_rows.max"])
		require.True(t, len(resp.KeyValues) == 1)

		// Non-admin with VIEWACTIVITY and not VIEWCLUSTERSETTING should only see console cluster settings.
		_, err = conn.Exec(fmt.Sprintf("ALTER USER %s NOVIEWCLUSTERSETTING", nonAdminUser))
		require.NoError(t, err)
		if err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, "settings", &resp, false); err != nil {
			t.Fatal(err)
		}

		gotKeys := make([]string, 0, len(resp.KeyValues))
		for k := range resp.KeyValues {
			gotKeys = append(gotKeys, k)
		}
		sort.Strings(gotKeys)
		require.Equal(t, len(consoleKeys), len(resp.KeyValues), "found:\n%+v\nexpected:\n%+v", gotKeys, consoleKeys)
		for k := range resp.KeyValues {
			require.True(t, slices.Contains(consoleKeys, settings.InternalKey(k)))
		}

		// Non-admin with VIEWACTIVITY and not VIEWCLUSTERSETTING permission requesting specific cluster setting
		// from console should return that cluster setting
		if err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, "settings?keys=ui.display_timezone",
			&resp, false); err != nil {
			t.Fatal(err)
		}
		require.NotNil(t, resp.KeyValues["ui.display_timezone"])
		require.True(t, len(resp.KeyValues) == 1)

		// Non-admin with VIEWACTIVITY and not VIEWCLUSTERSETTING permission requesting specific cluster setting
		// that is not from console should not return that cluster setting
		if err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, "settings?keys=sql.stats.persisted_rows.max",
			&resp, false); err != nil {
			t.Fatal(err)
		}
		require.True(t, len(resp.KeyValues) == 0)
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
