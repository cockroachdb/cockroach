package sql

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestSettingDefaultOverride verifies that settings can be
// pre-populated for new servers using an environment variable. This
// test is placed here (in package `sql`) instead of the `migrations`
// package to avoid a dependency cycle.
func TestSettingDefaultOverride(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		key            string
		overrideVal    string
		overrideValSQL string
		register       func(k string) settings.Setting
	}{
		{"", "bar", "", func(k string) settings.Setting { return settings.RegisterStringSetting(k, "", "foo") }},
		{"", "false", "", func(k string) settings.Setting { return settings.RegisterBoolSetting(k, "", true) }},
		{"", "456", "", func(k string) settings.Setting { return settings.RegisterIntSetting(k, "", 123) }},
		{"", "2s", "", func(k string) settings.Setting { return settings.RegisterDurationSetting(k, "", time.Second) }},
		{"", "4.56E+00", "4.56", func(k string) settings.Setting { return settings.RegisterFloatSetting(k, "", 1.23) }},
	}

	// Initialize the test data and load the env vars.
	var svars []settings.Setting
	for i, test := range testData {
		key := fmt.Sprintf("testing.override.%c", 'a'+i)
		testData[i].key = key
		os.Setenv("COCKROACH_SETTINGS_DEFAULT_OVERRIDE_"+key, test.overrideVal)
		svars = append(svars, test.register(key))
	}

	// Sanity check: prior to server init, the settings are not yet initialized.
	for i, svar := range svars {
		if svar.String() == testData[i].overrideVal {
			t.Fatalf("override active too early for %s", testData[i].key)
		}
	}

	// Start the server; this loads the overrides.
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	// Check the overrides are active now.
	for i, test := range testData {
		svar := svars[i]
		if svar.String() != test.overrideVal {
			t.Fatalf("override not active: expected %s, got %s", test.overrideVal, svar.String())
		}

		// Check they are also active via SQL.
		func() {
			rows, err := db.Query("SHOW CLUSTER SETTING " + test.key)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()

			n := 0
			for rows.Next() {
				if n >= 1 {
					t.Fatalf("too many rows")
				}
				var s string
				err := rows.Scan(&s)
				if err != nil {
					t.Fatal(err)
				}

				expected := test.overrideValSQL
				if expected == "" {
					expected = test.overrideVal
				}

				if s != expected {
					t.Fatalf("override not visible in SHOW: expected %s, got %s", expected, s)
				}
				n++
			}
			if n == 0 {
				t.Fatalf("too few rows")
			}
		}()
	}
}
