// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package integration_tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestSettingDefaultPropagationReadOnly1 runs one of 4 invocations of
// `RunSettingDefaultPropagationTest`. The test is split 4-ways to
// avoid timeouts in CI and increased test parallelism.
func TestSettingDefaultPropagationReadOnly1(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runSettingDefaultPropagationTest(t, roS, true)
}

// TestSettingDefaultPropagationReadOnly2 runs one of 4 invocations of
// `RunSettingDefaultPropagationTest`. The test is split 4-ways to
// avoid timeouts in CI and increased test parallelism.
func TestSettingDefaultPropagationReadOnly2(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runSettingDefaultPropagationTest(t, roS, false)
}

// TestSettingDefaultPropagationReadWrite1 runs one of 4 invocations
// of `RunSettingDefaultPropagationTest`. The test is split 4-ways to
// avoid timeouts in CI and increased test parallelism.
func TestSettingDefaultPropagationReadWrite1(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runSettingDefaultPropagationTest(t, rwS, true)
}

// TestSettingDefaultPropagationReadWrite2 runs one of 4 invocations
// of `RunSettingDefaultPropagationTest`. The test is split 4-ways to
// avoid timeouts in CI and increased test parallelism.
func TestSettingDefaultPropagationReadWrite2(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runSettingDefaultPropagationTest(t, rwS, false)
}

var roS = settings.RegisterStringSetting(settings.SystemVisible, "system.visible", "desc", "initial")
var rwS = settings.RegisterStringSetting(settings.ApplicationLevel, "application.level", "desc", "initial")

// runSettingDefaultPropagationTest is a test helper.
func runSettingDefaultPropagationTest(
	t *testing.T, setting *settings.StringSetting, setSystemBefore bool,
) {
	defer log.Scope(t).Close(t)

	// This test currently takes >1 minute.
	//
	// TODO(multi-tenant): make it faster. Currently most of the
	// overhead is in tenant service initialization and shutdown
	// (strangely, especially shutdown).
	skip.UnderShort(t)

	skip.UnderRace(t, "slow test")
	skip.UnderStress(t, "slow test")

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	sysDB := sqlutils.MakeSQLRunner(s.SystemLayer().SQLConn(t))
	sysDB.Exec(t, "SELECT crdb_internal.create_tenant($1, 'test')", serverutils.TestTenantID().ToUint64())
	// Speed up the tests.
	sysDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '10ms'")

	expectation := func(setting settings.Setting, sysOverride, tenantOverride, tenantAllOverride string) string {
		if tenantOverride != "" {
			// ALTER TENANT xxx SET CLUSTER SETTING -> highest precedence.
			return tenantOverride
		}
		if tenantAllOverride != "" {
			// ALTER TENANT ALL SET CLUSTER SETTING, override is used
			// only if there is no tenant-specific override.
			return tenantAllOverride
		}
		// No tenant override. What is the default?
		// For SystemVisible, if there is a custom value in the
		// system interface, that becomes the default.
		if setting.Class() == settings.SystemVisible && sysOverride != "" {
			return sysOverride
		}
		// Otherwise, fall back to the default.
		return "initial"
	}

	key := setting.InternalKey()
	testutils.RunTrueAndFalse(t, "set-override-before", func(t *testing.T, setOverrideBefore bool) {
		testutils.RunTrueAndFalse(t, "set-override-all-before", func(t *testing.T, setOverrideAllBefore bool) {
			testutils.RunTrueAndFalse(t, "set-system-after", func(t *testing.T, setSystemAfter bool) {
				testutils.RunTrueAndFalse(t, "set-override-after", func(t *testing.T, setOverrideAfter bool) {
					testutils.RunTrueAndFalse(t, "set-override-all-after", func(t *testing.T, setOverrideAllAfter bool) {
						var sysOverride string
						if setSystemBefore {
							t.Logf("before start: system custom value via SET CLUSTER SETTING")
							sysOverride = "before-sys"
							sysDB.Exec(t, fmt.Sprintf("SET CLUSTER SETTING %s = '%s'", key, sysOverride))
						} else {
							sysDB.Exec(t, fmt.Sprintf("RESET CLUSTER SETTING %s", key))
						}
						var tenantAllOverride string
						if setOverrideAllBefore {
							t.Logf("before start: override via ALTER VIRTUAL CLUSTER ALL SET CLUSTER SETTING")
							tenantAllOverride = "before-all"
							sysDB.Exec(t, fmt.Sprintf("ALTER VIRTUAL CLUSTER ALL SET CLUSTER SETTING %s = '%s'", key, tenantAllOverride))
						} else {
							sysDB.Exec(t, fmt.Sprintf("ALTER VIRTUAL CLUSTER ALL RESET CLUSTER SETTING %s", key))
						}
						var tenantOverride string
						if setOverrideBefore {
							t.Logf("before start: override via ALTER VIRTUAL CLUSTER test SET CLUSTER SETTING")
							tenantOverride = "before-specific"
							sysDB.Exec(t, fmt.Sprintf("ALTER VIRTUAL CLUSTER test SET CLUSTER SETTING %s = '%s'", key, tenantOverride))
						} else {
							sysDB.Exec(t, fmt.Sprintf("ALTER VIRTUAL CLUSTER test RESET CLUSTER SETTING %s", key))
						}

						t.Logf("starting secondary tenant service")
						ten, err := s.TenantController().StartTenant(ctx, base.TestTenantArgs{
							TenantID: serverutils.TestTenantID(),
						})
						require.NoError(t, err)
						defer ten.AppStopper().Stop(ctx)

						expected := expectation(setting, sysOverride, tenantOverride, tenantAllOverride)
						t.Logf("expecting setting set to %q", expected)
						val := setting.Get(&ten.ClusterSettings().SV)
						t.Logf("setting currenly at %q", val)
						if val != expected {
							t.Errorf("expected %q, got %q", expected, val)
						}

						t.Logf("changing configuration")
						if setSystemAfter {
							t.Logf("after start: setting system custom value via SET CLUSTER SETTING")
							sysOverride = "after-sys"
							sysDB.Exec(t, fmt.Sprintf("SET CLUSTER SETTING %s = '%s'", key, sysOverride))
						} else {
							t.Logf("after start: resetting system custom value with RESET CLUSTER SETTING")
							sysOverride = ""
							sysDB.Exec(t, fmt.Sprintf("RESET CLUSTER SETTING %s", key))
						}
						if setOverrideAllAfter {
							t.Logf("after start: override via ALTER VIRTUAL CLUSTER ALL SET CLUSTER SETTING")
							tenantAllOverride = "after-all"
							sysDB.Exec(t, fmt.Sprintf("ALTER VIRTUAL CLUSTER ALL SET CLUSTER SETTING %s = '%s'", key, tenantAllOverride))
						} else {
							t.Logf("after start: resetting system custom value with ALTER VIRTUAL CLUSTER ALL RESET CLUSTER SETTING")
							tenantAllOverride = ""
							sysDB.Exec(t, fmt.Sprintf("ALTER VIRTUAL CLUSTER ALL RESET CLUSTER SETTING %s", key))
						}
						if setOverrideAfter {
							t.Logf("after start: override via ALTER VIRTUAL CLUSTER test SET CLUSTER SETTING")
							tenantOverride = "after-specific"
							sysDB.Exec(t, fmt.Sprintf("ALTER VIRTUAL CLUSTER test SET CLUSTER SETTING %s = '%s'", key, tenantOverride))
						} else {
							t.Logf("after start: resetting system custom value with ALTER VIRTUAL CLUSTER test RESET CLUSTER SETTING")
							tenantOverride = ""
							sysDB.Exec(t, fmt.Sprintf("ALTER VIRTUAL CLUSTER test RESET CLUSTER SETTING %s", key))
						}

						expected = expectation(setting, sysOverride, tenantOverride, tenantAllOverride)
						t.Logf("expecting setting set to %q", expected)
						testutils.SucceedsSoon(t, func() error {
							val := setting.Get(&ten.ClusterSettings().SV)
							t.Logf("setting currenly at %q", val)
							if val != expected {
								return errors.Newf("expected %q, got %q", expected, val)
							}
							return nil
						})
					})
				})
			})
		})
	})
}
