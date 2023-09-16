// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package integration_tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

const strKey = "testing.str"
const intKey = "testing.int"
const durationKey = "testing.duration"
const byteSizeKey = "testing.bytesize"
const enumKey = "testing.enum"

var strA = settings.RegisterStringSetting(
	settings.TenantWritable, strKey, "desc", "<default>",
	settings.WithValidateString(func(sv *settings.Values, v string) error {
		if len(v) > 15 {
			return errors.Errorf("can't set %s to string longer than 15: %s", strKey, v)
		}
		return nil
	}))
var intA = settings.RegisterIntSetting(
	settings.TenantWritable, intKey, "desc", 1,
	settings.WithValidateInt(func(v int64) error {
		if v < 0 {
			return errors.Errorf("can't set %s to a negative value: %d", intKey, v)
		}
		return nil
	}))
var durationA = settings.RegisterDurationSetting(
	settings.TenantWritable, durationKey, "desc", time.Minute,
	settings.WithValidateDuration(func(v time.Duration) error {
		if v < 0 {
			return errors.Errorf("can't set %s to a negative duration: %s", durationKey, v)
		}
		return nil
	}))
var byteSizeA = settings.RegisterByteSizeSetting(
	settings.TenantWritable, byteSizeKey, "desc", 1024*1024,
)
var enumA = settings.RegisterEnumSetting(
	settings.TenantWritable, enumKey, "desc", "foo", map[int64]string{1: "foo", 2: "bar"})

func TestSettingsRefresh(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, rawDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	st := s.ApplicationLayer().ClusterSettings()
	db := sqlutils.MakeSQLRunner(rawDB)

	insertQ := `UPSERT INTO system.settings (name, value, "lastUpdated", "valueType")
		VALUES ($1, $2, now(), $3)`
	deleteQ := "DELETE FROM system.settings WHERE name = $1"

	if expected, actual := "<default>", strA.Get(&st.SV); expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
	if expected, actual := int64(1), intA.Get(&st.SV); expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}

	// Inserting a new setting is reflected in cache.
	db.Exec(t, insertQ, strKey, "foo", "s")
	db.Exec(t, insertQ, intKey, settings.EncodeInt(2), "i")
	// Wait until we observe the gossip-driven update propagating to cache.
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := "foo", strA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := int64(2), intA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// Setting to empty also works.
	db.Exec(t, insertQ, strKey, "", "s")
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := "", strA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// An unknown value doesn't block updates to a known one.
	db.Exec(t, insertQ, "dne", "???", "s")
	db.Exec(t, insertQ, strKey, "qux", "s")

	testutils.SucceedsSoon(t, func() error {
		if expected, actual := "qux", strA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := int64(2), intA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// A malformed value doesn't revert previous set or block other changes.
	db.Exec(t, deleteQ, "dne")
	db.Exec(t, insertQ, intKey, "invalid", "i")
	db.Exec(t, insertQ, strKey, "after-invalid", "s")

	testutils.SucceedsSoon(t, func() error {
		if expected, actual := int64(2), intA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "after-invalid", strA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// A mis-typed value doesn't revert a previous set or block other changes.
	db.Exec(t, insertQ, intKey, settings.EncodeInt(7), "b")
	db.Exec(t, insertQ, strKey, "after-mistype", "s")

	testutils.SucceedsSoon(t, func() error {
		if expected, actual := int64(2), intA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "after-mistype", strA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// An invalid value doesn't revert a previous set or block other changes.
	prevStrA := strA.Get(&st.SV)
	prevIntA := intA.Get(&st.SV)
	prevDurationA := durationA.Get(&st.SV)
	prevByteSizeA := byteSizeA.Get(&st.SV)
	db.Exec(t, insertQ, strKey, "this is too big for this setting", "s")
	db.Exec(t, insertQ, intKey, settings.EncodeInt(-1), "i")
	db.Exec(t, insertQ, durationKey, settings.EncodeDuration(-time.Minute), "d")
	db.Exec(t, insertQ, byteSizeKey, settings.EncodeInt(-1), "z")

	testutils.SucceedsSoon(t, func() error {
		if expected, actual := prevStrA, strA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := prevIntA, intA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := prevDurationA, durationA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := prevByteSizeA, byteSizeA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// Deleting a value reverts to default.
	db.Exec(t, deleteQ, strKey)
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := "<default>", strA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})
}

func TestSettingsSetAndShow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, rawDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	st := s.ApplicationLayer().ClusterSettings()
	db := sqlutils.MakeSQLRunner(rawDB)

	// TODO(dt): add placeholder support to SET and SHOW.
	setQ := `SET CLUSTER SETTING "%s" = %s`
	showQ := `SHOW CLUSTER SETTING "%s"`

	db.Exec(t, fmt.Sprintf(setQ, strKey, "'via-set'"))
	if expected, actual := "via-set", db.QueryStr(t, fmt.Sprintf(showQ, strKey))[0][0]; expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}

	db.Exec(t, fmt.Sprintf(setQ, intKey, "5"))
	if expected, actual := "5", db.QueryStr(t, fmt.Sprintf(showQ, intKey))[0][0]; expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}

	db.Exec(t, fmt.Sprintf(setQ, durationKey, "'2h'"))
	if expected, actual := time.Hour*2, durationA.Get(&st.SV); expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
	if expected, actual := "02:00:00", db.QueryStr(t, fmt.Sprintf(showQ, durationKey))[0][0]; expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}

	db.Exec(t, fmt.Sprintf(setQ, byteSizeKey, "'1500MB'"))
	if expected, actual := int64(1500000000), byteSizeA.Get(&st.SV); expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
	if expected, actual := "1.4 GiB", db.QueryStr(t, fmt.Sprintf(showQ, byteSizeKey))[0][0]; expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}

	db.Exec(t, fmt.Sprintf(setQ, byteSizeKey, "'1450MB'"))
	if expected, actual := "1.4 GiB", db.QueryStr(t, fmt.Sprintf(showQ, byteSizeKey))[0][0]; expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}

	db.ExpectErr(t, `could not parse "a-str" as type int`, fmt.Sprintf(setQ, intKey, "'a-str'"))

	db.Exec(t, fmt.Sprintf(setQ, enumKey, "2"))
	if expected, actual := int64(2), enumA.Get(&st.SV); expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
	if expected, actual := "bar", db.QueryStr(t, fmt.Sprintf(showQ, enumKey))[0][0]; expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}

	db.Exec(t, fmt.Sprintf(setQ, enumKey, "'foo'"))
	if expected, actual := int64(1), enumA.Get(&st.SV); expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
	if expected, actual := "foo", db.QueryStr(t, fmt.Sprintf(showQ, enumKey))[0][0]; expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}

	db.ExpectErr(
		t, `invalid string value 'unknown' for enum setting`,
		fmt.Sprintf(setQ, enumKey, "'unknown'"),
	)

	db.ExpectErr(t, `invalid integer value '7' for enum setting`, fmt.Sprintf(setQ, enumKey, "7"))
}

func TestSettingsShowAll(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, rawDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	db := sqlutils.MakeSQLRunner(rawDB)

	rows := db.QueryStr(t, "SHOW ALL CLUSTER SETTINGS")
	if len(rows) < 2 {
		t.Fatalf("show all returned too few rows (%d)", len(rows))
	}
	const expColumns = 7
	if len(rows[0]) != expColumns {
		t.Fatalf("show all must return %d columns, found %d", expColumns, len(rows[0]))
	}
	hasIntKey := false
	hasStrKey := false
	for _, row := range rows {
		switch row[0] {
		case strKey:
			hasStrKey = true
		case intKey:
			hasIntKey = true
		}
	}
	if !hasIntKey || !hasStrKey {
		t.Fatalf("show all did not find the test keys: %q", rows)
	}
}

var roS = settings.RegisterStringSetting(settings.TenantReadOnly, "tenant.read.only", "desc", "initial")
var rwS = settings.RegisterStringSetting(settings.TenantWritable, "tenant.writable", "desc", "initial")

// TestSettingDefaultPropagationReadOnly checks that setting defaults
// propagate correctly through the tenant boundary.
func TestSettingDefaultPropagationReadOnly(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runSettingDefaultPropagationTest(t, roS)
}

// TestSettingDefaultPropagationReadWrite checks that setting defaults
// propagate correctly through the tenant boundary.
func TestSettingDefaultPropagationReadWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runSettingDefaultPropagationTest(t, rwS)
}

func runSettingDefaultPropagationTest(t *testing.T, setting *settings.StringSetting) {
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	sysDB := sqlutils.MakeSQLRunner(s.SystemLayer().SQLConn(t, ""))
	sysDB.Exec(t, "SELECT crdb_internal.create_tenant($1, 'test')", serverutils.TestTenantID().ToUint64())

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
		// For TenantReadOnly, if there is a custom value in the
		// system interface, that becomes the default.
		if setting.Class() == settings.TenantReadOnly && sysOverride != "" {
			return sysOverride
		}
		// Otherwise, fall back to the default.
		return "initial"
	}

	key := setting.InternalKey()
	testutils.RunTrueAndFalse(t, "set-system-before", func(t *testing.T, setSystemBefore bool) {
		testutils.RunTrueAndFalse(t, "set-system-after", func(t *testing.T, setSystemAfter bool) {
			testutils.RunTrueAndFalse(t, "set-override-before", func(t *testing.T, setOverrideBefore bool) {
				testutils.RunTrueAndFalse(t, "set-override-after", func(t *testing.T, setOverrideAfter bool) {
					testutils.RunTrueAndFalse(t, "set-override-all-before", func(t *testing.T, setOverrideAllBefore bool) {
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
	})
}
