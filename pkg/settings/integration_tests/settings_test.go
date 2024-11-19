// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package integration_tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/listenerutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
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
const enum2Key = "testing.enum-2"

var strA = settings.RegisterStringSetting(
	settings.ApplicationLevel, strKey, "desc", "<default>",
	settings.WithValidateString(func(sv *settings.Values, v string) error {
		if len(v) > 15 {
			return errors.Errorf("can't set %s to string longer than 15: %s", strKey, v)
		}
		return nil
	}))
var intA = settings.RegisterIntSetting(
	settings.ApplicationLevel, intKey, "desc", 1,
	settings.WithValidateInt(func(v int64) error {
		if v < 0 {
			return errors.Errorf("can't set %s to a negative value: %d", intKey, v)
		}
		return nil
	}))
var durationA = settings.RegisterDurationSetting(
	settings.ApplicationLevel, durationKey, "desc", time.Minute,
	settings.WithValidateDuration(func(v time.Duration) error {
		if v < 0 {
			return errors.Errorf("can't set %s to a negative duration: %s", durationKey, v)
		}
		return nil
	}))
var byteSizeA = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel, byteSizeKey, "desc", 1024*1024,
)
var enumA = settings.RegisterEnumSetting(
	settings.ApplicationLevel, enumKey, "desc", "foo", map[int64]string{1: "foo", 2: "bar"})

type enumBType uint8

var enumB = settings.RegisterEnumSetting(
	settings.ApplicationLevel, enum2Key, "desc", "foo", map[enumBType]string{1: "foo", 2: "bar"})

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

	db.Exec(t, fmt.Sprintf(setQ, enum2Key, "2"))
	if expected, actual := enumBType(2), enumB.Get(&st.SV); expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
	if expected, actual := "bar", db.QueryStr(t, fmt.Sprintf(showQ, enum2Key))[0][0]; expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}

	db.Exec(t, fmt.Sprintf(setQ, enumKey, "'foo'"))
	if expected, actual := int64(1), enumA.Get(&st.SV); expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
	if expected, actual := "foo", db.QueryStr(t, fmt.Sprintf(showQ, enumKey))[0][0]; expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}

	db.Exec(t, fmt.Sprintf(setQ, enum2Key, "'foo'"))
	if expected, actual := enumBType(1), enumB.Get(&st.SV); expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
	if expected, actual := "foo", db.QueryStr(t, fmt.Sprintf(showQ, enum2Key))[0][0]; expected != actual {
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

// TestSettingsPersistenceEndToEnd checks that a stale entry in the
// local cache on one node properly gets overridden by an update to
// the setting, either on the same node or another.
func TestSettingsPersistenceEndToEnd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "slow test") // test takes >1mn under race

	ctx := context.Background()

	// We're going to restart the test server, but expecting storage to
	// persist. Define a sticky VFS for this purpose.
	stickyVFSRegistry := fs.NewStickyRegistry()
	// We'll also need stable listeners to enable port reuse across restarts.
	lisReg := listenerutil.NewListenerRegistry()
	defer lisReg.Close()

	serverKnobs := &server.TestingKnobs{
		StickyVFSRegistry: stickyVFSRegistry,
	}
	testClusterArgs := base.TestClusterArgs{
		ReusableListenerReg: lisReg,
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {
				StoreSpecs: []base.StoreSpec{{InMemory: true, StickyVFSID: "1"}},
				Knobs:      base.TestingKnobs{Server: serverKnobs},
			},
			1: {
				StoreSpecs: []base.StoreSpec{{InMemory: true, StickyVFSID: "2"}},
				Knobs:      base.TestingKnobs{Server: serverKnobs},
			},
		},
	}

	tc := serverutils.StartCluster(t, 2, testClusterArgs)
	defer tc.Stopper().Stop(ctx)

	db := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	// Make the tests (slightly) faster.
	db.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '10 ms'")
	db.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '10 ms'")
	db.Exec(t, "SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval = '10 ms'")

	// We need a custom value for the cluster setting that's guaranteed
	// to be different from the default. So check that it's not equal to
	// the default always.
	const differentValue = `something`

	setting, _ := settings.LookupForLocalAccessByKey("cluster.label", true)
	s := setting.(*settings.StringSetting)
	st := tc.SystemLayer(0).ClusterSettings()
	require.NotEqual(t, s.Get(&st.SV), differentValue)
	origValue := db.QueryStr(t, `SHOW CLUSTER SETTING cluster.label`)[0][0]

	// Regression test for #70567.
	t.Run("same node", func(t *testing.T) {
		// Customize the setting.
		db.Exec(t, `SET CLUSTER SETTING cluster.label = $1`, differentValue)
		newValue := db.QueryStr(t, `SHOW CLUSTER SETTING cluster.label`)[0][0]
		t.Logf("setting customized")

		// Restart the server; verify the setting customization is preserved.
		// For this we disable the settings watcher, to ensure that
		// only the value loaded by the local persisted cache is used.
		tc.StopServer(0)
		serverKnobs.DisableSettingsWatcher = true
		require.NoError(t, tc.RestartServer(0))
		db = sqlutils.MakeSQLRunner(tc.ServerConn(0))

		t.Logf("restarted server and checking value")
		db.CheckQueryResults(t, `SHOW CLUSTER SETTING cluster.label`, [][]string{{newValue}})

		// Restart the server to make the setting writable again.
		tc.StopServer(0)
		serverKnobs.DisableSettingsWatcher = false
		require.NoError(t, tc.RestartServer(0))
		db = sqlutils.MakeSQLRunner(tc.ServerConn(0))

		// Reset the setting, then check the original value is restored.
		db.Exec(t, `RESET CLUSTER SETTING cluster.label`)
		db.CheckQueryResults(t, `SHOW CLUSTER SETTING cluster.label`, [][]string{{origValue}})
		t.Logf("setting reset")

		// Restart the server; verify the original value is still there.
		tc.StopServer(0)
		require.NoError(t, tc.RestartServer(0))
		db = sqlutils.MakeSQLRunner(tc.ServerConn(0))

		t.Logf("restarted server and checking value")
		db.CheckQueryResults(t, `SHOW CLUSTER SETTING cluster.label`, [][]string{{origValue}})
	})

	// Regression test for #111610.
	t.Run("different node", func(t *testing.T) {
		db2 := sqlutils.MakeSQLRunner(tc.ServerConn(1))

		// Customize the setting on 1st node.
		db.Exec(t, `SET CLUSTER SETTING cluster.label = $1`, differentValue)
		t.Logf("setting customized, waiting for value on 2nd node")

		// Verify the setting has propagated to both nodes.
		testutils.SucceedsSoon(t, func() error {
			res := db2.QueryStr(t, `SHOW CLUSTER SETTING cluster.label`)
			if res[0][0] != differentValue {
				err := errors.Newf("not updated yet: expecting %v, got %v", differentValue, res[0][0])
				t.Log(err)
				return err
			}
			return nil
		})

		t.Logf("stopping 2nd node")
		// Stop the 2nd node; we want to reset the setting while the first
		// node is down and see if the 2nd node picks it up afterwards.
		tc.StopServer(1)

		// Reset the setting to defaults on 1st node.
		db.Exec(t, `RESET CLUSTER SETTING cluster.label`)
		t.Logf("2nd node stopped, resetting on 1st node")

		// Restart the 2nd node; assert the default is eventually
		// observed there.
		require.NoError(t, tc.RestartServer(1))
		t.Logf("2nd node restarted, waiting on reset to become visible")
		db2 = sqlutils.MakeSQLRunner(tc.ServerConn(1))

		testutils.SucceedsSoon(t, func() error {
			res := db2.QueryStr(t, `SHOW CLUSTER SETTING cluster.label`)
			if res[0][0] != origValue {
				return errors.New("not updated yet")
			}
			return nil
		})
	})
}
