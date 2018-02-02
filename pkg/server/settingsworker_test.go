// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package server_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

const strKey = "testing.str"
const intKey = "testing.int"
const durationKey = "testing.duration"
const byteSizeKey = "testing.bytesize"
const enumKey = "testing.enum"

var strA = settings.RegisterValidatedStringSetting(strKey, "", "<default>", func(v string) error {
	if len(v) > 15 {
		return errors.Errorf("can't set %s to string longer than 15: %s", strKey, v)
	}
	return nil
})
var intA = settings.RegisterValidatedIntSetting(intKey, "", 1, func(v int64) error {
	if v < 0 {
		return errors.Errorf("can't set %s to a negative value: %d", intKey, v)
	}
	return nil

})
var durationA = settings.RegisterValidatedDurationSetting(durationKey, "", time.Minute, func(v time.Duration) error {
	if v < 0 {
		return errors.Errorf("can't set %s to a negative duration: %s", durationKey, v)
	}
	return nil
})
var byteSizeA = settings.RegisterValidatedByteSizeSetting(byteSizeKey, "", 1024*1024, func(v int64) error {
	if v < 0 {
		return errors.Errorf("can't set %s to a negative value: %d", byteSizeKey, v)
	}
	return nil
})
var enumA = settings.RegisterEnumSetting(enumKey, "", "foo", map[int64]string{1: "foo", 2: "bar"})

func TestSettingsRefresh(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Set up some additional cluster settings to play around with. Note that we
	// need to do this before starting the server, or there will be data races.
	st := cluster.MakeTestingClusterSettings()
	s, rawDB, _ := serverutils.StartServer(t, base.TestServerArgs{Settings: st})
	defer s.Stopper().Stop(context.TODO())

	db := sqlutils.MakeSQLRunner(rawDB)

	insertQ := `UPSERT INTO system.public.settings (name, value, "lastUpdated", "valueType")
		VALUES ($1, $2, NOW(), $3)`
	deleteQ := "DELETE FROM system.public.settings WHERE name = $1"

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
	// Set up some additional cluster settings to play around with. Note that we
	// need to do this before starting the server, or there will be data races.
	st := cluster.MakeTestingClusterSettings()
	s, rawDB, _ := serverutils.StartServer(t, base.TestServerArgs{Settings: st})
	defer s.Stopper().Stop(context.TODO())

	db := sqlutils.MakeSQLRunner(rawDB)

	// TODO(dt): add placeholder support to SET and SHOW.
	setQ := `SET CLUSTER SETTING "%s" = %s`
	showQ := `SHOW CLUSTER SETTING "%s"`

	db.Exec(t, fmt.Sprintf(setQ, strKey, "'via-set'"))
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := "via-set", db.QueryStr(t, fmt.Sprintf(showQ, strKey))[0][0]; expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	db.Exec(t, fmt.Sprintf(setQ, intKey, "5"))
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := "5", db.QueryStr(t, fmt.Sprintf(showQ, intKey))[0][0]; expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	db.Exec(t, fmt.Sprintf(setQ, durationKey, "'2h'"))
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := time.Hour*2, durationA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "2h", db.QueryStr(t, fmt.Sprintf(showQ, durationKey))[0][0]; expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	db.Exec(t, fmt.Sprintf(setQ, byteSizeKey, "'1500MB'"))
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := int64(1500000000), byteSizeA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "1.4 GiB", db.QueryStr(t, fmt.Sprintf(showQ, byteSizeKey))[0][0]; expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	db.Exec(t, fmt.Sprintf(setQ, byteSizeKey, "'1450MB'"))
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := "1.4 GiB", db.QueryStr(t, fmt.Sprintf(showQ, byteSizeKey))[0][0]; expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	if _, err := db.DB.Exec(fmt.Sprintf(setQ, intKey, "'a-str'")); !testutils.IsError(
		err, `could not parse "a-str" as type int`,
	) {
		t.Fatal(err)
	}

	db.Exec(t, fmt.Sprintf(setQ, enumKey, "2"))
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := int64(2), enumA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "2", db.QueryStr(t, fmt.Sprintf(showQ, enumKey))[0][0]; expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	db.Exec(t, fmt.Sprintf(setQ, enumKey, "'foo'"))
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := int64(1), enumA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "1", db.QueryStr(t, fmt.Sprintf(showQ, enumKey))[0][0]; expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	if _, err := db.DB.Exec(fmt.Sprintf(setQ, enumKey, "'unknown'")); !testutils.IsError(err,
		`invalid string value 'unknown' for enum setting`,
	) {
		t.Fatal(err)
	}

	if _, err := db.DB.Exec(fmt.Sprintf(setQ, enumKey, "7")); !testutils.IsError(err,
		`invalid integer value '7' for enum setting`,
	) {
		t.Fatal(err)
	}

	db.Exec(t, `CREATE USER testuser`)
	pgURL, cleanupFunc := sqlutils.PGUrl(t, s.ServingAddr(), t.Name(), url.User("testuser"))
	defer cleanupFunc()
	testuser, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer testuser.Close()

	if _, err := testuser.Exec(`SET CLUSTER SETTING foo = 'bar'`); !testutils.IsError(err,
		`only superusers are allowed to SET CLUSTER SETTING`,
	) {
		t.Fatal(err)
	}
	if _, err := testuser.Exec(`SHOW CLUSTER SETTING foo`); !testutils.IsError(err,
		`only superusers are allowed to SHOW CLUSTER SETTINGS`,
	) {
		t.Fatal(err)
	}
	if _, err := testuser.Exec(`SHOW ALL CLUSTER SETTINGS`); !testutils.IsError(err,
		`only superusers are allowed to SHOW CLUSTER SETTINGS`,
	) {
		t.Fatal(err)
	}
	if _, err := testuser.Exec(`SELECT * FROM crdb_internal.cluster_settings`); !testutils.IsError(err,
		`only superusers are allowed to read crdb_internal.cluster_settings`,
	) {
		t.Fatal(err)
	}
}

func TestSettingsShowAll(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Set up some additional cluster settings to play around with. Note that we
	// need to do this before starting the server, or there will be data races.
	st := cluster.MakeTestingClusterSettings()

	s, rawDB, _ := serverutils.StartServer(t, base.TestServerArgs{Settings: st})
	defer s.Stopper().Stop(context.TODO())

	db := sqlutils.MakeSQLRunner(rawDB)

	rows := db.QueryStr(t, "SHOW ALL CLUSTER SETTINGS")
	if len(rows) < 2 {
		t.Fatalf("show all returned too few rows (%d)", len(rows))
	}
	if len(rows[0]) != 4 {
		t.Fatalf("show all must return 4 columns, found %d", len(rows[0]))
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
