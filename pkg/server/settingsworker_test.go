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
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

const strKey = "testing.str"
const intKey = "testing.int"
const durationKey = "testing.duration"
const byteSizeKey = "testing.bytesize"

var strA = settings.RegisterStringSetting(strKey, "", "<default>")
var intA = settings.RegisterIntSetting(intKey, "", 1)
var durationA = settings.RegisterDurationSetting(durationKey, "", time.Minute)
var byteSizeA = settings.RegisterByteSizeSetting(byteSizeKey, "", 1024*1024)

func TestSettingsRefresh(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, rawDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	db := sqlutils.MakeSQLRunner(t, rawDB)

	insertQ := `UPSERT INTO system.settings (name, value, lastUpdated, valueType)
		VALUES ($1, $2, NOW(), $3)`
	deleteQ := "DELETE FROM system.settings WHERE name = $1"

	if expected, actual := "<default>", strA.Get(); expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
	if expected, actual := int64(1), intA.Get(); expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}

	// Inserting a new setting is reflected in cache.
	db.Exec(insertQ, strKey, "foo", "s")
	db.Exec(insertQ, intKey, settings.EncodeInt(2), "i")
	// Wait until we observe the gossip-driven update propagating to cache.
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := "foo", strA.Get(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := int64(2), intA.Get(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// Setting to empty also works.
	db.Exec(insertQ, strKey, "", "s")
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := "", strA.Get(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// An unknown value doesn't block updates to a known one.
	db.Exec(insertQ, "dne", "???", "s")
	db.Exec(insertQ, strKey, "qux", "s")

	testutils.SucceedsSoon(t, func() error {
		if expected, actual := "qux", strA.Get(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := int64(2), intA.Get(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// A malformed value doesn't revert previous set or block other changes.
	db.Exec(deleteQ, "dne")
	db.Exec(insertQ, intKey, "invalid", "i")
	db.Exec(insertQ, strKey, "after-invalid", "s")

	testutils.SucceedsSoon(t, func() error {
		if expected, actual := int64(2), intA.Get(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "after-invalid", strA.Get(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// A mis-typed value doesn't revert previous set or block other changes.
	db.Exec(insertQ, intKey, settings.EncodeInt(7), "b")
	db.Exec(insertQ, strKey, "after-mistype", "s")

	testutils.SucceedsSoon(t, func() error {
		if expected, actual := int64(2), intA.Get(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "after-mistype", strA.Get(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// TODO(dt): add placeholder support to SET and SHOW.
	setQ := "SET CLUSTER SETTING %s = %s"
	showQ := "SHOW CLUSTER SETTING %s"

	// SET/SHOW work too.
	db.Exec(fmt.Sprintf(setQ, strKey, "'via-set'"))
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := "via-set", db.QueryStr(fmt.Sprintf(showQ, strKey))[0][0]; expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// SET/SHOW work too.
	db.Exec(fmt.Sprintf(setQ, intKey, "5"))
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := "5", db.QueryStr(fmt.Sprintf(showQ, intKey))[0][0]; expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	db.Exec(fmt.Sprintf(setQ, durationKey, "'2h'"))
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := time.Hour*2, durationA.Get(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "2h", db.QueryStr(fmt.Sprintf(showQ, durationKey))[0][0]; expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	db.Exec(fmt.Sprintf(setQ, byteSizeKey, "'1500MB'"))
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := int64(1500000000), byteSizeA.Get(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "1.4 GiB", db.QueryStr(fmt.Sprintf(showQ, byteSizeKey))[0][0]; expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	db.Exec(fmt.Sprintf(setQ, byteSizeKey, "'1450MB'"))
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := "1.4 GiB", db.QueryStr(fmt.Sprintf(showQ, byteSizeKey))[0][0]; expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// Deleting a value reverts to default.
	db.Exec(deleteQ, strKey)
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := "<default>", strA.Get(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	rows := db.QueryStr("SHOW ALL CLUSTER SETTINGS")
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
