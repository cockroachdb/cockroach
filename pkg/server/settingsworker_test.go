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

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

const strKey = "testing.str"
const intKey = "testing.int"

var strAccessor = settings.RegisterStringSetting(strKey, "", "<default>")
var intAccessor = settings.RegisterIntSetting(intKey, "", 1)

func TestSettingsRefresh(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, rawDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	db := sqlutils.MakeSQLRunner(t, rawDB)

	insertQ := `UPSERT INTO system.settings (name, value, lastUpdated, valueType)
		VALUES ($1, $2, NOW(), $3)`
	deleteQ := "DELETE FROM system.settings WHERE name = $1"

	if expected, actual := "<default>", strAccessor(); expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
	if expected, actual := 1, intAccessor(); expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}

	// Inserting a new setting is reflected in cache.
	db.Exec(insertQ, strKey, "foo", "s")
	db.Exec(insertQ, intKey, settings.EncodeInt(2), "i")
	// Wait until we observe the gossip-driven update propagating to cache.
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := "foo", strAccessor(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 2, intAccessor(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// Setting to empty also works.
	db.Exec(insertQ, strKey, "", "s")
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := "", strAccessor(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// An unknown value doesn't block updates to a known one.
	db.Exec(insertQ, "dne", "???", "s")
	db.Exec(insertQ, strKey, "qux", "s")

	testutils.SucceedsSoon(t, func() error {
		if expected, actual := "qux", strAccessor(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 2, intAccessor(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// A malformed value doesn't revert previous set or block other changes.
	db.Exec(deleteQ, "dne")
	db.Exec(insertQ, intKey, "invalid", "i")
	db.Exec(insertQ, strKey, "after-invalid", "s")

	testutils.SucceedsSoon(t, func() error {
		if expected, actual := 2, intAccessor(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "after-invalid", strAccessor(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// A mis-typed value doesn't revert previous set or block other changes.
	db.Exec(insertQ, intKey, settings.EncodeInt(7), "b")
	db.Exec(insertQ, strKey, "after-mistype", "s")

	testutils.SucceedsSoon(t, func() error {
		if expected, actual := 2, intAccessor(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "after-mistype", strAccessor(); expected != actual {
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

	// Deleting a value reverts to default.
	db.Exec(deleteQ, strKey)
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := "<default>", strAccessor(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})
}
