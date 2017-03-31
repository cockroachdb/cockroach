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
	"testing"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestSettingsRefresh(t *testing.T) {
	defer leaktest.AfterTest(t)()

	strKey, intKey, cleanup := settings.TestingAddTestVars()
	defer cleanup()

	args := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLExecutor: &sql.ExecutorTestingKnobs{
					WaitForGossipUpdate: true,
				},
			},
		},
	}

	// NB: all 3 "nodes" in our testcluster still share same `settings` package.
	tc := testcluster.StartTestCluster(t, 3, args)
	defer tc.Stopper().Stop()

	db1 := sqlutils.MakeSQLRunner(t, tc.Conns[0])
	db2 := sqlutils.MakeSQLRunner(t, tc.Conns[1])

	insertQ := `UPSERT INTO system.settings (name, value, lastUpdated, valueType)
		VALUES ($1, $2, NOW(), $3)`

	deleteQ := "DELETE FROM system.settings WHERE name = $1"

	if expected, actual := "<default>", settings.TestingGetString(); expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
	if expected, actual := 1, settings.TestingGetInt(); expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}

	// Inserting a new setting is reflected in cache.
	db1.Exec(insertQ, strKey, "foo", "s")
	db2.Exec(insertQ, intKey, settings.EncodeInt(2), "i")
	// Wait until we observe the gossip-driven update propagating to cache.
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := "foo", settings.TestingGetString(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 2, settings.TestingGetInt(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// Setting to empty also works.
	db2.Exec(insertQ, strKey, "", "s")
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := "", settings.TestingGetString(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// An unknown value doesn't block updates to a known one.
	db1.Exec(insertQ, "dne", "???", "s")
	db1.Exec(insertQ, strKey, "qux", "s")

	testutils.SucceedsSoon(t, func() error {
		if expected, actual := "qux", settings.TestingGetString(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 2, settings.TestingGetInt(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// A malformed value doesn't revert previous set or block other changes.
	db1.Exec(deleteQ, "dne")
	db1.Exec(insertQ, intKey, "invalid", "i")
	db1.Exec(insertQ, strKey, "after-invalid", "s")

	testutils.SucceedsSoon(t, func() error {
		if expected, actual := 2, settings.TestingGetInt(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "after-invalid", settings.TestingGetString(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// A mis-typed value doesn't revert previous set or block other changes.
	db1.Exec(insertQ, intKey, settings.EncodeInt(7), "b")
	db1.Exec(insertQ, strKey, "after-mistype", "s")

	testutils.SucceedsSoon(t, func() error {
		if expected, actual := 2, settings.TestingGetInt(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "after-mistype", settings.TestingGetString(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// Deleting a value reverts to default.
	db2.Exec(deleteQ, strKey)
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := "<default>", settings.TestingGetString(); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})
}
