// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package sql_test

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/gogo/protobuf/proto"
)

var configID = sql.ID(1)
var configDescKey = sql.MakeDescMetadataKey(keys.MaxReservedDescID)

// forceNewConfig forces a system config update by writing a bogus descriptor with an
// incremented value inside. It then repeatedly fetches the gossip config until the
// just-written descriptor is found.
func forceNewConfig(t *testing.T, s *server.TestServer) (*config.SystemConfig, error) {
	configID++
	configDesc := sql.DatabaseDescriptor{
		Name:       "sentinel",
		ID:         configID,
		Privileges: &sql.PrivilegeDescriptor{},
	}

	// This needs to be done in a transaction with the system trigger set.
	if err := s.DB().Txn(func(txn *client.Txn) error {
		txn.SetSystemDBTrigger()
		return txn.Put(configDescKey, &configDesc)
	}); err != nil {
		t.Fatal(err)
	}
	return waitForConfigChange(t, s)
}

func waitForConfigChange(t *testing.T, s *server.TestServer) (*config.SystemConfig, error) {
	var foundDesc sql.DatabaseDescriptor
	var cfg *config.SystemConfig
	return cfg, util.IsTrueWithin(func() bool {
		if cfg = s.Gossip().GetSystemConfig(); cfg != nil {
			if val := cfg.GetValue(configDescKey); val != nil {
				if err := val.GetProto(&foundDesc); err != nil {
					t.Fatal(err)
				}
				return foundDesc.ID == configID
			}
		}

		return false
	}, 10*time.Second)
}

// TestDescriptorCacheSchemaMutation shows how the cache can hide
// failures when mutating the schema and operating on it.
// When the operation adds a new value, this works fine since the
// cache descriptor tries on KV. However, entries that exist in the
// cache but not in KV will behave badly.
// This always fails with transactions. Without transaction, it is
// too dependent on timing to test reliable.
func TestDescriptorCacheSchemaMutation(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, sqlDB, _ := setup(t)
	defer cleanup(s, sqlDB)

	// Create the database, table and do a basic op in a transaction.
	// The cache falls back on KV lookups so should have no issues.
	if _, err := sqlDB.Exec(`
BEGIN TRANSACTION;
CREATE DATABASE test;
CREATE TABLE test.test (k INT PRIMARY KEY, v INT);
SELECT * FROM test.test;
COMMIT TRANSACTION;
`); err != nil {
		t.Fatal(err)
	}

	// Wait for system config load.
	if _, err := forceNewConfig(t, s); err != nil {
		t.Fatalf("failed to get latest system config: %s", err)
	}

	// Drop a table and perform a select within the same sql transaction.
	// The table is found in the cache, so the select succeeds.
	if _, err := sqlDB.Exec(`
BEGIN TRANSACTION;
DROP TABLE test.test;
SELECT * FROM test.test;
COMMIT TRANSACTION;
`); err != nil {
		t.Fatal(err)
	}

	sql.TestingDisableDescriptorCache = true
	defer func() { sql.TestingDisableDescriptorCache = false }()
	// With caching disabled, this fails. We intentionally execute statements separately
	// to ensure 'SELECT' is the failing one.
	if _, err := sqlDB.Exec(`CREATE TABLE test.test (k INT PRIMARY KEY, v INT)`); err != nil {
		t.Fatal(err)
	}
	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`DROP TABLE test.test`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`SELECT * FROM test.test`); err == nil {
		t.Fatal("unexpected success")
	}
	_ = tx.Rollback()
}

// TestDescriptorCache populates the system config with fake entries
// and verifies that the cache uses them.
func TestDescriptorCache(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, sqlDB, _ := setup(t)
	defer cleanup(s, sqlDB)

	if _, err := sqlDB.Exec(`CREATE DATABASE test`); err != nil {
		t.Fatal(err)
	}

	// Create two tables. We'll be populating them with different data.
	if _, err := sqlDB.Exec(`CREATE TABLE test.test1 (k INT PRIMARY KEY, v INT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`CREATE TABLE test.test2 (k INT PRIMARY KEY, v INT)`); err != nil {
		t.Fatal(err)
	}

	// Put some data into each table. test1 has one row, test2 has two.
	if _, err := sqlDB.Exec(`INSERT INTO test.test1 VALUES (1, 1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO test.test2 VALUES (2, 2), (3, 3)`); err != nil {
		t.Fatal(err)
	}

	// Wait for system config load.
	cfg, err := forceNewConfig(t, s)
	if err != nil {
		t.Fatalf("failed to get latest system config: %s", err)
	}

	dbID := keys.MaxReservedDescID + 1
	test1NameKey := sql.MakeNameMetadataKey(sql.ID(dbID), "test1")
	test2NameKey := sql.MakeNameMetadataKey(sql.ID(dbID), "test2")
	test1Index, ok := cfg.GetIndex(test1NameKey)
	if !ok {
		t.Fatalf("%s not found", test1NameKey)
	}

	test2Index, ok := cfg.GetIndex(test2NameKey)
	if !ok {
		t.Fatalf("%s not found", test2NameKey)
	}

	// Swap the namespace entries for the 'test1' and 'test2' tables in the system config.
	// This means that "test.test1" will resolve to the object ID for test2, and vice-versa.
	cfg.Values[test1Index].Value, cfg.Values[test2Index].Value =
		cfg.Values[test2Index].Value, cfg.Values[test1Index].Value
	// Gossip it and wait. We increment the fake descriptor in the system config and
	// wait for the callback to have it.
	configID++
	configDesc := sql.DatabaseDescriptor{
		Name:       "sentinel",
		ID:         configID,
		Privileges: &sql.PrivilegeDescriptor{},
	}
	raw, err := proto.Marshal(&configDesc)
	if err != nil {
		t.Fatal(err)
	}
	configDescIndex, ok := cfg.GetIndex(configDescKey)
	if !ok {
		t.Fatalf("%s not found", configDescKey)
	}
	cfg.Values[configDescIndex].Value.SetBytes(raw)
	if err := s.Gossip().AddInfoProto(gossip.KeySystemConfig, cfg, 0); err != nil {
		t.Fatal(err)
	}

	if _, err := waitForConfigChange(t, s); err != nil {
		t.Fatal(err)
	}

	// Pfeww. That was tedious. Now count the number of entries in each table.
	// We just waited for the config, so the cache should have it.
	// test1 was hacked to point to test2 which has two entries.
	count := 0
	if err := sqlDB.QueryRow(`SELECT COUNT(*) FROM test.test1`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if e := 2; count != e {
		t.Errorf("Bad number of rows from hacked test1, expected %d, got %d", e, count)
	}
	// test2 was hacked to point to test1 which has one entry.
	if err := sqlDB.QueryRow(`SELECT COUNT(*) FROM test.test2`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if e := 1; count != e {
		t.Errorf("Bad number of rows from hacked test2, expected %d, got %d", e, count)
	}

	// Now try again without caching.
	sql.TestingDisableDescriptorCache = true
	defer func() { sql.TestingDisableDescriptorCache = false }()
	if err := sqlDB.QueryRow(`SELECT COUNT(*) FROM test.test1`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if e := 1; count != e {
		t.Errorf("Bad number of rows from test1, expected %d, got %d", e, count)
	}
	if err := sqlDB.QueryRow(`SELECT COUNT(*) FROM test.test2`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if e := 2; count != e {
		t.Errorf("Bad number of rows from test2, expected %d, got %d", e, count)
	}
}
