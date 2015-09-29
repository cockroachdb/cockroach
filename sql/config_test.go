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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/leaktest"
	gogoproto "github.com/gogo/protobuf/proto"
)

// TestGetZoneConfig exercises config.GetZoneConfig and the sql hook for it.
func TestGetZoneConfig(t *testing.T) {
	defer leaktest.AfterTest(t)
	// Disable splitting. We're using bad attributes in zone configs
	// to be able to match.
	config.TestingDisableTableSplits = true
	defer func() { config.TestingDisableTableSplits = false }()
	s, sqlDB, _ := setup(t)
	defer cleanup(s, sqlDB)

	expectedCounter := uint32(keys.MaxReservedDescID + 1)

	// Naming scheme for database and tables:
	// db1 has tables tb11 and tb12
	// db2 has tables tb21 and tb22

	db1 := expectedCounter
	if _, err := sqlDB.Exec(`CREATE DATABASE db1`); err != nil {
		t.Fatal(err)
	}
	expectedCounter++

	db2 := expectedCounter
	if _, err := sqlDB.Exec(`CREATE DATABASE db2`); err != nil {
		t.Fatal(err)
	}
	expectedCounter++

	tb11 := expectedCounter
	if _, err := sqlDB.Exec(`CREATE TABLE db1.tb1 (k INT PRIMARY KEY, v INT)`); err != nil {
		t.Fatal(err)
	}
	expectedCounter++

	tb12 := expectedCounter
	if _, err := sqlDB.Exec(`CREATE TABLE db1.tb2 (k INT PRIMARY KEY, v INT)`); err != nil {
		t.Fatal(err)
	}
	expectedCounter++

	tb21 := expectedCounter
	if _, err := sqlDB.Exec(`CREATE TABLE db2.tb1 (k INT PRIMARY KEY, v INT)`); err != nil {
		t.Fatal(err)
	}
	expectedCounter++

	tb22 := expectedCounter
	if _, err := sqlDB.Exec(`CREATE TABLE db2.tb2 (k INT PRIMARY KEY, v INT)`); err != nil {
		t.Fatal(err)
	}
	expectedCounter++

	cfg, err := forceNewConfig(t, s)
	if err != nil {
		t.Fatalf("failed to get latest system config: %s", err)
	}

	// We have no custom zone configs.
	testCases := []struct {
		key     roachpb.Key
		zoneCfg config.ZoneConfig
	}{
		{roachpb.KeyMin, *config.DefaultZoneConfig},
		{keys.TableDataPrefix, *config.DefaultZoneConfig},
		{keys.MakeTablePrefix(1), *config.DefaultZoneConfig},
		{keys.MakeTablePrefix(keys.MaxReservedDescID), *config.DefaultZoneConfig},
		{keys.MakeTablePrefix(db1), *config.DefaultZoneConfig},
		{keys.MakeTablePrefix(db2), *config.DefaultZoneConfig},
		{keys.MakeTablePrefix(tb11), *config.DefaultZoneConfig},
		{keys.MakeTablePrefix(tb12), *config.DefaultZoneConfig},
		{keys.MakeTablePrefix(tb21), *config.DefaultZoneConfig},
		{keys.MakeTablePrefix(tb22), *config.DefaultZoneConfig},
	}

	for tcNum, tc := range testCases {
		zoneCfg, err := cfg.GetZoneConfigForKey(tc.key)
		if err != nil {
			t.Fatalf("#%d: err=%s", tcNum, err)
		}

		if !reflect.DeepEqual(*zoneCfg, tc.zoneCfg) {
			t.Errorf("#%d: bad zone config.\nexpected: %+v\ngot: %+v", tcNum, tc.zoneCfg, zoneCfg)
		}
	}

	// Now set some zone configs. We don't have a nice way of using table
	// names for this, so we do raw puts.
	// Here is the list of dbs/tables and whether they have a custom zone config:
	// db1: true
	//   tb1: true
	//   tb2: false
	// db1: false
	//   tb1: true
	//   tb2: false
	db1Cfg := config.ZoneConfig{ReplicaAttrs: []roachpb.Attributes{{[]string{"db1"}}}}
	tb11Cfg := config.ZoneConfig{ReplicaAttrs: []roachpb.Attributes{{[]string{"db1.tb1"}}}}
	tb21Cfg := config.ZoneConfig{ReplicaAttrs: []roachpb.Attributes{{[]string{"db2.tb1"}}}}
	for objID, objZone := range map[uint32]config.ZoneConfig{
		db1:  db1Cfg,
		tb11: tb11Cfg,
		tb21: tb21Cfg,
	} {
		buf, err := gogoproto.Marshal(&objZone)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = sqlDB.Exec(`INSERT INTO system.zones VALUES ($1, $2)`, objID, buf); err != nil {
			t.Fatalf("problem writing zone %+v: %s", objZone, err)
		}
	}

	cfg, err = forceNewConfig(t, s)
	if err != nil {
		t.Fatalf("failed to get latest system config: %s", err)
	}

	testCases = []struct {
		key     roachpb.Key
		zoneCfg config.ZoneConfig
	}{
		{roachpb.KeyMin, *config.DefaultZoneConfig},
		{keys.TableDataPrefix, *config.DefaultZoneConfig},
		{keys.MakeTablePrefix(1), *config.DefaultZoneConfig},
		{keys.MakeTablePrefix(keys.MaxReservedDescID), *config.DefaultZoneConfig},
		{keys.MakeTablePrefix(db1), db1Cfg},
		{keys.MakeTablePrefix(db2), *config.DefaultZoneConfig},
		{keys.MakeTablePrefix(tb11), tb11Cfg},
		{keys.MakeTablePrefix(tb12), db1Cfg},
		{keys.MakeTablePrefix(tb21), tb21Cfg},
		{keys.MakeTablePrefix(tb22), *config.DefaultZoneConfig},
	}

	for tcNum, tc := range testCases {
		zoneCfg, err := cfg.GetZoneConfigForKey(tc.key)
		if err != nil {
			t.Fatalf("#%d: err=%s", tcNum, err)
		}

		if !reflect.DeepEqual(*zoneCfg, tc.zoneCfg) {
			t.Errorf("#%d: bad zone config.\nexpected: %+v\ngot: %+v", tcNum, tc.zoneCfg, zoneCfg)
		}
	}
}
