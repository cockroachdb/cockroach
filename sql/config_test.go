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
// permissions and limitations under the License.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package sql_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/protoutil"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

var configID = sqlbase.ID(1)
var configDescKey = sqlbase.MakeDescMetadataKey(keys.MaxReservedDescID)

// forceNewConfig forces a system config update by writing a bogus descriptor with an
// incremented value inside. It then repeatedly fetches the gossip config until the
// just-written descriptor is found.
func forceNewConfig(t *testing.T, s *server.TestServer) config.SystemConfig {
	configID++
	configDesc := &sqlbase.Descriptor{
		Union: &sqlbase.Descriptor_Database{
			Database: &sqlbase.DatabaseDescriptor{
				Name:       "sentinel",
				ID:         configID,
				Privileges: &sqlbase.PrivilegeDescriptor{},
			},
		},
	}

	// This needs to be done in a transaction with the system trigger set.
	if err := s.DB().Txn(func(txn *client.Txn) error {
		txn.SetSystemConfigTrigger()
		return txn.Put(configDescKey, configDesc)
	}); err != nil {
		t.Fatal(err)
	}
	return waitForConfigChange(t, s)
}

func waitForConfigChange(t *testing.T, s *server.TestServer) config.SystemConfig {
	var foundDesc sqlbase.Descriptor
	var cfg config.SystemConfig
	util.SucceedsSoon(t, func() error {
		var ok bool
		if cfg, ok = s.Gossip().GetSystemConfig(); ok {
			if val := cfg.GetValue(configDescKey); val != nil {
				if err := val.GetProto(&foundDesc); err != nil {
					t.Fatal(err)
				}
				if id := foundDesc.GetDatabase().GetID(); id != configID {
					return errors.Errorf("expected database id %d; got %d", configID, id)
				}
				return nil
			}
		}
		return errors.Errorf("got nil system config")
	})
	return cfg
}

// TestGetZoneConfig exercises config.GetZoneConfig and the sql hook for it.
func TestGetZoneConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := createTestServerParams()
	srv, sqlDB, _ := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop()
	s := srv.(*server.TestServer)

	expectedCounter := uint32(keys.MaxReservedDescID + 1)

	defaultZoneConfig := config.DefaultZoneConfig()
	defaultZoneConfig.RangeMinBytes = 1 << 20
	defaultZoneConfig.RangeMaxBytes = 1 << 20
	defaultZoneConfig.GC.TTLSeconds = 60

	{
		buf, err := protoutil.Marshal(&defaultZoneConfig)
		if err != nil {
			t.Fatal(err)
		}
		objID := keys.RootNamespaceID
		if _, err = sqlDB.Exec(`UPDATE system.zones SET config = $2 WHERE id = $1`, objID, buf); err != nil {
			t.Fatalf("problem writing zone %+v: %s", defaultZoneConfig, err)
		}
	}

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

	{
		cfg := forceNewConfig(t, s)

		// We have no custom zone configs.
		testCases := []struct {
			key     roachpb.RKey
			zoneCfg config.ZoneConfig
		}{
			{roachpb.RKeyMin, defaultZoneConfig},
			{keys.MakeTablePrefix(0), defaultZoneConfig},
			{keys.MakeTablePrefix(1), defaultZoneConfig},
			{keys.MakeTablePrefix(keys.MaxReservedDescID), defaultZoneConfig},
			{keys.MakeTablePrefix(db1), defaultZoneConfig},
			{keys.MakeTablePrefix(db2), defaultZoneConfig},
			{keys.MakeTablePrefix(tb11), defaultZoneConfig},
			{keys.MakeTablePrefix(tb12), defaultZoneConfig},
			{keys.MakeTablePrefix(tb21), defaultZoneConfig},
			{keys.MakeTablePrefix(tb22), defaultZoneConfig},
		}

		for tcNum, tc := range testCases {
			zoneCfg, err := cfg.GetZoneConfigForKey(tc.key)
			if err != nil {
				t.Fatalf("#%d: err=%s", tcNum, err)
			}

			if !proto.Equal(&zoneCfg, &tc.zoneCfg) {
				t.Errorf("#%d: bad zone config.\nexpected: %+v\ngot: %+v", tcNum, tc.zoneCfg, zoneCfg)
			}
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
	db1Cfg := config.ZoneConfig{
		ReplicaAttrs: []roachpb.Attributes{{Attrs: []string{"db1"}}},
	}
	tb11Cfg := config.ZoneConfig{
		ReplicaAttrs: []roachpb.Attributes{{Attrs: []string{"db1.tb1"}}},
	}
	tb21Cfg := config.ZoneConfig{
		ReplicaAttrs: []roachpb.Attributes{{Attrs: []string{"db2.tb1"}}},
	}
	for objID, objZone := range map[uint32]config.ZoneConfig{
		db1:  db1Cfg,
		tb11: tb11Cfg,
		tb21: tb21Cfg,
	} {
		buf, err := protoutil.Marshal(&objZone)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = sqlDB.Exec(`INSERT INTO system.zones VALUES ($1, $2)`, objID, buf); err != nil {
			t.Fatalf("problem writing zone %+v: %s", objZone, err)
		}
	}

	{
		cfg := forceNewConfig(t, s)

		testCases := []struct {
			key     roachpb.RKey
			zoneCfg config.ZoneConfig
		}{
			{roachpb.RKeyMin, defaultZoneConfig},
			{keys.MakeTablePrefix(0), defaultZoneConfig},
			{keys.MakeTablePrefix(1), defaultZoneConfig},
			{keys.MakeTablePrefix(keys.MaxReservedDescID), defaultZoneConfig},
			{keys.MakeTablePrefix(db1), db1Cfg},
			{keys.MakeTablePrefix(db2), defaultZoneConfig},
			{keys.MakeTablePrefix(tb11), tb11Cfg},
			{keys.MakeTablePrefix(tb12), db1Cfg},
			{keys.MakeTablePrefix(tb21), tb21Cfg},
			{keys.MakeTablePrefix(tb22), defaultZoneConfig},
		}

		for tcNum, tc := range testCases {
			zoneCfg, err := cfg.GetZoneConfigForKey(tc.key)
			if err != nil {
				t.Fatalf("#%d: err=%s", tcNum, err)
			}

			if !proto.Equal(&zoneCfg, &tc.zoneCfg) {
				t.Errorf("#%d: bad zone config.\nexpected: %+v\ngot: %+v", tcNum, tc.zoneCfg, zoneCfg)
			}
		}
	}
}
