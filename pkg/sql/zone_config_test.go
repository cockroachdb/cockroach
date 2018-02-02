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

package sql_test

import (
	"context"
	"testing"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

var configID = sqlbase.ID(1)
var configDescKey = sqlbase.MakeDescMetadataKey(keys.MaxReservedDescID)

// forceNewConfig forces a system config update by writing a bogus descriptor with an
// incremented value inside. It then repeatedly fetches the gossip config until the
// just-written descriptor is found.
func forceNewConfig(t testing.TB, s *server.TestServer) config.SystemConfig {
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
	if err := s.DB().Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		return txn.Put(ctx, configDescKey, configDesc)
	}); err != nil {
		t.Fatal(err)
	}
	return waitForConfigChange(t, s)
}

func waitForConfigChange(t testing.TB, s *server.TestServer) config.SystemConfig {
	var foundDesc sqlbase.Descriptor
	var cfg config.SystemConfig
	testutils.SucceedsSoon(t, func() error {
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
	params, _ := tests.CreateTestServerParams()
	srv, sqlDB, _ := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop(context.TODO())
	s := srv.(*server.TestServer)

	expectedCounter := uint32(keys.MaxReservedDescID)

	defaultZoneConfig := config.DefaultZoneConfig()
	defaultZoneConfig.RangeMinBytes = 1 << 20
	defaultZoneConfig.RangeMaxBytes = 1 << 20
	defaultZoneConfig.GC.TTLSeconds = 60

	type testCase struct {
		objectID uint32

		// keySuffix and partitionName must specify the same subzone.
		keySuffix     []byte
		partitionName string

		zoneCfg config.ZoneConfig
	}
	verifyZoneConfigs := func(testCases []testCase) {
		cfg := forceNewConfig(t, s)

		for tcNum, tc := range testCases {
			// Verify SystemConfig.GetZoneConfigForKey.
			{
				key := append(keys.MakeTablePrefix(tc.objectID), tc.keySuffix...)
				zoneCfg, err := cfg.GetZoneConfigForKey(key)
				if err != nil {
					t.Fatalf("#%d: err=%s", tcNum, err)
				}

				if !tc.zoneCfg.Equal(zoneCfg) {
					t.Errorf("#%d: bad zone config.\nexpected: %+v\ngot: %+v", tcNum, tc.zoneCfg, zoneCfg)
				}
			}

			// Verify sql.GetZoneConfigInTxn.
			if err := s.DB().Txn(context.Background(), func(ctx context.Context, txn *client.Txn) error {
				_, zoneCfg, subzone, err := sql.GetZoneConfigInTxn(ctx, txn,
					tc.objectID, &sqlbase.IndexDescriptor{}, tc.partitionName)
				if err != nil {
					return err
				} else if subzone != nil {
					zoneCfg = subzone.Config
				}
				if !tc.zoneCfg.Equal(zoneCfg) {
					t.Errorf("#%d: bad zone config.\nexpected: %+v\ngot: %+v", tcNum, tc.zoneCfg, zoneCfg)
				}
				return nil
			}); err != nil {
				t.Fatalf("#%d: err=%s", tcNum, err)
			}
		}
	}

	{
		buf, err := protoutil.Marshal(&defaultZoneConfig)
		if err != nil {
			t.Fatal(err)
		}
		objID := keys.RootNamespaceID
		if _, err = sqlDB.Exec(`UPDATE system.public.zones SET config = $2 WHERE id = $1`, objID, buf); err != nil {
			t.Fatalf("problem writing zone %+v: %s", defaultZoneConfig, err)
		}
	}

	// Naming scheme for database and tables:
	// db1 has tables tb11 and tb12
	// db2 has tables tb21 and tb22

	expectedCounter++
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
	if _, err := sqlDB.Exec(`CREATE TABLE db1.public.tb1 (k INT PRIMARY KEY, v INT)`); err != nil {
		t.Fatal(err)
	}

	expectedCounter++
	tb12 := expectedCounter
	if _, err := sqlDB.Exec(`CREATE TABLE db1.public.tb2 (k INT PRIMARY KEY, v INT)`); err != nil {
		t.Fatal(err)
	}

	expectedCounter++
	tb21 := expectedCounter
	if _, err := sqlDB.Exec(`CREATE TABLE db2.public.tb1 (k INT PRIMARY KEY, v INT)`); err != nil {
		t.Fatal(err)
	}

	expectedCounter++
	if _, err := sqlDB.Exec(`CREATE TABLE db2.public.tb2 (k INT PRIMARY KEY, v INT)`); err != nil {
		t.Fatal(err)
	}

	expectedCounter++
	tb22 := expectedCounter
	if _, err := sqlDB.Exec(`TRUNCATE TABLE db2.public.tb2`); err != nil {
		t.Fatal(err)
	}

	// We have no custom zone configs.
	verifyZoneConfigs([]testCase{
		{0, nil, "", defaultZoneConfig},
		{1, nil, "", defaultZoneConfig},
		{keys.MaxReservedDescID, nil, "", defaultZoneConfig},
		{db1, nil, "", defaultZoneConfig},
		{db2, nil, "", defaultZoneConfig},
		{tb11, nil, "", defaultZoneConfig},
		{tb11, []byte{42}, "p0", defaultZoneConfig},
		{tb12, nil, "", defaultZoneConfig},
		{tb12, []byte{42}, "p0", defaultZoneConfig},
		{tb21, nil, "", defaultZoneConfig},
		{tb22, nil, "", defaultZoneConfig},
	})

	// Now set some zone configs. We don't have a nice way of using table
	// names for this, so we do raw puts.
	// Here is the list of dbs/tables/partitions and whether they have a custom
	// zone config:
	// db1: true
	//   tb1: true
	//   tb2: false
	// db2: false
	//   tb1: true
	//     p1: true [1, 2), [6, 7)
	//     p2: true [3, 5)
	//   tb2: false
	//     p1: true  [1, 255)
	db1Cfg := config.ZoneConfig{
		NumReplicas: 1,
		Constraints: config.Constraints{Constraints: []config.Constraint{{Value: "db1"}}},
	}
	tb11Cfg := config.ZoneConfig{
		NumReplicas: 1,
		Constraints: config.Constraints{Constraints: []config.Constraint{{Value: "db1.tb1"}}},
	}
	p211Cfg := config.ZoneConfig{
		NumReplicas: 1,
		Constraints: config.Constraints{Constraints: []config.Constraint{{Value: "db2.tb1.p1"}}},
	}
	p212Cfg := config.ZoneConfig{
		NumReplicas: 1,
		Constraints: config.Constraints{Constraints: []config.Constraint{{Value: "db2.tb1.p2"}}},
	}
	tb21Cfg := config.ZoneConfig{
		NumReplicas: 1,
		Constraints: config.Constraints{Constraints: []config.Constraint{{Value: "db2.tb1"}}},
		Subzones: []config.Subzone{
			{PartitionName: "p0", Config: p211Cfg},
			{PartitionName: "p1", Config: p212Cfg},
		},
		SubzoneSpans: []config.SubzoneSpan{
			{SubzoneIndex: 0, Key: []byte{1}},
			{SubzoneIndex: 1, Key: []byte{3}, EndKey: []byte{5}},
			{SubzoneIndex: 0, Key: []byte{6}},
		},
	}
	p221Cfg := config.ZoneConfig{
		NumReplicas: 1,
		Constraints: config.Constraints{Constraints: []config.Constraint{{Value: "db2.tb2.p1"}}},
	}
	tb22Cfg := config.ZoneConfig{
		NumReplicas: 0,
		Subzones:    []config.Subzone{{PartitionName: "p0", Config: p221Cfg}},
		SubzoneSpans: []config.SubzoneSpan{
			{SubzoneIndex: 0, Key: []byte{1}, EndKey: []byte{255}},
		},
	}
	for objID, objZone := range map[uint32]config.ZoneConfig{
		db1:  db1Cfg,
		tb11: tb11Cfg,
		tb21: tb21Cfg,
		tb22: tb22Cfg,
	} {
		buf, err := protoutil.Marshal(&objZone)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = sqlDB.Exec(`INSERT INTO system.public.zones VALUES ($1, $2)`, objID, buf); err != nil {
			t.Fatalf("problem writing zone %+v: %s", objZone, err)
		}
	}

	verifyZoneConfigs([]testCase{
		{0, nil, "", defaultZoneConfig},
		{1, nil, "", defaultZoneConfig},
		{keys.MaxReservedDescID, nil, "", defaultZoneConfig},
		{db1, nil, "", db1Cfg},
		{db2, nil, "", defaultZoneConfig},
		{tb11, nil, "", tb11Cfg},
		{tb11, []byte{42}, "p0", tb11Cfg},
		{tb12, nil, "", db1Cfg},
		{tb12, []byte{42}, "p0", db1Cfg},
		{tb21, nil, "", tb21Cfg},
		{tb21, []byte{}, "", tb21Cfg},
		{tb21, []byte{0}, "", tb21Cfg},
		{tb21, []byte{1}, "p0", p211Cfg},
		{tb21, []byte{1, 255}, "p0", p211Cfg},
		{tb21, []byte{2}, "", tb21Cfg},
		{tb21, []byte{3}, "p1", p212Cfg},
		{tb21, []byte{4}, "p1", p212Cfg},
		{tb21, []byte{5}, "", tb21Cfg},
		{tb21, []byte{6}, "p0", p211Cfg},
		{tb22, nil, "", defaultZoneConfig},
		{tb22, []byte{0}, "", defaultZoneConfig},
		{tb22, []byte{1}, "p0", p221Cfg},
		{tb22, []byte{255}, "", defaultZoneConfig},
	})
}

func BenchmarkGetZoneConfig(b *testing.B) {
	defer leaktest.AfterTest(b)()

	params, _ := tests.CreateTestServerParams()
	srv, _, _ := serverutils.StartServer(b, params)
	defer srv.Stopper().Stop(context.TODO())
	s := srv.(*server.TestServer)
	cfg := forceNewConfig(b, s)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := cfg.GetZoneConfigForKey(keys.MakeTablePrefix(keys.MaxReservedDescID + 1))
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}
