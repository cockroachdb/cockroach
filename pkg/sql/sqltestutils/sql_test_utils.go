// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package sqltestutils provides helper methods for testing sql packages.
package sqltestutils

import (
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// AddImmediateGCZoneConfig set the GC TTL to 0 for the given table ID. One must
// make sure to disable strict GC TTL enforcement when using this.
func AddImmediateGCZoneConfig(sqlDB *gosql.DB, id sqlbase.ID) (zonepb.ZoneConfig, error) {
	cfg := zonepb.DefaultZoneConfig()
	cfg.GC.TTLSeconds = 0
	buf, err := protoutil.Marshal(&cfg)
	if err != nil {
		return cfg, err
	}
	_, err = sqlDB.Exec(`UPSERT INTO system.zones VALUES ($1, $2)`, id, buf)
	return cfg, err
}

// DisableGCTTLStrictEnforcement sets the cluster setting to disable strict
// GC TTL enforcement.
func DisableGCTTLStrictEnforcement(t *testing.T, db *gosql.DB) (cleanup func()) {
	_, err := db.Exec(`SET CLUSTER SETTING kv.gc_ttl.strict_enforcement.enabled = false`)
	require.NoError(t, err)
	return func() {
		_, err := db.Exec(`SET CLUSTER SETTING kv.gc_ttl.strict_enforcement.enabled = DEFAULT`)
		require.NoError(t, err)
	}
}

// AddDefaultZoneConfig adds an entry for the given id into system.zones.
func AddDefaultZoneConfig(sqlDB *gosql.DB, id sqlbase.ID) (zonepb.ZoneConfig, error) {
	cfg := zonepb.DefaultZoneConfig()
	buf, err := protoutil.Marshal(&cfg)
	if err != nil {
		return cfg, err
	}
	_, err = sqlDB.Exec(`UPSERT INTO system.zones VALUES ($1, $2)`, id, buf)
	return cfg, err
}
