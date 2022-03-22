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
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// AddImmediateGCZoneConfig set the GC TTL to 0 for the given table ID. One must
// make sure to disable strict GC TTL enforcement when using this.
func AddImmediateGCZoneConfig(sqlDB *gosql.DB, id descpb.ID) (zonepb.ZoneConfig, error) {
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

// SetShortRangeFeedIntervals is a helper to set the cluster settings
// pertaining to rangefeeds to short durations. This is helps tests which
// rely on zone/span configuration changes to propagate.
func SetShortRangeFeedIntervals(t *testing.T, db sqlutils.DBHandle) {
	tdb := sqlutils.MakeSQLRunner(db)
	short := "'20ms'"
	if util.RaceEnabled {
		short = "'200ms'"
	}
	tdb.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = `+short)
	tdb.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = `+short)
}

// AddDefaultZoneConfig adds an entry for the given id into system.zones.
func AddDefaultZoneConfig(sqlDB *gosql.DB, id descpb.ID) (zonepb.ZoneConfig, error) {
	cfg := zonepb.DefaultZoneConfig()
	buf, err := protoutil.Marshal(&cfg)
	if err != nil {
		return cfg, err
	}
	_, err = sqlDB.Exec(`UPSERT INTO system.zones VALUES ($1, $2)`, id, buf)
	return cfg, err
}

// BulkInsertIntoTable fills up table t.test with (maxValue + 1) rows.
func BulkInsertIntoTable(sqlDB *gosql.DB, maxValue int) error {
	inserts := make([]string, maxValue+1)
	for i := 0; i < maxValue+1; i++ {
		inserts[i] = fmt.Sprintf(`(%d, %d)`, i, maxValue-i)
	}
	_, err := sqlDB.Exec(`INSERT INTO t.test (k, v) VALUES ` + strings.Join(inserts, ","))
	return err
}

// GetTableKeyCount returns the number of keys in t.test.
func GetTableKeyCount(ctx context.Context, kvDB *kv.DB) (int, error) {
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	tablePrefix := keys.SystemSQLCodec.TablePrefix(uint32(tableDesc.GetID()))
	tableEnd := tablePrefix.PrefixEnd()
	kvs, err := kvDB.Scan(ctx, tablePrefix, tableEnd, 0)
	return len(kvs), err
}

// CheckTableKeyCountExact returns whether the number of keys in t.test
// equals exactly e.
func CheckTableKeyCountExact(ctx context.Context, kvDB *kv.DB, e int) error {
	if count, err := GetTableKeyCount(ctx, kvDB); err != nil {
		return err
	} else if count != e {
		return errors.Newf("expected %d key value pairs, but got %d", e, count)
	}
	return nil
}

// CheckTableKeyCount returns the number of KVs in the DB, the multiple
// should be the number of columns.
func CheckTableKeyCount(ctx context.Context, kvDB *kv.DB, multiple int, maxValue int) error {
	return CheckTableKeyCountExact(ctx, kvDB, multiple*(maxValue+1))
}

// IsClientSideQueryCanceledErr returns whether err is a client-side
// QueryCanceled error.
func IsClientSideQueryCanceledErr(err error) bool {
	if pqErr := (*pq.Error)(nil); errors.As(err, &pqErr) {
		return pgcode.MakeCode(string(pqErr.Code)) == pgcode.QueryCanceled
	}
	return pgerror.GetPGCode(err) == pgcode.QueryCanceled
}
