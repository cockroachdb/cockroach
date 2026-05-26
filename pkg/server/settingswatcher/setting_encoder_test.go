// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package settingswatcher_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestSettingsEncoder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	ts := srv.ApplicationLayer()

	tdb := sqlutils.MakeSQLRunner(db)
	tdb.Exec(t, "SELECT 1")
	codec := ts.Codec()

	// Fetch the version from settings table, with a key
	// we encoded our selves.
	key := settingswatcher.EncodeSettingKey(codec, "version")
	kv, err := ts.DB().Get(ctx, key)
	require.NoError(t, err)

	// Fetch the currently observed version from SQL.
	currentVersion := tdb.QueryStr(t,
		`
SELECT
	value
FROM
	system.settings
WHERE
	name = 'version';
`)

	// Ensure we read the version value back.
	require.NoError(t, err)
	decoder := settingswatcher.MakeRowDecoder(codec)
	_, val, _, err := decoder.DecodeRow(roachpb.KeyValue{Key: kv.Key, Value: *kv.Value}, nil)
	require.NoError(t, err)
	var version clusterversion.ClusterVersion
	require.NoError(t, protoutil.Unmarshal([]byte(val.Value), &version))
	require.Equalf(t, []byte(val.Value), []byte(currentVersion[0][0]), "version did not match the expected value")

	// Next set the version to an invalid one using pure KV calls
	start := timeutil.Now()
	version = clusterversion.ClusterVersion{Version: clusterversion.MinSupported.Version()}
	versionBytes, err := protoutil.Marshal(&version)
	require.NoError(t, err)
	newVal, err := settingswatcher.EncodeSettingValue(versionBytes, val.Type)
	require.NoError(t, err)
	kvVal := &roachpb.Value{}
	kvVal.SetTuple(newVal)
	err = ts.DB().Put(ctx, roachpb.Key(key), kvVal)
	after := timeutil.Now()
	require.NoError(t, err)
	// Validate the current version is now the minimum.
	currentVersion = tdb.QueryStr(t,
		`
SELECT
	value, "valueType"
FROM
	system.settings
WHERE
	name = 'version';
`)
	require.Equalf(t, []byte(currentVersion[0][0]), versionBytes, "current version in settings doesn't match")
	require.Equalf(t, currentVersion[0][1], "m", "setting type was lost")
	// Validate the timestamp was set.
	row := tdb.QueryRow(t, `SELECT "lastUpdated" from system.settings WHERE name='version'`)
	var rowTime time.Time
	row.Scan(&rowTime)
	require.Greaterf(t, rowTime, start, "Time was less than expect")
	require.Lessf(t, rowTime, after, "Time was greater than expect")
}
