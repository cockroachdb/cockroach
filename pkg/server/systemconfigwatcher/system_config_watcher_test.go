// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package systemconfigwatcher_test

import (
	"context"
	gosql "database/sql"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSystemConfigWatcher is a test which exercises the end-to-end integration
// of the systemconfigwatcher
func TestSystemConfigWatcher(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t,
		base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		},
	)
	defer s.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(sqlDB)
	// Shorten the closed timestamp duration as a cheeky way to check the
	// checkpointing code while also speeding up the test.
	tdb.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '10 ms'")
	tdb.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '10 ms'")
	tdb.Exec(t, "SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval = '10 ms'")

	t.Run("system", func(t *testing.T) {
		runTest(t, s, sqlDB)
	})
	t.Run("secondary", func(t *testing.T) {
		tenant, tenantDB := serverutils.StartTenant(t, s, base.TestTenantArgs{
			TenantID: serverutils.TestTenantID(),
		})
		runTest(t, tenant, tenantDB)
	})
}

func runTest(t *testing.T, s serverutils.ApplicationLayerInterface, sqlDB *gosql.DB) {
	ctx := context.Background()
	tdb := sqlutils.MakeSQLRunner(sqlDB)
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	kvDB := execCfg.DB
	r := execCfg.SystemConfig
	rc, _ := r.RegisterSystemConfigChannel()
	clearChan := func() {
		select {
		case <-rc:
		default:
		}
	}
	checkEqual := func(t *testing.T) error {
		rs := r.GetSystemConfig()
		if rs == nil {
			return errors.New("nil config")
		}
		entries := protoutil.Clone(&rs.SystemConfigEntries).(*config.SystemConfigEntries)
		sc := getSystemDescriptorAndZonesSpans(ctx, t, execCfg.Codec, kvDB)
		sort.Sort(roachpb.KeyValueByKey(entries.Values))
		if !assert.Equal(noopT{}, sc, entries.Values) {
			return errors.Errorf("mismatch: %v", pretty.Diff(sc, entries.Values))
		}
		return nil
	}
	waitForEqual := func(t *testing.T) {
		testutils.SucceedsSoon(t, func() error {
			return checkEqual(t)
		})
	}
	waitForEqual(t)
	clearChan()
	tdb.Exec(t, "CREATE TABLE foo (i INT PRIMARY KEY)")
	<-rc
	waitForEqual(t)
}

func getSystemDescriptorAndZonesSpans(
	ctx context.Context, t *testing.T, codec keys.SQLCodec, kvDB *kv.DB,
) []roachpb.KeyValue {
	scanSpanForRows := func(startKey, endKey roachpb.Key) (rows []roachpb.KeyValue) {
		ba := &kvpb.BatchRequest{}
		ba.Add(
			kvpb.NewScan(
				append(codec.TenantPrefix(), startKey...),
				append(codec.TenantPrefix(), endKey...),
			),
		)
		br, pErr := kvDB.NonTransactionalSender().Send(ctx, ba)
		require.NoError(t, pErr.GoError())

		rows = br.Responses[0].GetScan().Rows
		return rows
	}

	return append(
		scanSpanForRows(keys.SystemDescriptorTableSpan.Key, keys.SystemDescriptorTableSpan.EndKey),
		scanSpanForRows(keys.SystemZonesTableSpan.Key, keys.SystemZonesTableSpan.EndKey)...,
	)
}

type noopT struct{}

func (noopT) Errorf(string, ...interface{}) {}

var _ assert.TestingT = (*noopT)(nil)
