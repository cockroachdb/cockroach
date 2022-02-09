// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package systemconfigwatchertest exists to exercise systemconfigwatcher
// in both ccl and non-ccl configurations.
package systemconfigwatchertest

import (
	"context"
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSystemConfigWatcher is a test which exercises the end-to-end integration
// of the systemconfigwatcher. It exists in this subpackage so that it can be
// run to exercise secondary tenants, which are ccl-only.
func TestSystemConfigWatcher(t *testing.T, skipSecondary bool) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(sqlDB)
	// Shorten the closed timestamp duration as a cheeky way to check the
	// checkpointing code while also speeding up the test.
	tdb.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '10 ms'")
	tdb.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '10 ms'")

	t.Run("system", func(t *testing.T) {
		runTest(t, s, sqlDB)
	})
	if !skipSecondary {
		t.Run("secondary", func(t *testing.T) {
			tenant, tenantDB := serverutils.StartTenant(t, s, base.TestTenantArgs{
				TenantID: serverutils.TestTenantID(),
			})
			runTest(t, tenant, tenantDB)
		})
	}
}

func runTest(t *testing.T, s serverutils.TestTenantInterface, sqlDB *gosql.DB) {
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
	getSystemConfig := func(t *testing.T) []roachpb.KeyValue {
		var ba roachpb.BatchRequest
		ba.Add(roachpb.NewScan(
			append(execCfg.Codec.TenantPrefix(), keys.SystemConfigSpan.Key...),
			append(execCfg.Codec.TenantPrefix(), keys.SystemConfigSpan.EndKey...),
			false, // forUpdate
		))
		br, pErr := kvDB.NonTransactionalSender().Send(ctx, ba)
		require.NoError(t, pErr.GoError())
		return br.Responses[0].GetScan().Rows
	}
	checkEqual := func(t *testing.T) error {
		rs := r.GetSystemConfig()
		if rs == nil {
			return errors.New("nil config")
		}
		sc := getSystemConfig(t)
		if !assert.Equal(noopT{}, sc, rs.Values) {
			return errors.Errorf("mismatch: %v", pretty.Diff(sc, rs.Values))
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

type noopT struct{}

func (noopT) Errorf(string, ...interface{}) {}

var _ assert.TestingT = (*noopT)(nil)
