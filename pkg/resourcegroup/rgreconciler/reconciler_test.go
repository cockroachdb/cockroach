// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rgreconciler_test

import (
	"context"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/resourcegroup"
	"github.com/cockroachdb/cockroach/pkg/resourcegroup/rgpb"
	"github.com/cockroachdb/cockroach/pkg/resourcegroup/rgreconciler"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type recordingPusher struct {
	mu       sync.Mutex
	upserts  []*rgpb.ResourceGroupUpsert
	deletes  []*rgpb.ResourceGroupDelete
	replaces int
}

var _ resourcegroup.Pusher = (*recordingPusher)(nil)

func (p *recordingPusher) Push(
	_ context.Context, ups []*rgpb.ResourceGroupUpsert, dels []*rgpb.ResourceGroupDelete,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.upserts = append(p.upserts, ups...)
	p.deletes = append(p.deletes, dels...)
	return nil
}

func (p *recordingPusher) Replace(_ context.Context, ups []*rgpb.ResourceGroupUpsert) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.replaces++
	p.upserts = append(p.upserts, ups...)
	return nil
}

func (p *recordingPusher) snapshot() ([]*rgpb.ResourceGroupUpsert, []*rgpb.ResourceGroupDelete) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*rgpb.ResourceGroupUpsert(nil), p.upserts...),
		append([]*rgpb.ResourceGroupDelete(nil), p.deletes...)
}

// TestReconcilerTrackingChanges runs a reconciler against a real
// system.resource_groups table and asserts that creates, updates, and
// drops are forwarded to the Pusher via the rangefeed.
func TestReconcilerTrackingChanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(s.SQLConn(t))
	sqlDB.Exec(t, "SET CLUSTER SETTING sql.experimental_resource_groups.enabled = true")

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	pusher := &recordingPusher{}
	r := rgreconciler.New(
		execCfg.Codec,
		execCfg.Clock,
		execCfg.RangeFeedFactory,
		s.Stopper(),
		execCfg.SystemTableIDResolver,
		pusher,
	)
	done := make(chan error, 1)
	go func() { done <- r.Reconcile(ctx) }()
	defer func() {
		cancel()
		<-done
	}()

	sqlDB.Exec(t, "CREATE RESOURCE GROUP first WITH cpu_weight = 100, max_cpu = true")
	testutils.SucceedsSoon(t, func() error {
		ups, _ := pusher.snapshot()
		for _, u := range ups {
			if u.Name == "first" && u.Config.CPUWeight == 100 && u.Config.MaxCPU {
				return nil
			}
		}
		return errors.Newf("upsert for 'first' not yet observed (have %d upserts)", len(ups))
	})

	sqlDB.Exec(t, "ALTER RESOURCE GROUP first WITH cpu_weight = 200")
	testutils.SucceedsSoon(t, func() error {
		ups, _ := pusher.snapshot()
		for _, u := range ups {
			if u.Name == "first" && u.Config.CPUWeight == 200 {
				return nil
			}
		}
		return errors.Newf("upsert with weight=200 not yet observed")
	})

	var firstID int64
	sqlDB.QueryRow(t, "SELECT id FROM system.resource_groups WHERE name = 'first'").Scan(&firstID)
	sqlDB.Exec(t, "DROP RESOURCE GROUP first")
	testutils.SucceedsSoon(t, func() error {
		_, dels := pusher.snapshot()
		for _, d := range dels {
			if d.Id == firstID {
				return nil
			}
		}
		return errors.Newf("delete for id=%d not yet observed", firstID)
	})
}
