// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package perturbation

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/stretchr/testify/require"
)

type slowDisk struct {
	// slowLiveness will place the liveness range on the slow node (may not be the slow disk).
	slowLiveness bool
	// walFailover will add add WAL failover to the slow node.
	walFailover bool
	staller     roachtestutil.DiskStaller
}

// NB: slowData is an unusual perturbation since the staller is initialized
// later (in startTargetNode) instead of here.
var _ perturbation = &slowDisk{}

func (s *slowDisk) setup() variations {
	s.slowLiveness = true
	s.walFailover = true
	// With walFailover=true and 2 disks per node (the default for the full
	// variant), raft log writes fail over to the non-throttled store, so
	// foreground throughput is expected to stay close to baseline even
	// while the staller is active. Default thresholds apply to both
	// intervals; we keep the 1.25x floor (rather than tightening) only to
	// avoid flakes from the slowLiveness leg, which routes liveness
	// heartbeats through the slow disk. The metamorphic variant exercises
	// configurations where walFailover is off and throughput can drop
	// substantially -- those should override impact independently.
	return setup(s, defaultThresholds())
}

func (s *slowDisk) setupMetamorphic(rng *rand.Rand) variations {
	v := s.setup()
	s.slowLiveness = rng.Intn(2) == 0
	s.walFailover = rng.Intn(2) == 0
	v.perturbation = s
	v.specOptions = []spec.Option{spec.ReuseNone()}
	return v.randomize(rng)
}

// startTargetNode implements perturbation.
func (s *slowDisk) startTargetNode(ctx context.Context, t test.Test, v variations) {
	extraArgs := []string{}
	if s.walFailover && v.disks > 1 {
		extraArgs = append(extraArgs, "--wal-failover=among-stores")
	}
	v.startNoBackup(ctx, t, v.targetNodes(), extraArgs...)

	if s.slowLiveness {
		// TODO(baptist): Handle multiple target nodes.
		target := v.targetNodes()[0]
		db := v.Conn(ctx, t.L(), 1)
		defer db.Close()
		cmd := fmt.Sprintf(`ALTER RANGE liveness CONFIGURE ZONE USING CONSTRAINTS='{"+node%d":1}', lease_preferences='[[+node%d]]'`, target, target)
		_, err := db.ExecContext(ctx, cmd)
		require.NoError(t, err)
	}

	if v.IsLocal() {
		s.staller = roachtestutil.NoopDiskStaller{}
	} else {
		s.staller = roachtestutil.MakeCgroupDiskStaller(t, v, false /* readsToo */, false /* logsToo */, false)
		s.staller.Setup(ctx)
	}
}

// startPerturbation implements perturbation.
func (s *slowDisk) startPerturbation(ctx context.Context, t test.Test, v variations) time.Duration {
	// TODO(baptist): Do this more dynamically?
	s.staller.Slow(ctx, v.targetNodes(), 20_000_000)
	waitDuration(ctx, v.validationDuration)
	return v.validationDuration
}

// endPerturbation implements perturbation.
func (s *slowDisk) endPerturbation(ctx context.Context, t test.Test, v variations) time.Duration {
	if err := s.staller.Unstall(ctx, v.targetNodes()); err != nil {
		t.Fatal("failed to unstall disk:", err)
	}
	waitDuration(ctx, v.validationDuration)
	return v.validationDuration
}
