// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package perturbation

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// elasticWorkload will start a workload with elastic priority. It uses the same
// characteristics as the normal workload. However since the normal workload
// runs at 50% CPU this adds another 2x the stable rate so it will be slowed
// down by AC.
// TODO(baptist): Run against the same database to hit transaction conflicts and
// priority inversions.
type elasticWorkload struct{}

var _ perturbation = elasticWorkload{}

func (e elasticWorkload) setup() variations {
	// TODO(#137835): Determine why the impact is so high and reduce this once
	// it is addressed.
	return setup(e, math.Inf(1))
}

func (e elasticWorkload) setupMetamorphic(rng *rand.Rand) variations {
	v := e.setup()
	// NB: Running an elastic workload can sometimes increase the latency of
	// almost all regular requests. To prevent this, we set the min latency to
	// 100ms instead of the default.
	v.profileOptions = append(v.profileOptions, roachtestutil.ProfMinimumLatency(100*time.Millisecond))
	v = v.randomize(rng)
	// TODO(#134668): Remove this once this test passes with a longer perturbation duration.
	if v.perturbationDuration > 10*time.Minute {
		v.perturbationDuration = 10 * time.Minute
	}
	return v
}

func (e elasticWorkload) startTargetNode(ctx context.Context, t test.Test, v variations) {
	v.startNoBackup(ctx, t, v.targetNodes())
	initCmd := fmt.Sprintf("./cockroach workload init kv --db elastic --splits %d {pgurl:1}", v.splits)
	v.Run(ctx, option.WithNodes(v.Node(1)), initCmd)
}

func (e elasticWorkload) startPerturbation(
	ctx context.Context, t test.Test, v variations,
) time.Duration {
	startTime := timeutil.Now()
	runCmd := fmt.Sprintf(
		"./cockroach workload run kv --db elastic --txn-qos=background --duration=%s --max-block-bytes=%d --min-block-bytes=%d --concurrency=500 {pgurl%s}",
		v.perturbationDuration, v.blockSize, v.blockSize, v.stableNodes())
	v.Run(ctx, option.WithNodes(v.workloadNodes()), runCmd)

	// Wait a few seconds to allow the latency to resume after stopping the
	// workload. This makes it easier to separate the perturbation from the
	// validation phases.
	waitDuration(ctx, 5*time.Second)
	return timeutil.Since(startTime)
}

// endPerturbation implements perturbation.
func (e elasticWorkload) endPerturbation(
	ctx context.Context, t test.Test, v variations,
) time.Duration {
	waitDuration(ctx, v.validationDuration)
	return v.validationDuration
}
