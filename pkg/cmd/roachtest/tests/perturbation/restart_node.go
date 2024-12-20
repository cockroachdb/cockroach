// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package perturbation

import (
	"context"
	"math"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// restart will gracefully stop and then restart a node after a custom duration.
type restart struct {
	cleanRestart bool
}

var _ perturbation = restart{}

func (r restart) setup() variations {
	r.cleanRestart = true
	v := setup(r, math.Inf(1))

	// TODO(baptist): Remove this setting once #120073 is fixed.
	v.clusterSettings["kv.lease.reject_on_leader_unknown.enabled"] = "true"

	// NB: Prevent replicas from being removed from the store that is down. We
	// could consider making the dead time a variation so some tests will move
	// some of the replicas.
	v.clusterSettings["server.time_until_store_dead"] = (v.perturbationDuration + time.Minute).String()

	// TODO(kvoli,andrewbaptist): Re-introduce a lower than default suspect
	// duration once RACv2 pull mode (send queue) is enabled.
	// v.clusterSettings["server.time_after_store_suspect"] = (10 * time.Second).String()

	return v
}

func (r restart) setupMetamorphic(rng *rand.Rand) variations {
	v := r.setup()
	r.cleanRestart = rng.Intn(2) == 0
	v.perturbation = r
	v = v.randomize(rng)
	// TODO(#137666): The restart test can cause OOM with low memory
	// configurations.
	if v.mem == spec.Low {
		v.mem = spec.Standard
	}
	return v
}

func (r restart) startTargetNode(ctx context.Context, t test.Test, v variations) {
	v.startNoBackup(ctx, t, v.targetNodes())
}

// startPerturbation stops the target node with a graceful shutdown.
func (r restart) startPerturbation(ctx context.Context, t test.Test, v variations) time.Duration {
	startTime := timeutil.Now()
	gracefulOpts := option.DefaultStopOpts()
	// SIGTERM for clean shutdown
	if r.cleanRestart {
		gracefulOpts.RoachprodOpts.Sig = 15
	} else {
		gracefulOpts.RoachprodOpts.Sig = 9
	}
	gracefulOpts.RoachprodOpts.Wait = true
	v.Stop(ctx, t.L(), gracefulOpts, v.targetNodes())
	waitDuration(ctx, v.perturbationDuration)
	if r.cleanRestart {
		return timeutil.Since(startTime)
	}
	// If it is not a clean restart, we ignore the first 10 seconds to allow for lease movement.
	return timeutil.Since(startTime) + 10*time.Second
}

// endPerturbation restarts the node.
func (r restart) endPerturbation(ctx context.Context, t test.Test, v variations) time.Duration {
	startTime := timeutil.Now()
	v.startNoBackup(ctx, t, v.targetNodes())
	waitDuration(ctx, v.validationDuration)
	return timeutil.Since(startTime)
}
