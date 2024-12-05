// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package perturbation

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// addNode will add a node during the start phase and wait for it to complete.
// It doesn't do anything during the stop phase.
type addNode struct{}

var _ perturbation = addNode{}

func (a addNode) setup() variations {
	return setup(a, 5.0)
}

func (a addNode) setupMetamorphic(rng *rand.Rand) variations {
	v := a.setup()
	v = v.randomize(rng)
	//TODO(#133606): With high vcpu and large writes, the test can fail due to
	//the disk becoming saturated leading to 1-2s of fsync stall.
	if v.vcpu >= 16 && v.blockSize == 4096 {
		v.blockSize = 1024
	}
	return v
}

func (addNode) startTargetNode(ctx context.Context, t test.Test, v variations) {
}

func (a addNode) startPerturbation(ctx context.Context, t test.Test, v variations) time.Duration {
	startTime := timeutil.Now()
	v.startNoBackup(ctx, t, v.targetNodes())
	// Wait out the time until the store is no longer suspect. The 31s is based
	// on the 30s default server.time_after_store_suspect setting plus 1 sec for
	// the store to propagate its gossip information.
	waitDuration(ctx, 31*time.Second)
	v.waitForRebalanceToStop(ctx, t)
	return timeutil.Since(startTime)
}

// endPerturbation already waited for completion as part of start, so it doesn't
// need to wait again here.
func (addNode) endPerturbation(ctx context.Context, t test.Test, v variations) time.Duration {
	waitDuration(ctx, v.validationDuration)
	return v.validationDuration
}
