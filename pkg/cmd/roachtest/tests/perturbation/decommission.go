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

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// decommission will decommission the target node during the start phase. It
// allows optionally calling drain first. Draining first is the best practice
// recommendation, however it should not cause a latency impact either way.
type decommission struct {
	drain bool
}

var _ perturbation = decommission{}

func (d decommission) setup() variations {
	d.drain = true
	return setup(d, 5.0)
}

func (d decommission) setupMetamorphic(rng *rand.Rand) variations {
	v := d.setup()
	d.drain = rng.Intn(2) == 0
	v = v.randomize(rng)
	v.perturbation = d
	//TODO(#133606): With high vcpu and large writes, the test can fail due to
	//the disk becoming saturated leading to 1-2s of fsync stall.
	if v.vcpu >= 16 && v.blockSize == 4096 {
		v.blockSize = 1024
	}
	return v
}

func (d decommission) startTargetNode(ctx context.Context, t test.Test, v variations) {
	v.startNoBackup(ctx, t, v.targetNodes())
}

func (d decommission) startPerturbation(
	ctx context.Context, t test.Test, v variations,
) time.Duration {
	startTime := timeutil.Now()
	// TODO(baptist): If we want to support multiple decommissions in parallel,
	// run drain and decommission in separate goroutine.
	if d.drain {
		t.L().Printf("draining target nodes")
		for _, node := range v.targetNodes() {
			drainCmd := fmt.Sprintf(
				"./cockroach node drain --self --certs-dir=%s --port={pgport:%d}",
				install.CockroachNodeCertsDir,
				node,
			)
			v.Run(ctx, option.WithNodes(v.Node(node)), drainCmd)
		}
		// Wait for all the other nodes to see the drain over gossip.
		time.Sleep(10 * time.Second)
	}

	t.L().Printf("decommissioning nodes")
	for _, node := range v.targetNodes() {
		decommissionCmd := fmt.Sprintf(
			"./cockroach node decommission --self --certs-dir=%s --port={pgport:%d}",
			install.CockroachNodeCertsDir,
			node,
		)
		v.Run(ctx, option.WithNodes(v.Node(node)), decommissionCmd)
	}

	t.L().Printf("stopping decommissioned nodes")
	v.Stop(ctx, t.L(), option.DefaultStopOpts(), v.targetNodes())
	return timeutil.Since(startTime)
}

// endPerturbation already waited for completion as part of start, so it doesn't
// need to wait again here.
func (d decommission) endPerturbation(
	ctx context.Context, t test.Test, v variations,
) time.Duration {
	waitDuration(ctx, v.validationDuration)
	return v.validationDuration
}
