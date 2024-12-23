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

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// partition will partition the target node from either one other node or all
// other nodes in a different AZ.
type partition struct {
	partitionSite bool
}

var _ perturbation = partition{}

func (p partition) setup() variations {
	p.partitionSite = true
	v := setup(p, math.Inf(1))
	v.leaseType = registry.ExpirationLeases
	// TODO(baptist): Remove this setting once #120073 is fixed.
	v.clusterSettings["kv.lease.reject_on_leader_unknown.enabled"] = "true"
	return v
}

func (p partition) setupMetamorphic(rng *rand.Rand) variations {
	v := p.setup()
	p.partitionSite = rng.Intn(2) == 0
	v.perturbation = p
	v = v.randomize(rng)
	// TODO(#137666): The partition test can cause OOM with low memory
	// configurations.
	if v.mem == spec.Low {
		v.mem = spec.Standard
	}
	return v
}

func (p partition) startTargetNode(ctx context.Context, t test.Test, v variations) {
	v.startNoBackup(ctx, t, v.targetNodes())
}

func (p partition) startPerturbation(ctx context.Context, t test.Test, v variations) time.Duration {
	targetIPs, err := v.InternalIP(ctx, t.L(), v.targetNodes())
	require.NoError(t, err)

	if !v.IsLocal() {
		v.Run(
			ctx,
			v.withPartitionedNodes(v, p.partitionSite),
			fmt.Sprintf(
				`sudo iptables -A INPUT -p tcp -s %s -j DROP`, targetIPs[0]))
	}
	waitDuration(ctx, v.perturbationDuration)
	// During the first 10 seconds after the partition, we expect latency to drop,
	// start measuring after 20 seconds.
	return v.perturbationDuration - 20*time.Second
}

func (p partition) endPerturbation(ctx context.Context, t test.Test, v variations) time.Duration {
	startTime := timeutil.Now()
	if !v.IsLocal() {
		v.Run(ctx, v.withPartitionedNodes(v, p.partitionSite), `sudo iptables -F`)
	}
	waitDuration(ctx, v.validationDuration)
	return timeutil.Since(startTime)
}

// partition either the first node or all nodes in the first region.
func (v variations) withPartitionedNodes(c cluster.Cluster, partitionSite bool) install.RunOptions {
	numPartitionNodes := 1
	if partitionSite {
		numPartitionNodes = v.numNodes / NUM_REGIONS
	}
	return option.WithNodes(c.Range(1, numPartitionNodes))
}
