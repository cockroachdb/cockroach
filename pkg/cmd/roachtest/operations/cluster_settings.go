// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package operations

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type clusterSettingOp struct {
	Name      string
	Generator func() string
	Owner     registry.Owner
}

func timeBasedValue(
	timeSupplier func() time.Time, frequency time.Duration, valueForSegment func(int64) string,
) func() string {
	return func() string {
		segment := timeSupplier().Unix() / int64(frequency.Seconds())
		return valueForSegment(segment)
	}
}

// timeBasedValues returns a function that returns a value from the given list
// of values based on the current time and the given frequency.
func timeBasedValues(
	timeSupplier func() time.Time, values []string, frequency time.Duration,
) func() string {
	return timeBasedValue(timeSupplier, frequency, func(segment int64) string {
		idx := int(segment) % len(values)
		return values[idx]
	})
}

// timeBasedRandomValue returns a function that returns a random value based on
// the current time and the given frequency.
func timeBasedRandomValue(
	timeSupplier func() time.Time, frequency time.Duration, valueFromRNG func(*rand.Rand) string,
) func() string {
	return func() string {
		segment := timeSupplier().Unix() / int64(frequency.Seconds())
		return valueFromRNG(randutil.NewTestRandWithSeed(segment))
	}
}

func setClusterSetting(
	ctx context.Context, o operation.Operation, c cluster.Cluster, op clusterSettingOp,
) registry.OperationCleanup {
	value := op.Generator()
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	o.Status(fmt.Sprintf("setting cluster setting %s to %s", op.Name, value))
	_, err := conn.ExecContext(ctx, fmt.Sprintf("SET CLUSTER SETTING %s = '%s'", op.Name, value))
	if err != nil {
		o.Fatal(err)
	}
	return nil
}

func registerClusterSettings(r registry.Registry) {
	ops := []clusterSettingOp{
		// Converts all leases to expiration. Tradeoff between lower throughput and higher availability.
		// Weekly cycle.
		{
			Name:      "kv.expiration_leases_only.enabled",
			Generator: timeBasedValues(timeutil.Now, []string{"true", "false"}, 24*7*time.Hour),
			Owner:     registry.OwnerKV,
		},
		{
			// Change the fraction of replicas that run leader leases.
			Name: "kv.raft.leader_fortification.fraction_enabled",
			Generator: timeBasedRandomValue(timeutil.Now, 6*time.Hour, func(rng *rand.Rand) string {
				// Random value in the following set: {0, 0.25, 0.5, 0.75, 1.0}
				fraction := float32(rng.Intn(5)) / 4.0
				return fmt.Sprintf("%g", fraction)
			}),
			Owner: registry.OwnerKV,
		},
		// When running multi-store with `--wal-failover=among-stores`, this configures
		// the threshold to trigger a fail-over to a secondary store’s WAL.
		// 20-minute cycle.
		{
			Name: "storage.wal_failover.unhealthy_op_threshold",
			Generator: timeBasedRandomValue(timeutil.Now, 20*time.Minute, func(rng *rand.Rand) string {
				return fmt.Sprintf("%d", rng.Intn(246)+5)
			}),
			Owner: registry.OwnerStorage,
		},
		{
			// Observe if there is any unexpected impact with running the job periodically.
			Name:      "obs.tablemetadata.automatic_updates.enabled",
			Generator: timeBasedValues(timeutil.Now, []string{"true", "false"}, 12*time.Hour),
			Owner:     registry.OwnerObservability,
		},
	}
	sanitizeOpName := func(name string) string {
		return strings.ReplaceAll(name, ".", "_")
	}
	for _, op := range ops {
		r.AddOperation(registry.OperationSpec{
			Name:               "cluster-settings/scheduled/" + sanitizeOpName(op.Name),
			Owner:              op.Owner,
			Timeout:            5 * time.Minute,
			CompatibleClouds:   registry.AllClouds,
			CanRunConcurrently: registry.OperationCannotRunConcurrentlyWithItself,
			Dependencies:       []registry.OperationDependency{registry.OperationRequiresNodes},
			Run: func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
				return setClusterSetting(ctx, o, c, op)
			},
		})
	}
}
