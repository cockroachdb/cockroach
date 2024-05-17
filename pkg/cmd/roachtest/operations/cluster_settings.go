// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

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
	timeSupplier := func() time.Time {
		return timeutil.Now()
	}
	ops := []clusterSettingOp{
		// Converts all leases to expiration. Tradeoff between lower throughput and higher availability.
		// Weekly cycle.
		{
			Name:      "kv.expiration_leases_only.enabled",
			Generator: timeBasedValues(timeSupplier, []string{"true", "false"}, 24*7*time.Minute),
			Owner:     registry.OwnerKV,
		},
		// When running multi-store with `--wal-failover=among-stores`, this configures
		// the threshold to trigger a fail-over to a secondary store’s WAL.
		// 20-minute cycle.
		{
			Name: "storage.wal_failover.unhealthy_op_threshold",
			Generator: timeBasedRandomValue(timeSupplier, 20*time.Minute, func(rng *rand.Rand) string {
				return fmt.Sprintf("%d", rng.Intn(246)+5)
			}),
			Owner: registry.OwnerStorage,
		},
	}
	sanitizeOpName := func(name string) string {
		return strings.ReplaceAll(name, ".", "_")
	}
	for _, op := range ops {
		r.AddOperation(registry.OperationSpec{
			Name:             "cluster-settings/scheduled/" + sanitizeOpName(op.Name),
			Owner:            op.Owner,
			Timeout:          5 * time.Minute,
			CompatibleClouds: registry.AllClouds,
			Dependencies:     []registry.OperationDependency{registry.OperationRequiresNodes},
			Run: func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
				return setClusterSetting(ctx, o, c, op)
			},
		})
	}
}
