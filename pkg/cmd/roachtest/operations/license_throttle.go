// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package operations

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/licenseccl"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type cleanupLicenseRemoval struct {
	license string
}

func (c cleanupLicenseRemoval) Cleanup(
	ctx context.Context, o operation.Operation, cl cluster.Cluster,
) {
	conn := cl.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	o.Status(fmt.Sprintf("restoring original license: %q", c.license))
	_, err := conn.ExecContext(ctx, fmt.Sprintf("SET CLUSTER SETTING enterprise.license = '%s'", c.license))
	if err != nil {
		o.Fatal(err)
	}
}

func runAddExpiredLicenseToTriggerThrottle(
	ctx context.Context, o operation.Operation, cl cluster.Cluster,
) registry.OperationCleanup {
	conn := cl.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	rows, err := conn.QueryContext(ctx, "SHOW CLUSTER SETTING enterprise.license")
	if err != nil {
		o.Fatal(err)
	}
	if !rows.Next() {
		o.Fatal("now rows in 'SHOW CLUSTER SETTING enterprise.license'")
	}

	// Save off the current license so that we can reinstate it during cleanup.
	cleanup := cleanupLicenseRemoval{}
	if err := rows.Scan(&cleanup.license); err != nil {
		o.Fatal(err)
	}

	// Generate an expired license to trigger throttling once installed.
	newLicense, err := (&licenseccl.License{
		Type:              licenseccl.License_Free,
		ValidUntilUnixSec: timeutil.Unix(771890400, 0).Unix(), // expired on 6/17/94
	}).Encode()
	if err != nil {
		o.Fatal(err)
	}

	// Change the current license. This will cause the server to be in violation of
	// the license enforcement policies. Connection throttling, no more than 5 concurrent
	// transactions opened, will be in effect.
	o.Status(fmt.Sprintf("updating license from %q to %q", cleanup.license, newLicense))
	_, err = conn.ExecContext(ctx, fmt.Sprintf("SET CLUSTER SETTING enterprise.license = '%s'", newLicense))
	if err != nil {
		o.Fatal(err)
	}

	return cleanup
}

// registryLicenseThrottle triggers license enforcement throttling by temporarily
// installing an expired license. The expired license will cause the enforcer
// to throttle connections. During cleanup, the original license is restored to
// disable throttling.
func registerLicenseThrottle(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "license-throttle",
		Owner:              registry.OwnerSQLFoundations,
		Timeout:            5 * time.Minute,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCannotRunConcurrentlyWithItself,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresNodes},
		Run:                runAddExpiredLicenseToTriggerThrottle,
	})
}
