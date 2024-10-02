// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package democluster

import (
	"context"
	gosql "database/sql"

	democlusterapi "github.com/cockroachdb/cockroach/pkg/cli/democluster/api"
	"github.com/cockroachdb/cockroach/pkg/security/username"
)

// DemoCluster represents a demo cluster.
type DemoCluster interface {
	democlusterapi.DemoCluster

	// Start starts up the demo cluster.
	// The runInitialSQL function argument is applied to the first server
	// before the initialization completes.
	Start(
		ctx context.Context,
	) error

	// GetConnURL retrieves the connection URL to the first node.
	GetConnURL() string

	// GetSQLCredentials retrieves the authentication credentials to
	// establish SQL connections to the demo cluster.
	// (These are already embedded in the connection URL produced
	// by GetConnURL() however a client may wish to have them
	// available as discrete values.)
	GetSQLCredentials() (adminUser username.SQLUsername, adminPassword, certsDir string)

	// Close shuts down the demo cluster.
	Close(ctx context.Context)

	// EnableEnterprise enables enterprise features for this demo,
	// if available in this build. The returned callback should be called
	// before terminating the demo.
	EnableEnterprise(ctx context.Context) (func(), error)

	// SetupWorkload initializes the workload generator if defined.
	SetupWorkload(ctx context.Context) error

	// SetClusterSetting overrides a default cluster setting at system level
	// and for all tenants.
	SetClusterSetting(ctx context.Context, setting string, value interface{}) error

	// SetSimulatedLatency is used to enable or disable simulated latency.
	SetSimulatedLatency(on bool)

	// TenantName returns the tenant name that the default connection is for.
	TenantName() string
}

// EnableEnterprise is not implemented here in order to keep OSS/BSL builds successful.
// The cliccl package sets this function if enterprise features are available to demo.
var EnableEnterprise func(db *gosql.DB, org string) (func(), error)
