// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package democluster

import (
	"context"
	gosql "database/sql"
	"io"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// DemoCluster represents a demo cluster.
type DemoCluster interface {
	// Start starts up the demo cluster.
	// The runInitialSQL function argument is applied to the first server
	// before the initialization completes.
	Start(
		ctx context.Context,
		runInitialSQL func(ctx context.Context, s *server.Server, startSingleNode bool, adminUser, adminPassword string) error,
	) error

	// GetConnURL retrieves the connection URL to the first node.
	GetConnURL() string

	// GetSQLCredentials retrieves the authentication credentials to
	// establish SQL connections to the demo cluster.
	// (These are already embedded in the connection URL produced
	// by GetConnURL() however a client may wish to have them
	// available as discrete values.)
	GetSQLCredentials() (adminUser security.SQLUsername, adminPassword, certsDir string)

	// Close shuts down the demo cluster.
	Close(ctx context.Context)

	// AcquireDemoLicense acquires the demo license if configured,
	// otherwise does nothing. In any case, if there is no error, it
	// returns a channel that will either produce an error or a nil
	// value.
	AcquireDemoLicense(ctx context.Context) (chan error, error)

	// SetupWorkload initializes the workload generator if defined.
	SetupWorkload(ctx context.Context, licenseDone <-chan error) error

	// ListDemoNodes produces a listing of servers on the specified
	// writer. If justOne is specified, only the first node is listed.
	// Listing is printed to 'w'. Errors/wranings are printed to 'ew'.
	ListDemoNodes(w, ew io.Writer, justOne bool)

	// AddNode creates a new node with the given locality string.
	AddNode(ctx context.Context, localityString string) (newNodeID int32, err error)

	// GetLocality retrieves the locality of the given node.
	GetLocality(nodeID int32) string

	// NumNodes returns the number of nodes.
	NumNodes() int

	// DrainAndShutdown shuts down a node gracefully.
	DrainAndShutdown(ctx context.Context, nodeID int32) error

	// RestartNode starts the given node. The node must be down
	// prior to the call.
	RestartNode(ctx context.Context, nodeID int32) error

	// Decommission decommissions the given node.
	Decommission(ctx context.Context, nodeID int32) error

	// Recommission recommissions the given node.
	Recommission(ctx context.Context, nodeID int32) error
}

// GetAndApplyLicense is not implemented in order to keep OSS/BSL builds successful.
// The cliccl package sets this function if enterprise features are available to demo.
var GetAndApplyLicense func(dbConn *gosql.DB, clusterID uuid.UUID, org string) (bool, error)
