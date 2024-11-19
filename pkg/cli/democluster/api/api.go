// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package democlusterapi

import (
	"context"
	"io"
)

// DemoCluster represents the subset of the API of a demo cluster
// that is exposed to the SQL shell. It only contains the part
// of the API that do not create a strong dependency on CockroachDB's
// server package and machinery.
type DemoCluster interface {
	// ListDemoNodes produces a listing of servers on the specified
	// writer. If justOne is specified, only the first node is listed.
	// If verbose is specified, more URLs are printed.
	// Listing is printed to 'w'. Errors/warnings are printed to 'ew'.
	ListDemoNodes(w, ew io.Writer, justOne, verbose bool)

	// ExpandShortDemoURLs expands `demo://` in a string URLs to `postgres://...`.
	ExpandShortDemoURLs(string) string

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
}
