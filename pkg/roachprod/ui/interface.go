// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ui

import (
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

// ClusterManager defines the interface for cluster operations.
// This allows the UI package to avoid importing pkg/roachprod and breaking the dependency cycle.
type ClusterManager interface {
	// ListClusters returns information about all clusters or filtered by user
	ListClusters(ctx context.Context, userFilter string) ([]ClusterInfo, error)

	// CreateCluster creates a new cluster
	CreateCluster(ctx context.Context, req CreateClusterRequest) error

	// DeleteClusters deletes one or more clusters
	DeleteClusters(ctx context.Context, clusterNames []string) error

	// ExtendCluster extends a cluster's lifetime
	ExtendCluster(ctx context.Context, clusterName string, lifetime string) error

	// RefreshAWSSSO refreshes AWS SSO credentials and returns the CLI output
	RefreshAWSSSO(ctx context.Context) (string, error)

	// StageCluster stages cockroach binaries on all nodes
	StageCluster(ctx context.Context, clusterName string) error

	// StageLocalCluster stages local cockroach binary from ~/go/src/github.com/cockroachdb/cockroach/artifacts/cockroach
	StageLocalCluster(ctx context.Context, clusterName string) error

	// StartCluster starts cockroach on all nodes
	StartCluster(ctx context.Context, clusterName string) error

	// StopCluster stops cockroach on all nodes
	StopCluster(ctx context.Context, clusterName string) error

	// StopClusterNodes stops cockroach on specific nodes or all nodes
	StopClusterNodes(ctx context.Context, clusterName string, nodeSpec string) error

	// WipeCluster wipes all nodes of the cluster
	WipeCluster(ctx context.Context, clusterName string) error

	// GetClusterInfo returns detailed cluster information including IPs, URLs
	GetClusterInfo(ctx context.Context, clusterName string) (*ClusterDetails, error)

	// GetAdminURL returns the admin UI URL for the cluster
	GetAdminURL(ctx context.Context, clusterName string) (string, error)

	// GetClusterStatus returns the status of each node in the cluster
	GetClusterStatus(ctx context.Context, clusterName string) ([]NodeStatus, error)

	// GetCurrentUser returns the current OS username
	GetCurrentUser() (string, error)

	// StageWorkload stages the workload binary and sets up HAProxy on the workload node
	StageWorkload(ctx context.Context, clusterName string) error

	// StartWorkload initializes and starts the TPCC workload
	StartWorkload(ctx context.Context, clusterName string) error

	// StopWorkload stops the running workload
	StopWorkload(ctx context.Context, clusterName string) error
}

// ClusterOperations defines the interface for a synced cluster.
type ClusterOperations interface {
	NewSession(l *logger.Logger, node Node, cmd string, args []string) Session
	Nodes() []Node
}

// Node represents a cluster node.
type Node struct {
	Index int
}

// Session represents an SSH session.
type Session interface {
	RequestPty() error
	SetStdin(r io.Reader)
	SetStdout(w io.Writer)
	SetStderr(w io.Writer)
	Start() error
	Wait() error
	// Resize resizes the terminal to the specified dimensions.
	// Returns nil if resize is not supported.
	Resize(cols, rows int) error
}

// SetClusterManager sets the cluster manager implementation.
// This must be called before starting the UI server.
func (s *Server) SetClusterManager(cm ClusterManager) {
	s.clusterManager = cm
}

// SetNewCluster sets the NewCluster function.
// This must be called before starting the UI server.
func (s *Server) SetNewCluster(nc NewClusterFunc) {
	s.NewCluster = nc
}

// LogBroadcaster defines the interface for broadcasting logs to WebSocket clients.
type LogBroadcaster interface {
	Broadcast(message string)
}

// GetLogBroadcaster returns the server's log broadcaster.
func (s *Server) GetLogBroadcaster() LogBroadcaster {
	return s.logBroadcaster
}
