// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ui

import (
	"time"
)

// ClusterInfo represents a roachprod cluster for API responses.
type ClusterInfo struct {
	Name            string    `json:"name"`
	Nodes           int       `json:"nodes"`
	Cloud           string    `json:"cloud"`
	LifetimeRemaining string  `json:"lifetimeRemaining"`
	Created         time.Time `json:"created"`
	Owner           string    `json:"owner"`
	MachineType     string    `json:"machineType,omitempty"`
	Region          string    `json:"region,omitempty"`
}

// CreateClusterRequest represents a request to create a new cluster.
type CreateClusterRequest struct {
	Name        string            `json:"name"`
	Nodes       int               `json:"nodes"`
	Cloud       string            `json:"cloud"`
	MachineType string            `json:"machineType,omitempty"`
	Lifetime    string            `json:"lifetime"` // Duration string like "12h"
	LocalSSD    bool              `json:"localSSD"`
	// Advanced options
	Arch        string            `json:"arch,omitempty"`
	Geo         bool              `json:"geo,omitempty"`
	Zones       []string          `json:"zones,omitempty"`
	Filesystem  string            `json:"filesystem,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
}

// CreateClusterResponse represents the response after creating a cluster.
type CreateClusterResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Cluster string `json:"cluster,omitempty"`
}

// ExtendClusterRequest represents a request to extend cluster lifetime.
type ExtendClusterRequest struct {
	Lifetime string `json:"lifetime"` // Duration string like "6h"
}

// DeleteClusterRequest represents a request to delete clusters.
type DeleteClusterRequest struct {
	Clusters []string `json:"clusters"`
}

// ErrorResponse represents an API error.
type ErrorResponse struct {
	Error   string `json:"error"`
	Details string `json:"details,omitempty"`
}

// ListClustersResponse represents the response for listing clusters.
type ListClustersResponse struct {
	Clusters []ClusterInfo `json:"clusters"`
	Total    int           `json:"total"`
}

// AWSSSOLoginResponse represents the response from AWS SSO login.
type AWSSSOLoginResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Output  string `json:"output,omitempty"`
}

// ClusterDetails represents detailed information about a cluster.
type ClusterDetails struct {
	Name      string   `json:"name"`
	Nodes     []string `json:"nodes"`      // IP addresses
	PGURLs    []string `json:"pgUrls"`     // PostgreSQL connection URLs
	AdminURLs []string `json:"adminUrls"`  // Admin UI URLs
}

// NodeStatus represents the status of a single node in a cluster.
type NodeStatus struct {
	NodeID  int    `json:"nodeId"`
	Running bool   `json:"running"`
	Version string `json:"version"`
	Pid     string `json:"pid"`
	Err     string `json:"err,omitempty"` // Error message if any
}

// CloudProviderOptions represents available options for a cloud provider.
type CloudProviderOptions struct {
	Zones        []string      `json:"zones"`
	MachineTypes []MachineType `json:"machineTypes"`
}

// MachineType represents a machine type with its specifications.
type MachineType struct {
	Name        string  `json:"name"`
	Description string  `json:"description"` // e.g., "4 vCPUs, 16 GB RAM"
	VCPUs       int     `json:"-"`           // Not sent to frontend, used for sorting
	MemoryGB    float64 `json:"-"`           // Not sent to frontend, used for sorting
}
