// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package dbconsole provides Backend-For-Frontend (BFF) endpoints for DB Console.
//
// @title DB Console API
// @version 1.0.0
// @description BFF API for the CockroachDB DB Console
//
// @host localhost:8080
// @BasePath /api/v2/dbconsole
//
// @securityDefinitions.apikey ApiKeyAuth
// @in header
// @name X-Cockroach-API-Session
package dbconsole

import (
	"context"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/server/apiutil"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
)

// StatusAPI defines the status methods used by dbconsole.
type StatusAPI interface {
	NodesUI(ctx context.Context, req *serverpb.NodesRequest) (*serverpb.NodesResponseExternal, error)
}

// ApiV2DBConsole implements the DB Console BFF API endpoints.
type ApiV2DBConsole struct {
	Status StatusAPI
}

// NodeInfo contains summarized node information for the UI.
type NodeInfo struct {
	// NodeID is the unique identifier for the node.
	NodeID int32 `json:"node_id" example:"1"`
	// Address is the RPC address of the node.
	Address string `json:"address" example:"localhost:26257"`
	// SQLAddress is the SQL address of the node.
	SQLAddress string `json:"sql_address" example:"localhost:26257"`
	// StartedAt is the unix timestamp when the node was started.
	StartedAt int64 `json:"started_at"`
	// UpdatedAt is the unix timestamp when the node status was last updated.
	UpdatedAt int64 `json:"updated_at"`
	// Locality contains the locality information for the node.
	Locality map[string]string `json:"locality,omitempty"`
	// BuildTag is the build version tag of the node.
	BuildTag string `json:"build_tag" example:"v24.1.0"`
	// LivenessStatus is the liveness status of the node.
	LivenessStatus string `json:"liveness_status" example:"LIVE"`
	// TotalSystemMemory is the total RAM available to the system in bytes.
	TotalSystemMemory int64 `json:"total_system_memory"`
	// NumCpus is the number of logical CPUs.
	NumCpus int32 `json:"num_cpus"`
}

// NodesResponse contains all nodes for the cluster overview.
type NodesResponse struct {
	// Nodes contains status information for all nodes in the cluster.
	Nodes []NodeInfo `json:"nodes"`
}

// ErrorResponse represents an error response from the API.
type ErrorResponse struct {
	// Error is the error message.
	Error string `json:"error"`
}

// GetNodes returns node information for the cluster overview page.
// @Summary Get cluster nodes
// @Description Returns status information for all nodes in the cluster.
// @Tags Cluster
// @Produce json
// @Success 200 {object} NodesResponse "Node list"
// @Failure 500 {object} ErrorResponse "Internal error"
// @Security ApiKeyAuth
// @Router /nodes [get]
func (api *ApiV2DBConsole) GetNodes(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx = authserver.ForwardHTTPAuthInfoToRPCCalls(ctx, r)

	resp, err := api.Status.NodesUI(ctx, &serverpb.NodesRequest{})
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	result := NodesResponse{
		Nodes: make([]NodeInfo, 0, len(resp.Nodes)),
	}
	for _, n := range resp.Nodes {
		liveness := resp.LivenessByNodeID[n.Desc.NodeID]
		result.Nodes = append(result.Nodes, NodeInfo{
			NodeID:            int32(n.Desc.NodeID),
			Address:           n.Desc.Address.String(),
			SQLAddress:        n.Desc.SQLAddress.String(),
			StartedAt:         n.StartedAt,
			UpdatedAt:         n.UpdatedAt,
			Locality:          localityToMap(n.Desc.Locality),
			BuildTag:          n.BuildInfo.Tag,
			LivenessStatus:    livenessStatusToString(liveness),
			TotalSystemMemory: n.TotalSystemMemory,
			NumCpus:           n.NumCpus,
		})
	}

	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, result)
}

// localityToMap converts a serverpb.Locality to a map of key-value pairs.
func localityToMap(locality serverpb.Locality) map[string]string {
	if len(locality.Tiers) == 0 {
		return nil
	}
	result := make(map[string]string, len(locality.Tiers))
	for _, tier := range locality.Tiers {
		result[tier.Key] = tier.Value
	}
	return result
}

// livenessStatusToString converts a livenesspb.NodeLivenessStatus to its string
// representation.
func livenessStatusToString(status livenesspb.NodeLivenessStatus) string {
	switch status {
	case livenesspb.NodeLivenessStatus_UNKNOWN:
		return "UNKNOWN"
	case livenesspb.NodeLivenessStatus_DEAD:
		return "DEAD"
	case livenesspb.NodeLivenessStatus_UNAVAILABLE:
		return "UNAVAILABLE"
	case livenesspb.NodeLivenessStatus_LIVE:
		return "LIVE"
	case livenesspb.NodeLivenessStatus_DECOMMISSIONING:
		return "DECOMMISSIONING"
	case livenesspb.NodeLivenessStatus_DECOMMISSIONED:
		return "DECOMMISSIONED"
	case livenesspb.NodeLivenessStatus_DRAINING:
		return "DRAINING"
	default:
		return "UNKNOWN"
	}
}
