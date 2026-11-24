// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dbconsole

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type mockStatusAPI struct {
	resp *serverpb.NodesResponseExternal
	err  error
}

func (m *mockStatusAPI) NodesUI(
	_ context.Context, _ *serverpb.NodesRequest,
) (*serverpb.NodesResponseExternal, error) {
	return m.resp, m.err
}

func TestGetNodes(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mock := &mockStatusAPI{
			resp: &serverpb.NodesResponseExternal{
				Nodes: []serverpb.NodeResponse{
					{
						Desc: serverpb.NodeDescriptor{
							NodeID:  1,
							Address: util.MakeUnresolvedAddr("tcp", "10.0.0.1:26257"),
							SQLAddress: util.MakeUnresolvedAddr(
								"tcp", "10.0.0.1:26258",
							),
							Locality: serverpb.Locality{
								Tiers: []serverpb.Tier{
									{Key: "region", Value: "us-east-1"},
									{Key: "zone", Value: "us-east-1a"},
								},
							},
						},
						BuildInfo:         build.Info{Tag: "v24.1.0"},
						StartedAt:         1000,
						UpdatedAt:         2000,
						TotalSystemMemory: 8589934592,
						NumCpus:           4,
					},
					{
						Desc: serverpb.NodeDescriptor{
							NodeID:     2,
							Address:    util.MakeUnresolvedAddr("tcp", "10.0.0.2:26257"),
							SQLAddress: util.MakeUnresolvedAddr("tcp", "10.0.0.2:26258"),
						},
						BuildInfo:         build.Info{Tag: "v24.1.0"},
						StartedAt:         1100,
						UpdatedAt:         2100,
						TotalSystemMemory: 17179869184,
						NumCpus:           8,
					},
				},
				LivenessByNodeID: map[roachpb.NodeID]livenesspb.NodeLivenessStatus{
					1: livenesspb.NodeLivenessStatus_LIVE,
					2: livenesspb.NodeLivenessStatus_DEAD,
				},
			},
		}

		api := &ApiV2DBConsole{Status: mock}
		req := httptest.NewRequest("GET", "/api/v2/dbconsole/nodes", nil)
		w := httptest.NewRecorder()
		api.GetNodes(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var result NodesResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		require.Len(t, result.Nodes, 2)

		// First node.
		n := result.Nodes[0]
		require.Equal(t, int32(1), n.NodeID)
		require.Equal(t, "10.0.0.1:26257", n.Address)
		require.Equal(t, "10.0.0.1:26258", n.SQLAddress)
		require.Equal(t, int64(1000), n.StartedAt)
		require.Equal(t, int64(2000), n.UpdatedAt)
		require.Equal(t, "v24.1.0", n.BuildTag)
		require.Equal(t, "LIVE", n.LivenessStatus)
		require.Equal(t, int64(8589934592), n.TotalSystemMemory)
		require.Equal(t, int32(4), n.NumCpus)
		require.Equal(t, map[string]string{
			"region": "us-east-1",
			"zone":   "us-east-1a",
		}, n.Locality)

		// Second node: no locality, dead.
		n = result.Nodes[1]
		require.Equal(t, int32(2), n.NodeID)
		require.Equal(t, "DEAD", n.LivenessStatus)
		require.Nil(t, n.Locality)
	})

	t.Run("error", func(t *testing.T) {
		mock := &mockStatusAPI{
			err: errors.New("connection refused"),
		}

		api := &ApiV2DBConsole{Status: mock}
		req := httptest.NewRequest("GET", "/api/v2/dbconsole/nodes", nil)
		w := httptest.NewRecorder()
		api.GetNodes(w, req)

		require.Equal(t, http.StatusInternalServerError, w.Code)
	})

	t.Run("empty cluster", func(t *testing.T) {
		mock := &mockStatusAPI{
			resp: &serverpb.NodesResponseExternal{
				Nodes:            []serverpb.NodeResponse{},
				LivenessByNodeID: map[roachpb.NodeID]livenesspb.NodeLivenessStatus{},
			},
		}

		api := &ApiV2DBConsole{Status: mock}
		req := httptest.NewRequest("GET", "/api/v2/dbconsole/nodes", nil)
		w := httptest.NewRecorder()
		api.GetNodes(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var result NodesResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		require.Empty(t, result.Nodes)
	})
}

func TestLivenessStatusToString(t *testing.T) {
	tests := []struct {
		status livenesspb.NodeLivenessStatus
		want   string
	}{
		{livenesspb.NodeLivenessStatus_UNKNOWN, "UNKNOWN"},
		{livenesspb.NodeLivenessStatus_DEAD, "DEAD"},
		{livenesspb.NodeLivenessStatus_UNAVAILABLE, "UNAVAILABLE"},
		{livenesspb.NodeLivenessStatus_LIVE, "LIVE"},
		{livenesspb.NodeLivenessStatus_DECOMMISSIONING, "DECOMMISSIONING"},
		{livenesspb.NodeLivenessStatus_DECOMMISSIONED, "DECOMMISSIONED"},
		{livenesspb.NodeLivenessStatus_DRAINING, "DRAINING"},
		{livenesspb.NodeLivenessStatus(999), "UNKNOWN"},
	}

	for _, tt := range tests {
		require.Equal(t, tt.want, livenessStatusToString(tt.status))
	}
}

func TestLocalityToMap(t *testing.T) {
	t.Run("with tiers", func(t *testing.T) {
		locality := serverpb.Locality{
			Tiers: []serverpb.Tier{
				{Key: "region", Value: "us-east-1"},
				{Key: "zone", Value: "us-east-1a"},
			},
		}
		result := localityToMap(locality)
		require.Equal(t, map[string]string{
			"region": "us-east-1",
			"zone":   "us-east-1a",
		}, result)
	})

	t.Run("empty", func(t *testing.T) {
		result := localityToMap(serverpb.Locality{})
		require.Nil(t, result)
	})
}
