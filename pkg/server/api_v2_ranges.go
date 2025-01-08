// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/apiutil"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/redact"
	"github.com/gorilla/mux"
)

// Status about a node.
type nodeStatus struct {
	// NodeID is the integer ID of this node.
	NodeID int32 `json:"node_id"`
	// Address is the unresolved network listen address of this node.
	Address  util.UnresolvedAddr `json:"address"`
	Attrs    roachpb.Attributes  `json:"attrs"`
	Locality roachpb.Locality    `json:"locality"`
	// ServerVersion is the exact version of Cockroach this node is running.
	ServerVersion roachpb.Version `json:"ServerVersion"`
	// BuildTag is an internal build marker.
	BuildTag string `json:"build_tag"`
	// StartedAt is the time when this node was started, expressed as
	// nanoseconds since Unix epoch.
	StartedAt int64 `json:"started_at"`
	// ClusterName is the string name of this cluster, if set.
	ClusterName string `json:"cluster_name"`
	// SQLAddress is the listen address to which SQL clients can connect.
	SQLAddress util.UnresolvedAddr `json:"sql_address"`

	// Metrics contain the last sampled metrics for this node.
	Metrics map[string]float64 `json:"metrics,omitempty"`
	// StoreMetrics contain the last sampled store metrics for this node.
	StoreMetrics map[roachpb.StoreID]map[string]float64 `json:"store_metrics,omitempty"`
	// TotalSystemMemory is the total amount of available system memory on this
	// node (or cgroup), in bytes.
	TotalSystemMemory int64 `json:"total_system_memory,omitempty"`
	// NumCpus is the number of CPUs on this node.
	NumCpus int32 `json:"num_cpus,omitempty"`
	// UpdatedAt is the time at which the node status record was last updated,
	// in nanoseconds since Unix epoch.
	UpdatedAt int64 `json:"updated_at,omitempty"`

	// LivenessStatus is the status of the node from the perspective of the
	// liveness subsystem. For internal use only.
	LivenessStatus int32 `json:"liveness_status"`
}

// Response struct for listNodes.
type nodesResponse struct {
	// Status of nodes.
	Nodes []nodeStatus `json:"nodes"`
	// Continuation offset for the next paginated call, if more values are present.
	// Specify as the `offset` parameter.
	Next int `json:"next,omitempty"`
}

// # List nodes
//
// List all nodes on this cluster.
//
// Client must be logged-in as a user with admin privileges.
//
// ---
// parameters:
//   - name: limit
//     type: integer
//     in: query
//     description: Maximum number of results to return in this call.
//     required: false
//   - name: offset
//     type: integer
//     in: query
//     description: Continuation offset for results after a past limited run.
//     required: false
//
// produces:
// - application/json
// security:
// - api_session: []
// responses:
//
//	"200":
//	  description: List nodes response.
//	  schema:
//	    "$ref": "#/definitions/nodesResponse"
func (a *apiV2SystemServer) listNodes(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	limit, offset := getSimplePaginationValues(r)
	ctx = authserver.ForwardHTTPAuthInfoToRPCCalls(ctx, r)

	nodes, next, err := a.systemStatus.nodesHelper(ctx, limit, offset)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}
	var resp nodesResponse
	resp.Next = next
	for _, n := range nodes.Nodes {
		storeMetrics := make(map[roachpb.StoreID]map[string]float64)
		for _, ss := range n.StoreStatuses {
			storeMetrics[ss.Desc.StoreID] = ss.Metrics
		}
		resp.Nodes = append(resp.Nodes, nodeStatus{
			NodeID:            int32(n.Desc.NodeID),
			Address:           n.Desc.Address,
			Attrs:             n.Desc.Attrs,
			Locality:          n.Desc.Locality,
			ServerVersion:     n.Desc.ServerVersion,
			BuildTag:          n.Desc.BuildTag,
			StartedAt:         n.Desc.StartedAt,
			ClusterName:       n.Desc.ClusterName,
			SQLAddress:        n.Desc.SQLAddress,
			Metrics:           n.Metrics,
			StoreMetrics:      storeMetrics,
			TotalSystemMemory: n.TotalSystemMemory,
			NumCpus:           n.NumCpus,
			UpdatedAt:         n.UpdatedAt,
			LivenessStatus:    int32(nodes.LivenessByNodeID[n.Desc.NodeID]),
		})
	}
	apiutil.WriteJSONResponse(ctx, w, 200, resp)
}

func (a *apiV2Server) listNodes(w http.ResponseWriter, r *http.Request) {
	apiutil.WriteJSONResponse(r.Context(), w, http.StatusNotImplemented, nil)
}

func parseRangeIDs(input string, w http.ResponseWriter) (ranges []roachpb.RangeID, ok bool) {
	if len(input) == 0 {
		return nil, true
	}
	for _, reqRange := range strings.Split(input, ",") {
		rangeID, err := strconv.ParseInt(reqRange, 10, 64)
		if err != nil {
			http.Error(w, "invalid range ID", http.StatusBadRequest)
			return nil, false
		}

		ranges = append(ranges, roachpb.RangeID(rangeID))
	}
	return ranges, true
}

type nodeRangeResponse struct {
	RangeInfo rangeInfo `json:"range_info"`
	Error     string    `json:"error,omitempty"`
}

type rangeResponse struct {
	Responses map[string]nodeRangeResponse `json:"responses_by_node_id"`
}

// # Get info about a range
//
// Retrieves more information about a specific range.
//
// Client must be logged-in as a user with admin privileges.
//
// ---
// parameters:
//   - name: range_id
//     in: path
//     type: integer
//     required: true
//
// produces:
// - application/json
// security:
// - api_session: []
// responses:
//
//	"200":
//	  description: List range response
//	  schema:
//	    "$ref": "#/definitions/rangeResponse"
func (a *apiV2Server) listRange(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx = authserver.ForwardHTTPAuthInfoToRPCCalls(ctx, r)
	vars := mux.Vars(r)
	rangeID, err := strconv.ParseInt(vars["range_id"], 10, 64)
	if err != nil {
		http.Error(w, "invalid range ID", http.StatusBadRequest)
		return
	}

	response := &rangeResponse{
		Responses: make(map[string]nodeRangeResponse),
	}

	rangesRequest := &serverpb.RangesRequest{
		RangeIDs: []roachpb.RangeID{roachpb.RangeID(rangeID)},
	}

	nodeFn := func(ctx context.Context, status serverpb.StatusClient, _ roachpb.NodeID) (interface{}, error) {
		return status.Ranges(ctx, rangesRequest)
	}
	responseFn := func(nodeID roachpb.NodeID, resp interface{}) {
		rangesResp := resp.(*serverpb.RangesResponse)
		// Age the MVCCStats to a consistent current timestamp. An age that is
		// not up to date is less useful.
		if len(rangesResp.Ranges) == 0 {
			return
		}
		var ri rangeInfo
		ri.init(rangesResp.Ranges[0])
		response.Responses[nodeID.String()] = nodeRangeResponse{RangeInfo: ri}
	}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		response.Responses[nodeID.String()] = nodeRangeResponse{
			Error: err.Error(),
		}
	}

	if err := iterateNodes(
		ctx,
		a.status.serverIterator,
		a.status.stopper,
		redact.Sprintf("details about range %d", rangeID),
		noTimeout,
		a.status.dialNode,
		nodeFn,
		responseFn, errorFn,
	); err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}
	apiutil.WriteJSONResponse(ctx, w, 200, response)
}

// rangeDescriptorInfo contains a subset of fields from the Cockroach-internal
// range descriptor that are safe to be returned from APIs.
type rangeDescriptorInfo struct {
	// RangeID is the integer id of this range.
	RangeID int64 `json:"range_id"`
	// StartKey is the resolved Cockroach-internal key that denotes the start of
	// this range.
	StartKey []byte `json:"start_key,omitempty"`
	// EndKey is the resolved Cockroach-internal key that denotes the end of
	// this range.
	EndKey []byte `json:"end_key,omitempty"`

	// StoreID is the ID of the store this hot range is on. Only set for hot
	// ranges.
	StoreID int32 `json:"store_id"`
	// QueriesPerSecond is the number of queries per second this range is
	// serving. Only set for hot ranges.
	QueriesPerSecond float64 `json:"queries_per_second"`
}

func (r *rangeDescriptorInfo) init(rd *roachpb.RangeDescriptor) {
	if rd == nil {
		*r = rangeDescriptorInfo{}
		return
	}
	*r = rangeDescriptorInfo{
		RangeID:  int64(rd.RangeID),
		StartKey: rd.StartKey,
		EndKey:   rd.EndKey,
	}
}

// Info related to a range.
type rangeInfo struct {
	Desc rangeDescriptorInfo `json:"desc"`

	// Span is the pretty-ified start/end key span for this range.
	Span serverpb.PrettySpan `json:"span"`
	// SourceNodeID is the ID of the node where this range info was retrieved
	// from.
	SourceNodeID int32 `json:"source_node_id,omitempty"`
	// SourceStoreID is the ID of the store on the node where this range info was
	// retrieved from.
	SourceStoreID int32 `json:"source_store_id,omitempty"`
	// ErrorMessage is any error retrieved from the internal range info. For
	// internal use only.
	ErrorMessage string `json:"error_message,omitempty"`
	// LeaseHistory is for internal use only.
	LeaseHistory []roachpb.Lease `json:"lease_history"`
	// Problems is a map of any issues reported by this range. For internal use
	// only.
	Problems serverpb.RangeProblems `json:"problems"`
	// Stats is for internal use only.
	Stats serverpb.RangeStatistics `json:"stats"`
	// Quiescent is for internal use only.
	Quiescent bool `json:"quiescent,omitempty"`
	// Ticking is for internal use only.
	Ticking bool `json:"ticking,omitempty"`
}

func (ri *rangeInfo) init(r serverpb.RangeInfo) {
	*ri = rangeInfo{
		Span:          r.Span,
		SourceNodeID:  int32(r.SourceNodeID),
		SourceStoreID: int32(r.SourceStoreID),
		ErrorMessage:  r.ErrorMessage,
		LeaseHistory:  r.LeaseHistory,
		Problems:      r.Problems,
		Stats:         r.Stats,
		Quiescent:     r.Quiescent,
		Ticking:       r.Ticking,
	}
	ri.Desc.init(r.State.Desc)
}

// Response struct for listNodeRanges.
type nodeRangesResponse struct {
	// Info about retrieved ranges.
	Ranges []rangeInfo `json:"ranges"`
	// Continuation token for the next limited run. Use in the `offset` parameter.
	Next int `json:"next,omitempty"`
}

// # List ranges on a node
//
// Lists information about ranges on a specified node. If a list of range IDs
// is specified, only information about those ranges is returned.
//
// Client must be logged-in as a user with admin privileges.
//
// ---
// parameters:
//   - name: node_id
//     in: path
//     type: integer
//     description: ID of node to query, or `local` for local node.
//     required: true
//   - name: ranges
//     in: query
//     type: array
//     required: false
//     description: IDs of ranges to return information for. All ranges returned
//     if unspecified.
//     items:
//     type: integer
//   - name: limit
//     type: integer
//     in: query
//     description: Maximum number of results to return in this call.
//     required: false
//   - name: offset
//     type: integer
//     in: query
//     description: Continuation offset for results after a past limited run.
//     required: false
//
// produces:
// - application/json
// security:
// - api_session: []
// responses:
//
//	"200":
//	  description: Node ranges response.
//	  schema:
//	    "$ref": "#/definitions/nodeRangesResponse"
func (a *apiV2SystemServer) listNodeRanges(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx = authserver.ForwardHTTPAuthInfoToRPCCalls(ctx, r)
	vars := mux.Vars(r)
	nodeIDStr := vars["node_id"]
	if nodeIDStr != "local" {
		nodeID, err := strconv.ParseInt(nodeIDStr, 10, 32)
		if err != nil || nodeID <= 0 {
			http.Error(w, "invalid node ID", http.StatusBadRequest)
			return
		}
	}

	ranges, ok := parseRangeIDs(r.URL.Query().Get("ranges"), w)
	if !ok {
		return
	}
	req := &serverpb.RangesRequest{
		NodeId:   nodeIDStr,
		RangeIDs: ranges,
	}
	limit, offset := getSimplePaginationValues(r)
	statusResp, next, err := a.systemStatus.rangesHelper(ctx, req, limit, offset)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}
	resp := nodeRangesResponse{
		Ranges: make([]rangeInfo, 0, len(statusResp.Ranges)),
		Next:   next,
	}
	for _, r := range statusResp.Ranges {
		var ri rangeInfo
		ri.init(r)
		resp.Ranges = append(resp.Ranges, ri)
	}
	apiutil.WriteJSONResponse(ctx, w, 200, resp)
}

func (a *apiV2Server) listNodeRanges(w http.ResponseWriter, r *http.Request) {
	apiutil.WriteJSONResponse(r.Context(), w, http.StatusNotImplemented, nil)
}

type responseError struct {
	ErrorMessage string         `json:"error_message"`
	NodeID       roachpb.NodeID `json:"node_id,omitempty"`
}

// Response struct for listHotRanges.
type hotRangesResponse struct {
	Ranges []hotRangeInfo  `json:"ranges"`
	Errors []responseError `json:"response_error,omitempty"`
	// Continuation token for the next paginated call. Use as the `start`
	// parameter.
	Next string `json:"next,omitempty"`
}

// Hot range details struct describes common information about hot range,
// (ie its range ID, QPS, table name, etc.).
type hotRangeInfo struct {
	RangeID             roachpb.RangeID  `json:"range_id"`
	NodeID              roachpb.NodeID   `json:"node_id"`
	QPS                 float64          `json:"qps"`
	WritesPerSecond     float64          `json:"writes_per_second"`
	ReadsPerSecond      float64          `json:"reads_per_second"`
	WriteBytesPerSecond float64          `json:"write_bytes_per_second"`
	ReadBytesPerSecond  float64          `json:"read_bytes_per_second"`
	CPUTimePerSecond    float64          `json:"cpu_time_per_second"`
	LeaseholderNodeID   roachpb.NodeID   `json:"leaseholder_node_id"`
	Databases           []string         `json:"databases"`
	Tables              []string         `json:"tables"`
	Indexes             []string         `json:"indexes"`
	SchemaName          string           `json:"schema_name"`
	ReplicaNodeIDs      []roachpb.NodeID `json:"replica_node_ids"`
	StoreID             roachpb.StoreID  `json:"store_id"`
}

// # List hot ranges
//
// Lists information about hot ranges. If a list of range IDs
// is specified, only information about those ranges is returned.
//
// Client must be logged-in as a user with admin privileges.
//
// ---
// parameters:
//   - name: node_id
//     in: query
//     type: integer
//     description: ID of node to query, or `local` for local node. If
//     unspecified, all nodes are queried.
//     required: false
//   - name: limit
//     type: integer
//     in: query
//     description: Maximum number of results to return in this call.
//     required: false
//   - name: start
//     type: string
//     in: query
//     description: Continuation token for results after a past limited run.
//     required: false
//
// produces:
// - application/json
// security:
// - api_session: []
// responses:
//
//	"200":
//	  description: Hot ranges response.
//	  schema:
//	    "$ref": "#/definitions/hotRangesResponse"
func (a *apiV2Server) listHotRanges(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx = authserver.ForwardHTTPAuthInfoToRPCCalls(ctx, r)
	nodeIDStr := r.URL.Query().Get("node_id")
	limit, start := getRPCPaginationValues(r)

	response := &hotRangesResponse{}
	var requestedNodes []roachpb.NodeID
	if len(nodeIDStr) > 0 {
		requestedNodeID, _, err := a.status.parseNodeID(nodeIDStr)
		if err != nil {
			http.Error(w, "invalid node ID", http.StatusBadRequest)
			return
		}
		requestedNodes = []roachpb.NodeID{requestedNodeID}
	}

	remoteRequest := serverpb.HotRangesRequest{NodeID: "local"}
	nodeFn := func(ctx context.Context, status serverpb.StatusClient, nodeID roachpb.NodeID) ([]hotRangeInfo, error) {
		resp, err := status.HotRangesV2(ctx, &remoteRequest)
		if err != nil || resp == nil {
			return nil, err
		}

		var hotRangeInfos = make([]hotRangeInfo, len(resp.Ranges))
		for i, r := range resp.Ranges {
			hotRangeInfos[i] = hotRangeInfo{
				RangeID:             r.RangeID,
				NodeID:              r.NodeID,
				QPS:                 r.QPS,
				WritesPerSecond:     r.WritesPerSecond,
				ReadsPerSecond:      r.ReadsPerSecond,
				WriteBytesPerSecond: r.WriteBytesPerSecond,
				ReadBytesPerSecond:  r.ReadBytesPerSecond,
				CPUTimePerSecond:    r.CPUTimePerSecond,
				LeaseholderNodeID:   r.LeaseholderNodeID,
				Databases:           r.Databases,
				Tables:              r.Tables,
				Indexes:             r.Indexes,
				ReplicaNodeIDs:      r.ReplicaNodeIds,
				SchemaName:          r.SchemaName,
				StoreID:             r.StoreID,
			}
		}
		return hotRangeInfos, nil
	}
	responseFn := func(nodeID roachpb.NodeID, resp []hotRangeInfo) {
		response.Ranges = append(response.Ranges, resp...)
	}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		response.Errors = append(response.Errors, responseError{
			ErrorMessage: err.Error(),
			NodeID:       nodeID,
		})
	}

	timeout := HotRangesRequestNodeTimeout.Get(&a.status.st.SV)
	next, err := paginatedIterateNodes(
		ctx, a.status, "hot ranges", limit, start, requestedNodes, timeout,
		nodeFn, responseFn, errorFn)

	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}
	var nextBytes []byte
	if nextBytes, err = next.MarshalText(); err != nil {
		response.Errors = append(response.Errors, responseError{ErrorMessage: err.Error()})
	} else {
		response.Next = string(nextBytes)
	}
	apiutil.WriteJSONResponse(ctx, w, 200, response)
}
