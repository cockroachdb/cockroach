// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package server

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/coreos/etcd/raft"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	// Default Maximum number of log entries returned.
	defaultMaxLogEntries = 1000

	// stackTraceApproxSize is the approximate size of a goroutine stack trace.
	stackTraceApproxSize = 1024

	// statusPrefix is the root of the cluster statistics and metrics API.
	statusPrefix = "/_status/"

	// statusVars exposes prometheus metrics for monitoring consumption.
	statusVars = statusPrefix + "vars"

	// rangeDebugEndpoint exposes an html page with information about a specific range.
	rangeDebugEndpoint = "/debug/range"
)

// Pattern for local used when determining the node ID.
var localRE = regexp.MustCompile(`(?i)local`)

type metricMarshaler interface {
	json.Marshaler
	PrintAsText(io.Writer) error
}

// A statusServer provides a RESTful status API.
type statusServer struct {
	log.AmbientContext

	db           *client.DB
	gossip       *gossip.Gossip
	metricSource metricMarshaler
	nodeLiveness *storage.NodeLiveness
	rpcCtx       *rpc.Context
	stores       *storage.Stores
	stopper      *stop.Stopper
}

// newStatusServer allocates and returns a statusServer.
func newStatusServer(
	ambient log.AmbientContext,
	db *client.DB,
	gossip *gossip.Gossip,
	metricSource metricMarshaler,
	nodeLiveness *storage.NodeLiveness,
	rpcCtx *rpc.Context,
	stores *storage.Stores,
	stopper *stop.Stopper,
) *statusServer {
	ambient.AddLogTag("status", nil)
	server := &statusServer{
		AmbientContext: ambient,
		db:             db,
		gossip:         gossip,
		metricSource:   metricSource,
		nodeLiveness:   nodeLiveness,
		rpcCtx:         rpcCtx,
		stores:         stores,
		stopper:        stopper,
	}

	return server
}

// RegisterService registers the GRPC service.
func (s *statusServer) RegisterService(g *grpc.Server) {
	serverpb.RegisterStatusServer(g, s)
}

// RegisterGateway starts the gateway (i.e. reverse
// proxy) that proxies HTTP requests to the appropriate gRPC endpoints.
func (s *statusServer) RegisterGateway(
	ctx context.Context, mux *gwruntime.ServeMux, conn *grpc.ClientConn,
) error {
	ctx = s.AnnotateCtx(ctx)
	return serverpb.RegisterStatusHandler(ctx, mux, conn)
}

func (s *statusServer) parseNodeID(nodeIDParam string) (roachpb.NodeID, bool, error) {
	// No parameter provided or set to local.
	if len(nodeIDParam) == 0 || localRE.MatchString(nodeIDParam) {
		return s.gossip.NodeID.Get(), true, nil
	}

	id, err := strconv.ParseInt(nodeIDParam, 10, 64)
	if err != nil {
		return 0, false, errors.Wrap(err, "node id could not be parsed")
	}
	nodeID := roachpb.NodeID(id)
	return nodeID, nodeID == s.gossip.NodeID.Get(), nil
}

func (s *statusServer) dialNode(nodeID roachpb.NodeID) (serverpb.StatusClient, error) {
	addr, err := s.gossip.GetNodeIDAddress(nodeID)
	if err != nil {
		return nil, err
	}
	conn, err := s.rpcCtx.GRPCDial(addr.String())
	if err != nil {
		return nil, err
	}
	return serverpb.NewStatusClient(conn), nil
}

// Gossip returns gossip network status.
func (s *statusServer) Gossip(
	ctx context.Context, req *serverpb.GossipRequest,
) (*gossip.InfoStatus, error) {
	ctx = s.AnnotateCtx(ctx)
	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, err.Error())
	}

	if local {
		infoStatus := s.gossip.GetInfoStatus()
		return &infoStatus, nil
	}
	status, err := s.dialNode(nodeID)
	if err != nil {
		return nil, err
	}
	return status.Gossip(ctx, req)
}

// Details returns node details.
func (s *statusServer) Details(
	ctx context.Context, req *serverpb.DetailsRequest,
) (*serverpb.DetailsResponse, error) {
	ctx = s.AnnotateCtx(ctx)
	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, err.Error())
	}
	if local {
		resp := &serverpb.DetailsResponse{
			NodeID:    s.gossip.NodeID.Get(),
			BuildInfo: build.GetInfo(),
		}
		if addr, err := s.gossip.GetNodeIDAddress(s.gossip.NodeID.Get()); err == nil {
			resp.Address = *addr
		}
		return resp, nil
	}
	status, err := s.dialNode(nodeID)
	if err != nil {
		return nil, err
	}
	return status.Details(ctx, req)
}

// LogFilesList returns a list of available log files.
func (s *statusServer) LogFilesList(
	ctx context.Context, req *serverpb.LogFilesListRequest,
) (*serverpb.LogFilesListResponse, error) {
	ctx = s.AnnotateCtx(ctx)
	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, err.Error())
	}
	if !local {
		status, err := s.dialNode(nodeID)
		if err != nil {
			return nil, err
		}
		return status.LogFilesList(ctx, req)
	}
	log.Flush()
	logFiles, err := log.ListLogFiles()
	if err != nil {
		return nil, err
	}
	return &serverpb.LogFilesListResponse{Files: logFiles}, err
}

// LogFile returns a single log file.
func (s *statusServer) LogFile(
	ctx context.Context, req *serverpb.LogFileRequest,
) (*serverpb.LogEntriesResponse, error) {
	ctx = s.AnnotateCtx(ctx)
	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, err.Error())
	}
	if !local {
		status, err := s.dialNode(nodeID)
		if err != nil {
			return nil, err
		}
		return status.LogFile(ctx, req)
	}

	log.Flush()
	reader, err := log.GetLogReader(req.File, true /* restricted */)
	if reader == nil || err != nil {
		return nil, fmt.Errorf("log file %s could not be opened: %s", req.File, err)
	}
	defer reader.Close()

	var entry log.Entry
	var resp serverpb.LogEntriesResponse
	decoder := log.NewEntryDecoder(reader)
	for {
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		resp.Entries = append(resp.Entries, entry)
	}

	return &resp, nil
}

// parseInt64WithDefault attempts to parse the passed in string. If an empty
// string is supplied or parsing results in an error the default value is
// returned.  If an error does occur during parsing, the error is returned as
// well.
func parseInt64WithDefault(s string, defaultValue int64) (int64, error) {
	if len(s) == 0 {
		return defaultValue, nil
	}
	result, err := strconv.ParseInt(s, 10, 0)
	if err != nil {
		return defaultValue, err
	}
	return result, nil
}

// Logs returns the log entries parsed from the log files stored on
// the server. Log entries are returned in reverse chronological order. The
// following options are available:
// * "starttime" query parameter filters the log entries to only ones that
//   occurred on or after the "starttime". Defaults to a day ago.
// * "endtime" query parameter filters the log entries to only ones that
//   occurred before on on the "endtime". Defaults to the current time.
// * "pattern" query parameter filters the log entries by the provided regexp
//   pattern if it exists. Defaults to nil.
// * "max" query parameter is the hard limit of the number of returned log
//   entries. Defaults to defaultMaxLogEntries.
// * "level" query parameter filters the log entries to be those of the
//   corresponding severity level or worse. Defaults to "info".
func (s *statusServer) Logs(
	_ context.Context, req *serverpb.LogsRequest,
) (*serverpb.LogEntriesResponse, error) {
	log.Flush()

	var sev log.Severity
	if len(req.Level) == 0 {
		sev = log.Severity_INFO
	} else {
		var sevFound bool
		sev, sevFound = log.SeverityByName(req.Level)
		if !sevFound {
			return nil, fmt.Errorf("level could not be determined: %s", req.Level)
		}
	}

	startTimestamp, err := parseInt64WithDefault(
		req.StartTime,
		timeutil.Now().AddDate(0, 0, -1).UnixNano())
	if err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, "StartTime could not be parsed: %s", err)
	}

	endTimestamp, err := parseInt64WithDefault(req.EndTime, timeutil.Now().UnixNano())
	if err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, "EndTime could not be parsed: %s", err)
	}

	if startTimestamp > endTimestamp {
		return nil, grpc.Errorf(codes.InvalidArgument, "StartTime: %d should not be greater than endtime: %d", startTimestamp, endTimestamp)
	}

	maxEntries, err := parseInt64WithDefault(req.Max, defaultMaxLogEntries)
	if err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, "Max could not be parsed: %s", err)
	}
	if maxEntries < 1 {
		return nil, grpc.Errorf(codes.InvalidArgument, "Max: %d should be set to a value greater than 0", maxEntries)
	}

	var regex *regexp.Regexp
	if len(req.Pattern) > 0 {
		if regex, err = regexp.Compile(req.Pattern); err != nil {
			return nil, grpc.Errorf(codes.InvalidArgument, "regex pattern could not be compiled: %s", err)
		}
	}

	entries, err := log.FetchEntriesFromFiles(sev, startTimestamp, endTimestamp, int(maxEntries), regex)
	if err != nil {
		return nil, err
	}

	return &serverpb.LogEntriesResponse{Entries: entries}, nil
}

// TODO(tschottdorf): significant overlap with /debug/pprof/goroutine, except
// that this one allows querying by NodeID.
//
// Stacks handles returns goroutine stack traces.
func (s *statusServer) Stacks(
	ctx context.Context, req *serverpb.StacksRequest,
) (*serverpb.JSONResponse, error) {
	ctx = s.AnnotateCtx(ctx)
	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(nodeID)
		if err != nil {
			return nil, err
		}
		return status.Stacks(ctx, req)
	}

	bufSize := runtime.NumGoroutine() * stackTraceApproxSize
	for {
		buf := make([]byte, bufSize)
		length := runtime.Stack(buf, true)
		// If this wasn't large enough to accommodate the full set of
		// stack traces, increase by 2 and try again.
		if length == bufSize {
			bufSize = bufSize * 2
			continue
		}
		return &serverpb.JSONResponse{Data: buf[:length]}, nil
	}
}

// Nodes returns all node statuses.
func (s *statusServer) Nodes(
	ctx context.Context, req *serverpb.NodesRequest,
) (*serverpb.NodesResponse, error) {
	ctx = s.AnnotateCtx(ctx)
	startKey := keys.StatusNodePrefix
	endKey := startKey.PrefixEnd()

	b := &client.Batch{}
	b.Scan(startKey, endKey)
	if err := s.db.Run(ctx, b); err != nil {
		log.Error(ctx, err)
		return nil, grpc.Errorf(codes.Internal, err.Error())
	}
	rows := b.Results[0].Rows

	resp := serverpb.NodesResponse{
		Nodes: make([]status.NodeStatus, len(rows)),
	}
	for i, row := range rows {
		if err := row.ValueProto(&resp.Nodes[i]); err != nil {
			log.Error(ctx, err)
			return nil, grpc.Errorf(codes.Internal, err.Error())
		}
	}
	return &resp, nil
}

// handleNodeStatus handles GET requests for a single node's status.
func (s *statusServer) Node(
	ctx context.Context, req *serverpb.NodeRequest,
) (*status.NodeStatus, error) {
	ctx = s.AnnotateCtx(ctx)
	nodeID, _, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, err.Error())
	}

	key := keys.NodeStatusKey(nodeID)
	b := &client.Batch{}
	b.Get(key)
	if err := s.db.Run(ctx, b); err != nil {
		log.Error(ctx, err)
		return nil, grpc.Errorf(codes.Internal, err.Error())
	}

	var nodeStatus status.NodeStatus
	if err := b.Results[0].Rows[0].ValueProto(&nodeStatus); err != nil {
		err = errors.Errorf("could not unmarshal NodeStatus from %s: %s", key, err)
		log.Error(ctx, err)
		return nil, grpc.Errorf(codes.Internal, err.Error())
	}
	return &nodeStatus, nil
}

// Metrics return metrics information for the server specified.
func (s *statusServer) Metrics(
	ctx context.Context, req *serverpb.MetricsRequest,
) (*serverpb.JSONResponse, error) {
	ctx = s.AnnotateCtx(ctx)
	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(nodeID)
		if err != nil {
			return nil, err
		}
		return status.Metrics(ctx, req)
	}
	return marshalJSONResponse(s.metricSource)
}

// RaftDebug returns raft debug information for all known nodes.
func (s *statusServer) RaftDebug(
	ctx context.Context, req *serverpb.RaftDebugRequest,
) (*serverpb.RaftDebugResponse, error) {
	ctx = s.AnnotateCtx(ctx)
	nodes, err := s.Nodes(ctx, nil)
	if err != nil {
		return nil, err
	}

	mu := struct {
		syncutil.Mutex
		resp serverpb.RaftDebugResponse
	}{
		resp: serverpb.RaftDebugResponse{
			Ranges: make(map[roachpb.RangeID]serverpb.RaftRangeStatus),
		},
	}

	// Subtract base.NetworkTimeout from the deadline so we have time to process
	// the results and return them.
	if deadline, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline.Add(-base.NetworkTimeout))
		defer cancel()
	}

	// Parallelize fetching of ranges to minimize total time.
	var wg sync.WaitGroup
	for _, node := range nodes.Nodes {
		wg.Add(1)
		nodeID := node.Desc.NodeID
		go func() {
			defer wg.Done()
			ranges, err := s.Ranges(ctx, &serverpb.RangesRequest{NodeId: nodeID.String(), RangeIDs: req.RangeIDs})

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				err := errors.Wrapf(err, "failed to get ranges from %d", nodeID)
				mu.resp.Errors = append(mu.resp.Errors, serverpb.RaftRangeError{Message: err.Error()})
				return
			}

			for _, rng := range ranges.Ranges {
				rangeID := rng.State.Desc.RangeID
				status, ok := mu.resp.Ranges[rangeID]
				if !ok {
					status = serverpb.RaftRangeStatus{
						RangeID: rangeID,
					}
				}
				status.Nodes = append(status.Nodes, serverpb.RaftRangeNode{
					NodeID: nodeID,
					Range:  rng,
				})
				mu.resp.Ranges[rangeID] = status
			}
		}()
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()

	// Check for errors.
	for i, rng := range mu.resp.Ranges {
		for j, node := range rng.Nodes {
			desc := node.Range.State.Desc
			// Check for whether replica should be GCed.
			containsNode := false
			for _, replica := range desc.Replicas {
				if replica.NodeID == node.NodeID {
					containsNode = true
				}
			}
			if !containsNode {
				rng.Errors = append(rng.Errors, serverpb.RaftRangeError{
					Message: fmt.Sprintf("node %d not in range descriptor and should be GCed", node.NodeID),
				})
			}

			// Check for replica descs not matching.
			if j > 0 {
				prevDesc := rng.Nodes[j-1].Range.State.Desc
				if !reflect.DeepEqual(&desc, &prevDesc) {
					prevNodeID := rng.Nodes[j-1].NodeID
					rng.Errors = append(rng.Errors, serverpb.RaftRangeError{
						Message: fmt.Sprintf("node %d range descriptor does not match node %d", node.NodeID, prevNodeID),
					})
				}
			}
			mu.resp.Ranges[i] = rng
		}
	}
	return &mu.resp, nil
}

func (s *statusServer) handleVars(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(httputil.ContentTypeHeader, httputil.PlaintextContentType)
	err := s.metricSource.PrintAsText(w)
	if err != nil {
		log.Error(r.Context(), err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// Ranges returns range info for the specified node.
func (s *statusServer) Ranges(
	ctx context.Context, req *serverpb.RangesRequest,
) (*serverpb.RangesResponse, error) {
	ctx = s.AnnotateCtx(ctx)
	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(nodeID)
		if err != nil {
			return nil, err
		}
		return status.Ranges(ctx, req)
	}

	output := serverpb.RangesResponse{
		Ranges: make([]serverpb.RangeInfo, 0, s.stores.GetStoreCount()),
	}

	convertRaftStatus := func(raftStatus *raft.Status) serverpb.RaftState {
		var state serverpb.RaftState
		if raftStatus == nil {
			state.State = "StateDormant"
			return state
		}

		state.ReplicaID = raftStatus.ID
		state.HardState = raftStatus.HardState
		state.Applied = raftStatus.Applied

		// Grab Lead and State, which together form the SoftState.
		state.Lead = raftStatus.Lead
		state.State = raftStatus.RaftState.String()

		state.Progress = make(map[uint64]serverpb.RaftState_Progress)
		for id, progress := range raftStatus.Progress {
			state.Progress[id] = serverpb.RaftState_Progress{
				Match:           progress.Match,
				Next:            progress.Next,
				Paused:          progress.Paused,
				PendingSnapshot: progress.PendingSnapshot,
				State:           progress.State.String(),
			}
		}

		return state
	}

	constructRangeInfo := func(desc roachpb.RangeDescriptor, rep *storage.Replica, storeID roachpb.StoreID) serverpb.RangeInfo {
		return serverpb.RangeInfo{
			Span: serverpb.PrettySpan{
				StartKey: desc.StartKey.String(),
				EndKey:   desc.EndKey.String(),
			},
			RaftState:     convertRaftStatus(rep.RaftStatus()),
			State:         rep.State(),
			SourceNodeID:  nodeID,
			SourceStoreID: storeID,
		}
	}

	err = s.stores.VisitStores(func(store *storage.Store) error {
		if len(req.RangeIDs) == 0 {
			// All ranges requested.

			// Use IterateRangeDescriptors to read from the engine only
			// because it's already exported.
			err := storage.IterateRangeDescriptors(ctx, store.Engine(),
				func(desc roachpb.RangeDescriptor) (bool, error) {
					rep, err := store.GetReplica(desc.RangeID)
					if err != nil {
						return true, err
					}
					output.Ranges = append(output.Ranges, constructRangeInfo(desc, rep, store.Ident.StoreID))
					return false, nil
				})
			return err
		}

		// Specific ranges requested:
		for _, rid := range req.RangeIDs {
			rep, err := store.GetReplica(rid)
			if err != nil {
				// Not found: continue.
				continue
			}
			desc := rep.Desc()
			output.Ranges = append(output.Ranges, constructRangeInfo(*desc, rep, store.Ident.StoreID))
		}
		return nil
	})
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, err.Error())
	}
	return &output, nil
}

// SpanStats requests the total statistics stored on a node for a given key
// span, which may include multiple ranges.
func (s *statusServer) SpanStats(
	ctx context.Context, req *serverpb.SpanStatsRequest,
) (*serverpb.SpanStatsResponse, error) {
	ctx = s.AnnotateCtx(ctx)
	nodeID, local, err := s.parseNodeID(req.NodeID)
	if err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(nodeID)
		if err != nil {
			return nil, err
		}
		return status.SpanStats(ctx, req)
	}

	output := &serverpb.SpanStatsResponse{}
	err = s.stores.VisitStores(func(store *storage.Store) error {
		stats, count := store.ComputeStatsForKeySpan(req.StartKey.Next(), req.EndKey)
		output.TotalStats.Add(stats)
		output.RangeCount += int32(count)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return output, nil
}

// Returns an HTML page displaying information about all node's view of a
// specific range.
func (s *statusServer) handleDebugRange(w http.ResponseWriter, r *http.Request) {
	ctx := s.AnnotateCtx(r.Context())
	w.Header().Add("Content-type", "text/html")
	rangeIDString := r.URL.Query().Get("id")
	if len(rangeIDString) == 0 {
		http.Error(
			w,
			"no range ID provided, please specify one: debug/range?id=[range_id]",
			http.StatusNoContent,
		)
	}

	rangeID, err := parseInt64WithDefault(rangeIDString, 1)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data := debugRangeData{
		RangeID:             rangeID,
		ReplicaDescPerStore: make(map[roachpb.StoreID]map[roachpb.ReplicaID]roachpb.ReplicaDescriptor),
		Replicas:            make(map[roachpb.ReplicaID][]roachpb.ReplicaDescriptor),
	}

	type nodeResponse struct {
		nodeID roachpb.NodeID
		resp   *serverpb.RangesResponse
		err    error
	}

	aliveNodes := len(s.nodeLiveness.GetLivenessMap())
	responses := make(chan nodeResponse)
	nodeCtx, cancel := context.WithTimeout(ctx, base.NetworkTimeout)
	defer cancel()
	for nodeID, alive := range s.nodeLiveness.GetLivenessMap() {
		if !alive {
			data.Failures = append(data.Failures, serverpb.RangeInfo{
				SourceNodeID: nodeID,
				ErrorMessage: "node liveness reports that the node is not alive",
			})
			aliveNodes--
			continue
		}
		nodeID := nodeID
		if err := s.stopper.RunAsyncTask(nodeCtx, func(ctx context.Context) {
			status, err := s.dialNode(nodeID)
			var rangesResponse *serverpb.RangesResponse
			if err == nil {
				req := &serverpb.RangesRequest{
					RangeIDs: []roachpb.RangeID{roachpb.RangeID(rangeID)},
				}
				rangesResponse, err = status.Ranges(ctx, req)
			}
			response := nodeResponse{
				nodeID: nodeID,
				resp:   rangesResponse,
				err:    err,
			}

			select {
			case responses <- response:
				// Response processed.
			case <-ctx.Done():
				// Context completed, response no longer needed.
			}
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	for remainingResponses := aliveNodes; remainingResponses > 0; remainingResponses-- {
		select {
		case resp := <-responses:
			if resp.err != nil {
				data.Failures = append(data.Failures, serverpb.RangeInfo{
					SourceNodeID: resp.nodeID,
					ErrorMessage: resp.err.Error(),
				})
				continue
			}
			for _, info := range resp.resp.Ranges {
				if len(info.ErrorMessage) != 0 {
					data.Failures = append(data.Failures, info)
				} else {
					data.RangeInfos = append(data.RangeInfos, info)
					data.ReplicaDescPerStore[info.SourceStoreID] = make(map[roachpb.ReplicaID]roachpb.ReplicaDescriptor)
					for _, desc := range info.State.Desc.Replicas {
						data.ReplicaDescPerStore[info.SourceStoreID][desc.ReplicaID] = desc
						data.Replicas[desc.ReplicaID] = append(data.Replicas[desc.ReplicaID], desc)
					}
				}
			}
		case <-ctx.Done():
			http.Error(w, ctx.Err().Error(), http.StatusRequestTimeout)
			return
		}
	}

	data.postProcessing()
	t, err := template.New("webpage").Parse(debugRangeTemplate)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := t.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// jsonWrapper provides a wrapper on any slice data type being
// marshaled to JSON. This prevents a security vulnerability
// where a phishing attack can trick a user's browser into
// requesting a document from Cockroach as an executable script,
// allowing the contents of the fetched document to be treated
// as executable javascript. More details here:
// http://haacked.com/archive/2009/06/25/json-hijacking.aspx/
type jsonWrapper struct {
	Data interface{} `json:"d"`
}

// marshalToJSON marshals the given value into nicely indented JSON. If the
// value is an array or slice it is wrapped in jsonWrapper and then marshalled.
func marshalToJSON(value interface{}) ([]byte, error) {
	switch reflect.ValueOf(value).Kind() {
	case reflect.Array, reflect.Slice:
		value = jsonWrapper{Data: value}
	}
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, errors.Errorf("unable to marshal %+v to json: %s", value, err)
	}
	return body, nil
}

// marshalJSONResponse converts an arbitrary value into a JSONResponse protobuf
// that can be sent via grpc.
func marshalJSONResponse(value interface{}) (*serverpb.JSONResponse, error) {
	data, err := marshalToJSON(value)
	if err != nil {
		return nil, err
	}
	return &serverpb.JSONResponse{Data: data}, nil
}

// replicaIDSlice implements sort.Interface.
type replicaIDSlice []roachpb.ReplicaID

var _ sort.Interface = replicaIDSlice(nil)

func (r replicaIDSlice) Len() int           { return len(r) }
func (r replicaIDSlice) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r replicaIDSlice) Less(i, j int) bool { return r[i] < r[j] }

// rangeInfoSlice implements sort.Interface.
type rangeInfoSlice []serverpb.RangeInfo

var _ sort.Interface = rangeInfoSlice(nil)

func (r rangeInfoSlice) Len() int      { return len(r) }
func (r rangeInfoSlice) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r rangeInfoSlice) Less(i, j int) bool {
	if r[i].SourceNodeID != r[j].SourceNodeID {
		return r[i].SourceNodeID < r[j].SourceNodeID
	}
	return r[i].SourceStoreID < r[j].SourceStoreID
}

type debugRangeData struct {
	RangeID             int64
	RangeInfos          rangeInfoSlice
	Replicas            map[roachpb.ReplicaID][]roachpb.ReplicaDescriptor
	ReplicaDescPerStore map[roachpb.StoreID]map[roachpb.ReplicaID]roachpb.ReplicaDescriptor
	Failures            rangeInfoSlice

	// The following are populated in post-processing.
	ReplicaIDs    replicaIDSlice
	HeaderClasses map[string]string
}

func (d *debugRangeData) postProcessing() {
	// Populate ReplicaIDs
	d.ReplicaIDs = make(replicaIDSlice, 0, len(d.Replicas))
	for repID := range d.Replicas {
		d.ReplicaIDs = append(d.ReplicaIDs, repID)
	}

	// Find any replicas with differenting descriptors.
	d.HeaderClasses = make(map[string]string)
	for repID, descs := range d.Replicas {
		for i := 1; i < len(descs); i++ {
			if !reflect.DeepEqual(descs[0], descs[i]) {
				d.HeaderClasses[fmt.Sprintf("%d", repID)] = "warning"
				break
			}
		}
	}

	// Add custom CSS classes for headers.
	for i := 1; i < len(d.RangeInfos); i++ {
		if !reflect.DeepEqual(d.RangeInfos[0].Span, d.RangeInfos[i].Span) {
			d.HeaderClasses["Key Range"] = "warning"
		}
		if d.RangeInfos[0].State.Lease.Replica.ReplicaID != d.RangeInfos[i].State.Lease.Replica.ReplicaID {
			d.HeaderClasses["Lease"] = "warning"
		}
		if d.RangeInfos[0].State.Lease.Epoch != d.RangeInfos[i].State.Lease.Epoch {
			d.HeaderClasses["Lease Epoch"] = "warning"
		}
		if d.RangeInfos[0].State.Lease.Start != d.RangeInfos[i].State.Lease.Start {
			d.HeaderClasses["Lease Start"] = "warning"
		}
		if d.RangeInfos[0].State.Lease.Expiration != d.RangeInfos[i].State.Lease.Expiration {
			d.HeaderClasses["Lease Expiration"] = "warning"
		}
		if !reflect.DeepEqual(d.RangeInfos[0].Span, d.RangeInfos[i].Span) {
			d.HeaderClasses["Key Range"] = "warning"
		}
		if d.RangeInfos[0].RaftState.Lead != d.RangeInfos[i].RaftState.Lead {
			d.HeaderClasses["Leader"] = "warning"
		}
		if d.RangeInfos[0].RaftState.Applied != d.RangeInfos[i].RaftState.Applied {
			d.HeaderClasses["Applied"] = "warning"
		}
		if d.RangeInfos[0].State.LastIndex != d.RangeInfos[i].State.LastIndex {
			d.HeaderClasses["Last Index"] = "warning"
		}
		if d.RangeInfos[0].State.RaftLogSize != d.RangeInfos[i].State.RaftLogSize {
			d.HeaderClasses["Log Size"] = "warning"
		}
		if d.RangeInfos[0].State.NumPending != d.RangeInfos[i].State.NumPending {
			d.HeaderClasses["Pending Commands"] = "warning"
		}
		if d.RangeInfos[0].RaftState.HardState.Term != d.RangeInfos[i].RaftState.HardState.Term {
			d.HeaderClasses["Term"] = "warning"
		}
		if d.RangeInfos[0].RaftState.HardState.Vote != d.RangeInfos[i].RaftState.HardState.Vote {
			d.HeaderClasses["Vote"] = "warning"
		}
		if d.RangeInfos[0].RaftState.HardState.Commit != d.RangeInfos[i].RaftState.HardState.Commit {
			d.HeaderClasses["Commit"] = "warning"
		}
	}

	sort.Sort(d.ReplicaIDs)
	sort.Sort(d.RangeInfos)
	sort.Sort(d.Failures)
}

// Call these functions so the unused check is appeased. They are used in
// debugRangeTemplate.
var _ = debugRangeData.ConvertRaftState
var _ = debugRangeData.GetStoreID

// ConvertRaftState take a raft.State string and converts it to a displayable
// string.
func (d debugRangeData) ConvertRaftState(state string) string {
	// Strip the "State" from the state.
	if len(state) < 5 {
		return state
	}
	return strings.ToLower(state[5:])
}

// GetStoreID returns the storeID given a uint64 replica ID. Returns 0 if it
// isn't found.
func (d debugRangeData) GetStoreID(repID uint64) roachpb.StoreID {
	for _, desc := range d.Replicas[roachpb.ReplicaID(repID)] {
		return desc.StoreID
	}
	return roachpb.StoreID(0)
}

const debugRangeTemplate = `
<!DOCTYPE html>
<HTML>
  <HEAD>
  	<META CHARSET="UTF-8"/>
    <TITLE>Range ID:{{.RangeID}}</TITLE>
    <STYLE>
      body {
        font-family: "Helvetica Neue", Helvetica, Arial;
        font-size: 14px;
        line-height: 20px;
        font-weight: 400;
        color: #3b3b3b;
        -webkit-font-smoothing: antialiased;
        font-smoothing: antialiased;
        background: #e4e4e4;
      }
      .wrapper {
        margin: 0 auto;
        padding: 0 40px;
      }
      .table {
        margin: 0 0 40px 0;
        display: flex;
      }
      .column {
        display: inline-block;
        background: #f6f6f6;
      }
      .column:nth-of-type(odd) {
        background: #e9e9e9;
      }
      .column.header {
        font-weight: 900;
        color: #ffffff;
        background: #2980b9;
        width: auto;
      }
      .cell {
        padding: 6px 12px;
        display: block;
        height: 20px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        width: 200px;
        border-width: 1px 1px 0 0;
        border-color: rgba(0, 0, 0, 0.1);
        border-style: solid;
      }
      .column:last-child .cell {
        border-right: none;
      }
      .column .cell:first-child {
        border-top: none;
      }
      .header .cell {
        border-color: #2980b9;
      }
      .table.failure {
          display: table;
      }
      .row {
          display: table-row;
          background: #f6f6f6;
      }
      .row .cell {
          display: table-cell;
          width: auto;
      }
      .row:nth-of-type(odd) {
        background: #e9e9e9;
      }
      .row.header {
        font-weight: 900;
        color: #ffffff;
        background: #ea6153;
        width: auto;
      }
      .row .cell:last-child {
        border-right: none;
      }
      .header .cell.warning {
        color: yellow;
      }
      .cell.warning {
        color: red;
      }
      .cell.match {
        color: green;
      }
      .cell.raftstate-leader {
        color: green;
      }
      .cell.raftstate-follower {
        color: blue;
      }
      .cell.raftstate-candidate {
        color: orange;
      }
      .cell.raftstate-precandidate {
        color: darkorange;
      }
      .cell.raftstate-dormant {
        color: gray;
      }
      .cell.lease-holder {
        color: green;
      }
      .cell.lease-follower {
        color: blue;
      }
    </STYLE>
  </HEAD>
  <BODY>
    <DIV CLASS="wrapper">
      <H1>Range r{{$.RangeID}}</H1>
      {{if $.Replicas}}
        <DIV CLASS="table">
          <DIV CLASS="column header">
            <DIV CLASS="cell">Node</DIV>
            <DIV CLASS="cell">Store</DIV>
            <DIV CLASS="cell {{index $.HeaderClasses "Key Range"}}">Key Range</DIV>
            <DIV CLASS="cell {{index $.HeaderClasses "Raft State"}}">Raft State</DIV>
            <DIV CLASS="cell {{index $.HeaderClasses "Lease"}}">Lease</DIV>
            <DIV CLASS="cell {{index $.HeaderClasses "Lease Epoch"}}">Lease Epoch</DIV>
            <DIV CLASS="cell {{index $.HeaderClasses "Lease Start"}}">Lease Start</DIV>
            <DIV CLASS="cell {{index $.HeaderClasses "Lease Expiration"}}">Lease Expiration</DIV>
            <DIV CLASS="cell {{index $.HeaderClasses "Leader"}}">Leader</DIV>
            <DIV CLASS="cell {{index $.HeaderClasses "Applied"}}">Applied</DIV>
            <DIV CLASS="cell {{index $.HeaderClasses "Last Index"}}">Last Index</DIV>
            <DIV CLASS="cell {{index $.HeaderClasses "Log Size"}}">Log Size</DIV>
            <DIV CLASS="cell {{index $.HeaderClasses "Pending Commands"}}">Pending Commands</DIV>
            <DIV CLASS="cell {{index $.HeaderClasses "Term"}}">Term</DIV>
            <DIV CLASS="cell {{index $.HeaderClasses "Vote"}}">Vote</DIV>
            <DIV CLASS="cell {{index $.HeaderClasses "Commit"}}">Commit</DIV>
            {{range $repID, $rep := $.Replicas -}}
              <DIV CLASS="cell {{index $.HeaderClasses $repID.String}}">
                Replica {{$repID}}
              </DIV>
            {{- end}}
          </DIV>
          {{range $_, $det := $.RangeInfos -}}
            <DIV CLASS="column">
              <DIV CLASS="cell" TITLE="n{{$det.SourceNodeID}}">n{{$det.SourceNodeID}}</DIV>
              <DIV CLASS="cell" TITLE="n{{$det.SourceStoreID}}">s{{$det.SourceStoreID}}</DIV>
              <DIV CLASS="cell" TITLE="n{{$det.Span}}">s{{$det.Span}}</DIV>
              {{- $state := $.ConvertRaftState $det.RaftState.State}}
              <DIV CLASS="cell raftstate-{{$state}}" TITLE="{{$state}}">{{$state}}</DIV>
              {{- if not $det.State.Lease}}
                <DIV CLASS="cell">-</DIV>
                <DIV CLASS="cell">-</DIV>
                <DIV CLASS="cell">-</DIV>
                <DIV CLASS="cell">-</DIV>
              {{- else}}
                <DIV CLASS="cell {{if eq $det.State.Lease.Replica.StoreID $det.SourceStoreID}}
                  lease-holder
                {{- else}}
                  lease-follower
                {{- end -}}
                  " TITLE="{{$det.State.Lease.Replica.ReplicaID}}">{{$det.State.Lease.Replica.ReplicaID}}</DIV>
                {{- if not $det.State.Lease.Epoch}}
                  <DIV CLASS="cell">-</DIV>
                {{- else}}
                  <DIV CLASS="cell" TITLE="{{$det.State.Lease.Epoch}}">{{$det.State.Lease.Epoch}}</DIV>
                {{- end}}
                <DIV CLASS="cell" TITLE="{{$det.State.Lease.Start}}">{{$det.State.Lease.Start}}</DIV>
                <DIV CLASS="cell" TITLE="{{$det.State.Lease.Expiration}}">{{$det.State.Lease.Expiration}}</DIV>
              {{- end}}
              {{- $leadStoreID := $.GetStoreID $det.RaftState.Lead}}
              <DIV CLASS="cell {{if eq $det.SourceStoreID $leadStoreID -}}
                  raftstate-leader
                {{- else -}}
                  raftstate-follower
                {{- end -}}
                " TITLE="{{$det.RaftState.Lead}}">{{$det.RaftState.Lead}}</DIV>
              <DIV CLASS="cell" TITLE="{{$det.RaftState.Applied}}">{{$det.RaftState.Applied}}</DIV>
              <DIV CLASS="cell" TITLE="{{$det.State.LastIndex}}">{{$det.State.LastIndex}}</DIV>
              <DIV CLASS="cell" TITLE="{{$det.State.RaftLogSize}}">{{$det.State.RaftLogSize}}</DIV>
              <DIV CLASS="cell" TITLE="{{$det.State.NumPending}}">{{$det.State.NumPending}}</DIV>
              <DIV CLASS="cell" TITLE="{{$det.RaftState.HardState.Term}}">{{$det.RaftState.HardState.Term}}</DIV>
              {{- $voteStoreID := $.GetStoreID $det.RaftState.HardState.Vote}}
              <DIV CLASS="cell {{if eq $det.SourceStoreID $voteStoreID -}}
                  raftstate-leader
                {{- else -}}
                  raftstate-follower
                {{- end -}}
              " TITLE="{{$det.RaftState.HardState.Vote}}">{{$det.RaftState.HardState.Vote}}</DIV>
              <DIV CLASS="cell" TITLE="{{$det.RaftState.HardState.Commit}}">{{$det.RaftState.HardState.Commit}}</DIV>
              {{range $repID, $rep := $.Replicas -}}
                {{- with index (index $.ReplicaDescPerStore $det.SourceStoreID) $repID -}}
                  {{- if eq .ReplicaID 0 -}}
                    <DIV CLASS="cell">-</DIV>
                  {{- else -}}
                    <DIV CLASS="cell {{if not (index $.ReplicaDescPerStore .StoreID) -}}
                        warning
                      {{- else if eq .StoreID $det.SourceStoreID -}}
                        match
                      {{- end}}" TITLE="n{{.NodeID}} s{{.StoreID}}">
                      n{{.NodeID}} s{{.StoreID}}
                    </DIV>
                  {{- end}}
                {{- end}}
              {{- end}}
            </DIV>
          {{- end}}
        </DIV>
      {{else}}
        <p>No information available for Range r{{$.RangeID}}</p>
      {{end}}
      {{if $.Failures}}
        <H2>Failures</H2>
        <DIV CLASS="table failure">
          <DIV CLASS="row header">
            <DIV CLASS="cell">Node</DIV>
            <DIV CLASS="cell">Store</DIV>
            <DIV CLASS="cell">Error</DIV>
          </DIV>
          {{- range $_, $det := $.Failures}}
            <DIV CLASS="row">
              <DIV CLASS="cell">n{{$det.SourceNodeID}}</DIV>
              {{- if not (eq $det.SourceStoreID 0)}}
                <DIV CLASS="cell">n{{$det.SourceStoreID}}</DIV>
              {{- else -}}
                <DIV CLASS="cell">-</DIV>
              {{- end}}
              <DIV CLASS="cell" TITLE="{{$det.ErrorMessage}}">{{$det.ErrorMessage}}</DIV>
            </DIV>
          {{- end}}
        </DIV>
      {{- end}}
    </DIV>
  </BODY>
</HTML>
`
