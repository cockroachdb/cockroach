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
	"io"
	"net/http"
	"regexp"
	"runtime"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"golang.org/x/net/context"

	gwruntime "github.com/gengo/grpc-gateway/runtime"
	"github.com/julienschmidt/httprouter"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/build"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/server/status"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

const (
	/*
	   Note that :node_id can always be replaced by the value "local" to see
	   the local nodes response.

	   /_status/details/:node_id        - specific node's details
	   /_status/gossip/:node_id         - specific node's gossip
	   /_status/logfiles/:node_id       - list log files
	   /_status/logfiles/:node_id/:file - returns the contents of the specific
	                                      log files on specific node
	   /_status/logs/:node_id           - log entries from a specific node
	   /_status/stacks/:node_id         - exposes stack traces of running goroutines
	   /_status/nodes                   - all nodes' status
	   /_status/nodes/:node_id          - a specific node's status
	   /_status/metrics/:node_id        - a specific node's metrics
	   /_status/ranges/:node_id         - a specific node's range metadata
	*/

	// statusPrefix is the root of the cluster statistics and metrics API.
	statusPrefix = "/_status/"

	// statusLogFilesListPattern exposes a list of log files.
	statusLogFilesListPattern = statusPrefix + "logfiles/:node_id"
	// statusLogFilePattern exposes a specific file on a node.
	statusLogFilePattern = statusPrefix + "logfiles/:node_id/:file"

	// statusLogKeyPrefix exposes the logs for each node.
	statusLogsPattern = statusPrefix + "logs/:node_id"
	// Default Maximum number of log entries returned.
	defaultMaxLogEntries = 1000

	// statusStacksPattern exposes the stack traces of running goroutines.
	statusStacksPattern = statusPrefix + "stacks/:node_id"
	// stackTraceApproxSize is the approximate size of a goroutine stack trace.
	stackTraceApproxSize = 1024

	// statusNodesPrefix exposes status for all nodes in the cluster.
	statusNodesPrefix = statusPrefix + "nodes"

	// statusMetricsPrefix exposes transient stats.
	statusMetricsPrefix = statusPrefix + "metrics/"
	// statusMetricsPattern exposes transient stats for a node.
	statusMetricsPattern = statusPrefix + "metrics/:node_id"

	// statusRangesPrefix exposes range information.
	statusRangesPrefix = statusPrefix + "ranges/"

	// healthEndpoint is a shortcut for local details, intended for use by
	// monitoring processes to verify that the server is up.
	healthEndpoint = "/health"
)

// Pattern for local used when determining the node ID.
var localRE = regexp.MustCompile(`(?i)local`)

func inconsistentBatch() *client.Batch {
	b := &client.Batch{}
	b.Header.ReadConsistency = roachpb.INCONSISTENT
	return b
}

// A statusServer provides a RESTful status API.
type statusServer struct {
	db           *client.DB
	gossip       *gossip.Gossip
	metricSource json.Marshaler
	router       *httprouter.Router
	ctx          *Context
	rpcCtx       *rpc.Context
	proxyClient  *http.Client
	stores       *storage.Stores
}

// newStatusServer allocates and returns a statusServer.
func newStatusServer(
	db *client.DB,
	gossip *gossip.Gossip,
	metricSource json.Marshaler,
	ctx *Context,
	rpcCtx *rpc.Context,
	stores *storage.Stores,
) *statusServer {
	// Create an http client with a timeout
	tlsConfig, err := ctx.GetClientTLSConfig()
	if err != nil {
		log.Error(err)
		return nil
	}
	httpClient := &http.Client{
		Transport: &http.Transport{TLSClientConfig: tlsConfig},
		Timeout:   base.NetworkTimeout,
	}

	server := &statusServer{
		db:           db,
		gossip:       gossip,
		metricSource: metricSource,
		router:       httprouter.New(),
		ctx:          ctx,
		rpcCtx:       rpcCtx,
		proxyClient:  httpClient,
		stores:       stores,
	}

	server.router.GET(statusLogFilesListPattern, server.handleLogFilesList)
	server.router.GET(statusLogFilePattern, server.handleLogFile)
	server.router.GET(statusLogsPattern, server.handleLogs)
	// TODO(tschottdorf): significant overlap with /debug/pprof/goroutine,
	// except that this one allows querying by NodeID.
	server.router.GET(statusStacksPattern, server.handleStacks)
	server.router.GET(statusMetricsPattern, server.handleMetrics)

	return server
}

// RegisterService registers the GRPC service.
func (s *statusServer) RegisterService(g *grpc.Server) {
	RegisterStatusServer(g, s)
}

// RegisterGateway starts the gateway (i.e. reverse
// proxy) that proxies HTTP requests to the appropriate gRPC endpoints.
func (s *statusServer) RegisterGateway(
	ctx context.Context,
	mux *gwruntime.ServeMux,
	addr string,
	opts []grpc.DialOption,
) error {
	if err := RegisterStatusHandlerFromEndpoint(ctx, mux, addr, opts); err != nil {
		return util.Errorf("error constructing grpc-gateway: %s. are your certificates valid?", err)
	}

	// Pass all requests for gRPC-based API endpoints to the gateway mux.
	s.router.NotFound = mux
	return nil
}

// ServeHTTP implements the http.Handler interface.
func (s *statusServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}

func (s *statusServer) parseNodeID(nodeIDParam string) (roachpb.NodeID, bool, error) {
	// No parameter provided or set to local.
	if len(nodeIDParam) == 0 || localRE.MatchString(nodeIDParam) {
		return s.gossip.GetNodeID(), true, nil
	}

	id, err := strconv.ParseInt(nodeIDParam, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("node id could not be parsed: %s", err)
	}
	nodeID := roachpb.NodeID(id)
	return nodeID, nodeID == s.gossip.GetNodeID(), nil
}

func (s *statusServer) dialNode(nodeID roachpb.NodeID) (StatusClient, error) {
	addr, err := s.gossip.GetNodeIDAddress(nodeID)
	if err != nil {
		return nil, err
	}
	conn, err := s.rpcCtx.GRPCDial(addr.String())
	if err != nil {
		return nil, err
	}
	return NewStatusClient(conn), nil
}

// Gossip returns gossip network status.
func (s *statusServer) Gossip(ctx context.Context, req *GossipRequest) (*gossip.InfoStatus, error) {
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
func (s *statusServer) Details(ctx context.Context, req *DetailsRequest) (*DetailsResponse, error) {
	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, err.Error())
	}
	if local {
		resp := &DetailsResponse{
			NodeID:    s.gossip.GetNodeID(),
			BuildInfo: build.GetInfo(),
		}
		if addr, err := s.gossip.GetNodeIDAddress(s.gossip.GetNodeID()); err == nil {
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
func (s *statusServer) LogFilesList(ctx context.Context, req *LogFilesListRequest) (*JSONResponse, error) {
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
	return marshalJSONResponse(logFiles)
}

// handleLogFilesList handles GET requests for a list of available log files.
func (s *statusServer) handleLogFilesList(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	resp, err := s.LogFilesList(context.TODO(), &LogFilesListRequest{NodeId: ps.ByName("node_id")})
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	writeJSONResponse(w, resp)
}

// LogFile returns a single log file.
func (s *statusServer) LogFile(ctx context.Context, req *LogFileRequest) (*JSONResponse, error) {
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

	entry := log.Entry{}
	var entries []log.Entry
	decoder := log.NewEntryDecoder(reader)
	for {
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		entries = append(entries, entry)
	}

	return marshalJSONResponse(entries)
}

// handleLogFile handles GET requests for a single log file.
func (s *statusServer) handleLogFile(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	req := LogFileRequest{
		NodeId: ps.ByName("node_id"),
		File:   ps.ByName("file"),
	}
	resp, err := s.LogFile(context.TODO(), &req)
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	writeJSONResponse(w, resp)
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
func (s *statusServer) Logs(ctx context.Context, req *LogsRequest) (*JSONResponse, error) {
	log.Flush()

	var sev log.Severity
	if len(req.Level) == 0 {
		sev = log.InfoLog
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

	return marshalJSONResponse(entries)
}

// handleLogs handles GET requests for log entires.
func (s *statusServer) handleLogs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	q := r.URL.Query()
	req := LogsRequest{
		NodeId:    ps.ByName("node_id"),
		Level:     q.Get("level"),
		StartTime: q.Get("starttime"),
		EndTime:   q.Get("endtime"),
		Max:       q.Get("max"),
		Pattern:   q.Get("pattern"),
	}
	resp, err := s.Logs(context.TODO(), &req)
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	writeJSONResponse(w, resp)
}

// Stacks handles returns goroutine stack traces.
func (s *statusServer) Stacks(ctx context.Context, req *StacksRequest) (*JSONResponse, error) {
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
		return &JSONResponse{Data: buf[:length]}, nil
	}
}

// handleStacksLocal handles GET requests for goroutine stack traces.
func (s *statusServer) handleStacks(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	resp, err := s.Stacks(context.TODO(), &StacksRequest{NodeId: ps.ByName("node_id")})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set(util.ContentTypeHeader, util.PlaintextContentType)
	if _, err := w.Write(resp.Data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// Nodes returns all node statuses.
func (s *statusServer) Nodes(_ context.Context, req *NodesRequest) (*NodesResponse, error) {
	startKey := keys.StatusNodePrefix
	endKey := startKey.PrefixEnd()

	b := inconsistentBatch()
	b.Scan(startKey, endKey, 0)
	err := s.db.Run(b)
	if err != nil {
		log.Error(err)
		return nil, grpc.Errorf(codes.Internal, err.Error())
	}
	rows := b.Results[0].Rows

	resp := NodesResponse{
		Nodes: make([]status.NodeStatus, len(rows)),
	}
	for i, row := range rows {
		if err := row.ValueProto(&resp.Nodes[i]); err != nil {
			log.Error(err)
			return nil, grpc.Errorf(codes.Internal, err.Error())
		}
	}
	return &resp, nil
}

// handleNodeStatus handles GET requests for a single node's status.
func (s *statusServer) Node(ctx context.Context, req *NodeRequest) (*status.NodeStatus, error) {
	nodeID, _, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, err.Error())
	}

	key := keys.NodeStatusKey(int32(nodeID))
	b := inconsistentBatch()
	b.Get(key)
	if err := s.db.Run(b); err != nil {
		log.Error(err)
		return nil, grpc.Errorf(codes.Internal, err.Error())
	}

	var nodeStatus status.NodeStatus
	if err := b.Results[0].Rows[0].ValueProto(&nodeStatus); err != nil {
		err = util.Errorf("could not unmarshal NodeStatus from %s: %s", key, err)
		log.Error(err)
		return nil, grpc.Errorf(codes.Internal, err.Error())
	}
	return &nodeStatus, nil
}

// Metrics return metrics information for the server specified.
func (s *statusServer) Metrics(ctx context.Context, req *MetricsRequest) (*JSONResponse, error) {
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

func (s *statusServer) handleMetrics(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	resp, err := s.Metrics(context.TODO(), &MetricsRequest{NodeId: ps.ByName("node_id")})
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	writeJSONResponse(w, resp)
}

// Ranges returns range info for the server specified
func (s *statusServer) Ranges(ctx context.Context, req *RangesRequest) (*RangesResponse, error) {
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

	output := RangesResponse{
		Ranges: make([]RangeInfo, 0, s.stores.GetStoreCount()),
	}
	err = s.stores.VisitStores(func(store *storage.Store) error {
		// Use IterateRangeDescriptors to read from the engine only
		// because it's already exported.
		err := storage.IterateRangeDescriptors(store.Engine(),
			func(desc roachpb.RangeDescriptor) (bool, error) {
				status := store.RaftStatus(desc.RangeID)
				var raftState string
				if status != nil {
					// We can't put the whole raft.Status object in the json output
					// because it contains a map with integer keys. Just extract
					// the most interesting bit for now.
					raftState = status.RaftState.String()
				}
				output.Ranges = append(output.Ranges, RangeInfo{
					Desc:      desc,
					RaftState: raftState,
				})
				return false, nil
			})
		return err
	})
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, err.Error())
	}
	return &output, nil
}

// marshalJSONResponse converts an arbitrary value into a JSONResponse protobuf
// that can be sent via grpc.
func marshalJSONResponse(value interface{}) (*JSONResponse, error) {
	data, err := util.MarshalToJSON(value)
	if err != nil {
		return nil, err
	}
	return &JSONResponse{Data: data}, nil
}

// writeJSONResponse writes a JSONResponse to a http.ResponseWriter.
func writeJSONResponse(w http.ResponseWriter, resp *JSONResponse) {
	w.Header().Set(util.ContentTypeHeader, util.JSONContentType)
	if _, err := w.Write(resp.Data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// PathForNodeStatus returns the path needed to issue a GET request for node status. If passed
// an empty nodeID, this returns the path to GET status for all nodes.
func PathForNodeStatus(nodeID string) string {
	if len(nodeID) == 0 {
		return statusNodesPrefix
	}
	return fmt.Sprintf("%s/%s", statusNodesPrefix, nodeID)
}
