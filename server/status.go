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

	"github.com/julienschmidt/httprouter"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/server/status"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

const (
	/*
		Note that :node_id can always be replaced by the value "local" to see
		the local nodes response.

		/_status/details/:node_id		 - specific node's details
		/_status/gossip/:node_id         - specific node's gossip
		/_status/logfiles/:node_id       - list log files
		/_status/logfiles/:node_id/:file - returns the contents of the specific
										   log files on specific node
		/_status/logs/:node_id           - log entries from a specific node
		/_status/stacks/:node_id		 - exposes stack traces of running
										   goroutines
		/_status/nodes				     - all nodes' status
		/_status/nodes/:node_id		     - a specific node's status
	*/

	// statusPrefix is the root of the cluster statistics and metrics API.
	statusPrefix = "/_status/"

	// statusGossipPattern exposes a view of the gossip network.
	statusGossipPattern = statusPrefix + "gossip/:node_id"

	// statusDetailsPattern exposes a node's details.
	statusDetailsPattern = statusPrefix + "details/:node_id"

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
	statusNodesPrefix = statusPrefix + "nodes/"
	// statusNodePattern exposes status for a single node.
	statusNodePattern = statusPrefix + "nodes/:node_id"

	// statusMetricsPrefix exposes transient stats.
	statusMetricsPrefix = statusPrefix + "metrics/"
	// statusMetricsPattern exposes transient stats for a node.
	statusMetricsPattern = statusPrefix + "metrics/:node_id"

	// healthEndpoint is a shortcut for local details, intended for use by
	// monitoring processes to verify that the server is up.
	healthEndpoint = "/health"
)

// Pattern for local used when determining the node ID.
var localRE = regexp.MustCompile(`(?i)local`)

// A statusServer provides a RESTful status API.
type statusServer struct {
	db           *client.DB
	gossip       *gossip.Gossip
	metricSource json.Marshaler
	router       *httprouter.Router
	ctx          *Context
	proxyClient  *http.Client
}

// newStatusServer allocates and returns a statusServer.
func newStatusServer(db *client.DB, gossip *gossip.Gossip, metricSource json.Marshaler, ctx *Context) *statusServer {
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
		proxyClient:  httpClient,
	}

	server.router.GET(statusGossipPattern, server.handleGossip)
	server.router.GET(statusDetailsPattern, server.handleDetails)
	server.router.GET(statusLogFilesListPattern, server.handleLogFilesList)
	server.router.GET(statusLogFilePattern, server.handleLogFile)
	server.router.GET(statusLogsPattern, server.handleLogs)
	// TODO(tschottdorf): significant overlap with /debug/pprof/goroutine,
	// except that this one allows querying by NodeID.
	server.router.GET(statusStacksPattern, server.handleStacks)
	server.router.GET(statusNodesPrefix, server.handleNodesStatus)
	server.router.GET(statusNodePattern, server.handleNodeStatus)
	server.router.GET(statusMetricsPattern, server.handleMetrics)

	server.router.GET(healthEndpoint, server.handleDetailsLocal)
	return server
}

// ServeHTTP implements the http.Handler interface.
func (s *statusServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}

// extractNodeID examines the node_id URL parameter and returns the nodeID and a
// boolean showing if it is this node. If node_id is "local" or not present, it
// returns the local nodeID.
func (s *statusServer) extractNodeID(ps httprouter.Params) (roachpb.NodeID, bool, error) {
	nodeIDParam := ps.ByName("node_id")

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

// proxyRequest performs a GET request to another node's status server.
func (s *statusServer) proxyRequest(nodeID roachpb.NodeID, w http.ResponseWriter, r *http.Request) {
	addr, err := s.gossip.GetNodeIDAddress(nodeID)
	if err != nil {
		http.Error(w,
			fmt.Sprintf("node could not be located: %s", nodeID),
			http.StatusBadRequest)
		return
	}

	// Create a call to the other node. We might want to consider moving this
	// to an RPC instead of just proxying it.
	// Generate the redirect url and copy all the parameters to it.
	requestURL := fmt.Sprintf("%s://%s%s?%s", s.ctx.HTTPRequestScheme(), addr, r.URL.Path, r.URL.RawQuery)
	req, err := http.NewRequest("GET", requestURL, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp, err := s.proxyClient.Do(req)
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set(util.ContentTypeHeader, resp.Header.Get(util.ContentTypeHeader))

	// Only pass through a whitelisted set of status codes.
	switch resp.StatusCode {
	case http.StatusOK, http.StatusNotFound, http.StatusBadRequest, http.StatusInternalServerError:
		w.WriteHeader(resp.StatusCode)
	default:
		w.WriteHeader(http.StatusInternalServerError)
	}

	defer resp.Body.Close()
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		log.Error(err)
	}
}

// handleGossipLocal handles local requests for gossip network status.
func (s *statusServer) handleGossipLocal(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	b, err := s.gossip.GetInfosAsJSON()
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	w.Header().Set(util.ContentTypeHeader, util.JSONContentType)
	if _, err := w.Write(b); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleGossip handles GET requests for gossip network status.
func (s *statusServer) handleGossip(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	nodeID, local, err := s.extractNodeID(ps)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if local {
		s.handleGossipLocal(w, r, ps)
	} else {
		s.proxyRequest(nodeID, w, r)
	}
}

// handleDetailsLocal handles local requests for node details.
func (s *statusServer) handleDetailsLocal(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	local := struct {
		NodeID    roachpb.NodeID      `json:"nodeID"`
		Address   util.UnresolvedAddr `json:"address"`
		BuildInfo util.BuildInfo      `json:"buildInfo"`
	}{
		NodeID:    s.gossip.GetNodeID(),
		BuildInfo: util.GetBuildInfo(),
	}
	if addr, err := s.gossip.GetNodeIDAddress(s.gossip.GetNodeID()); err == nil {
		local.Address = *addr
	}
	respondAsJSON(w, r, local)
}

// handleDetails handles GET requests for node details.
func (s *statusServer) handleDetails(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	nodeID, local, err := s.extractNodeID(ps)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if local {
		s.handleDetailsLocal(w, r, ps)
	} else {
		s.proxyRequest(nodeID, w, r)
	}
}

// handleLogFilesList handles local requests for a list of available log files.
func (s *statusServer) handleLogFilesListLocal(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	log.Flush()
	logFiles, err := log.ListLogFiles()
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondAsJSON(w, r, logFiles)
}

// handleLogFilesList handles GET requests for a list of available log files.
func (s *statusServer) handleLogFilesList(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	nodeID, local, err := s.extractNodeID(ps)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if local {
		s.handleLogFilesListLocal(w, r, ps)
	} else {
		s.proxyRequest(nodeID, w, r)
	}
}

// handleLocalLogFile handles local requests for a single log. If no filename is
// available, it returns 404. The log contents are returned in structured
// format as JSON.
func (s *statusServer) handleLogFileLocal(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	log.Flush()
	file := ps.ByName("file")
	reader, err := log.GetLogReader(file, true /* restricted */)
	if reader == nil || err != nil {
		log.Errorf("log file %s could not be opened: %s", file, err)
		http.NotFound(w, r)
		return
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
			log.Error(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		entries = append(entries, entry)
	}

	respondAsJSON(w, r, entries)
}

// handleLogFile handles GET requests for a single log file.
func (s *statusServer) handleLogFile(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	nodeID, local, err := s.extractNodeID(ps)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if local {
		s.handleLogFileLocal(w, r, ps)
	} else {
		s.proxyRequest(nodeID, w, r)
	}
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

// handleLogsLocal returns the log entries parsed from the log files stored on
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
func (s *statusServer) handleLogsLocal(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	log.Flush()

	level := r.URL.Query().Get("level")
	var sev log.Severity
	if len(level) == 0 {
		sev = log.InfoLog
	} else {
		var sevFound bool
		sev, sevFound = log.SeverityByName(level)
		if !sevFound {
			http.Error(w,
				fmt.Sprintf("level could not be determined: %s", level),
				http.StatusBadRequest)
			return
		}
	}

	startTimestamp, err := parseInt64WithDefault(
		r.URL.Query().Get("starttime"),
		timeutil.Now().AddDate(0, 0, -1).UnixNano())
	if err != nil {
		http.Error(w,
			fmt.Sprintf("starttime could not be parsed: %s", err),
			http.StatusBadRequest)
		return
	}

	endTimestamp, err := parseInt64WithDefault(
		r.URL.Query().Get("endtime"),
		timeutil.Now().UnixNano())
	if err != nil {
		http.Error(w,
			fmt.Sprintf("endtime could not be parsed: %s", err),
			http.StatusBadRequest)
		return
	}

	if startTimestamp > endTimestamp {
		http.Error(w,
			fmt.Sprintf("startime: %d should not be greater than endtime: %d", startTimestamp, endTimestamp),
			http.StatusBadRequest)
		return
	}

	maxEntries, err := parseInt64WithDefault(
		r.URL.Query().Get("max"),
		defaultMaxLogEntries)
	if err != nil {
		http.Error(w,
			fmt.Sprintf("max could not be parsed: %s", err),
			http.StatusBadRequest)
		return
	}
	if maxEntries < 1 {
		http.Error(w,
			fmt.Sprintf("max: %d should be set to a value greater than 0", maxEntries),
			http.StatusBadRequest)
		return
	}

	pattern := r.URL.Query().Get("pattern")
	var regex *regexp.Regexp
	if len(pattern) > 0 {
		if regex, err = regexp.Compile(pattern); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "regex pattern could not be compiled: %s", err)
			return
		}
	}

	entries, err := log.FetchEntriesFromFiles(sev, startTimestamp, endTimestamp, int(maxEntries), regex)
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondAsJSON(w, r, entries)
}

// handleLogs handles GET requests for log entires.
func (s *statusServer) handleLogs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	nodeID, local, err := s.extractNodeID(ps)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if local {
		s.handleLogsLocal(w, r, ps)
	} else {
		s.proxyRequest(nodeID, w, r)
	}
}

// handleStacksLocal handles local requests for goroutines stack traces.
func (s *statusServer) handleStacksLocal(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
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
		w.Header().Set(util.ContentTypeHeader, util.PlaintextContentType)
		if _, err := w.Write(buf[:length]); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
}

// handleStacksLocal handles GET requests for goroutine stack traces.
func (s *statusServer) handleStacks(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	nodeID, local, err := s.extractNodeID(ps)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if local {
		s.handleStacksLocal(w, r, ps)
	} else {
		s.proxyRequest(nodeID, w, r)
	}
}

// handleNodesStatus handles GET requests for all node statuses.
func (s *statusServer) handleNodesStatus(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	startKey := keys.StatusNodePrefix
	endKey := startKey.PrefixEnd()

	rows, pErr := s.db.ScanInconsistent(startKey, endKey, 0)
	if pErr != nil {
		log.Error(pErr)
		http.Error(w, pErr.String(), http.StatusInternalServerError)
		return
	}

	nodeStatuses := []status.NodeStatus{}
	for _, row := range rows {
		nodeStatus := &status.NodeStatus{}
		if pErr := row.ValueProto(nodeStatus); pErr != nil {
			log.Error(pErr)
			http.Error(w, pErr.Error(), http.StatusInternalServerError)
			return
		}
		nodeStatuses = append(nodeStatuses, *nodeStatus)
	}
	respondAsJSON(w, r, nodeStatuses)
}

// handleNodeStatus handles GET requests for a single node's status.
func (s *statusServer) handleNodeStatus(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	nodeID, _, err := s.extractNodeID(ps)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	key := keys.NodeStatusKey(int32(nodeID))
	nodeStatus := &status.NodeStatus{}
	if pErr := s.db.GetProtoInconsistent(key, nodeStatus); pErr != nil {
		log.Error(pErr)
		http.Error(w, pErr.String(), http.StatusInternalServerError)
		return
	}

	respondAsJSON(w, r, nodeStatus)
}

func (s *statusServer) handleMetrics(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	nodeID, local, err := s.extractNodeID(ps)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if !local {
		s.proxyRequest(nodeID, w, r)
		return
	}
	respondAsJSON(w, r, s.metricSource)
}

func respondAsJSON(w http.ResponseWriter, r *http.Request, response interface{}) {
	b, contentType, err := util.MarshalResponse(r, response, []util.EncodingType{util.JSONEncoding})
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set(util.ContentTypeHeader, contentType)
	if _, err := w.Write(b); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// PathForNodeStatus returns the path needed to issue a GET request for node status. If passed
// an empty nodeID, this returns the path to GET status for all nodes.
func PathForNodeStatus(nodeID string) string {
	return fmt.Sprintf("%s/%s", statusNodesPrefix, nodeID)
}
