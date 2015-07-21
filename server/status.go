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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Shawn Morel (shawn@strangemonad.com)
// Author: Bram Gruneir (bram@cockroachlabs.com)

package server

import (
	"fmt"
	"io"
	"net/http"
	"regexp"
	"runtime"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/server/status"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/julienschmidt/httprouter"
)

const (
	// stackTraceApproxSize is the approximate size of a goroutine stack trace.
	stackTraceApproxSize = 1024

	// statusKeyPrefix is the root of the RESTful cluster statistics and metrics API.
	statusKeyPrefix = "/_status/"

	// statusGossipKeyPrefix exposes a view of the gossip network.
	statusGossipKeyPrefix = statusKeyPrefix + "gossip"

	// statusLocalKeyPrefix is the key prefix for all local status
	// info. Unadorned, the URL exposes the status of the node serving
	// the request.  This is equivalent to GETing
	// statusNodesKeyPrefix/<current-node-id>.  Useful for debugging
	// nodes that aren't communicating with the cluster properly.
	statusLocalKeyPrefix = statusKeyPrefix + "local/"

	// statusLocalLogKeyPrefix exposes a list of log files for the node.
	// logfiles -> lists available log files
	// logfiles/ -> lists available log files
	// logfiles/{file} -> fetches contents of named log
	statusLocalLogFileKeyPrefix = statusLocalKeyPrefix + "logfiles/"
	// statusLocalLogFileKeyPattern is the pattern to match
	// logfiles/{file}
	statusLocalLogFileKeyPattern = statusLocalLogFileKeyPrefix + ":file"

	// statusLocalStacksKey exposes stack traces of running goroutines.
	statusLocalStacksKey = statusLocalKeyPrefix + "stacks"

	// statusNodeKeyPrefix exposes status for each of the nodes the cluster.
	// nodes -> lists all nodes
	// nodes/ -> lists all nodes
	// nodes/{NodeID} -> shows only the status for that specific node
	statusNodeKeyPrefix = statusKeyPrefix + "nodes/"
	// statusNodeKeyPattern is the pattern to match nodes/{NodeID}
	statusNodeKeyPattern = statusNodeKeyPrefix + ":id"

	// statusStoreKeyPrefix exposes status for each store.
	// stores -> lists all nodes
	// stores/ -> lists all nodes
	// stores/{StoreID} -> shows only the status for that specific store
	statusStoreKeyPrefix = statusKeyPrefix + "stores/"
	// statusStoreKeyPattern is the pattern to match stores/{StoreID}
	statusStoreKeyPattern = statusStoreKeyPrefix + ":id"

	// statusLogKeyPrefix exposes the logs for each node
	// logs -> list log entries for the local node
	// logs/ -> list log entries for the local node
	// logs/{NodeID} -> list logs entries for that specific node
	statusLogKeyPrefix = statusKeyPrefix + "logs/"
	// statusLogKeyPattern is the pattern to match log/{NodeID}
	statusLogKeyPattern = statusLogKeyPrefix + ":node_id"

	// statusTransactionsKeyPrefix exposes transaction statistics.
	statusTransactionsKeyPrefix = statusKeyPrefix + "txns/"

	// Default Maximum number of log entries returned.
	defaultMaxLogEntries = 1000

	// Timeout for used for looking up log entries in another node.
	logEntriesTimeout = time.Second
)

// A statusServer provides a RESTful status API.
type statusServer struct {
	db          *client.DB
	gossip      *gossip.Gossip
	router      *httprouter.Router
	ctx         *Context
	proxyClient *http.Client
}

// newStatusServer allocates and returns a statusServer.
func newStatusServer(db *client.DB, gossip *gossip.Gossip, ctx *Context) *statusServer {
	// Create an http client with a timeout
	tlsConfig, err := ctx.GetClientTLSConfig()
	if err != nil {
		log.Error(err)
		return nil
	}
	httpClient := &http.Client{
		Transport: &http.Transport{TLSClientConfig: tlsConfig},
		Timeout:   logEntriesTimeout,
	}

	server := &statusServer{
		db:          db,
		gossip:      gossip,
		router:      httprouter.New(),
		ctx:         ctx,
		proxyClient: httpClient,
	}

	server.router.GET(statusGossipKeyPrefix, server.handleGossipStatus)
	server.router.GET(statusLocalKeyPrefix, server.handleLocalStatus)
	server.router.GET(statusLocalLogFileKeyPrefix, server.handleLocalLogFiles)
	server.router.GET(statusLocalLogFileKeyPattern, server.handleLocalLogFile)
	server.router.GET(statusLogKeyPrefix, server.handleLocalLog)
	server.router.GET(statusLogKeyPattern, server.handleLogs)
	server.router.GET(statusLocalStacksKey, server.handleLocalStacks)
	server.router.GET(statusNodeKeyPrefix, server.handleNodesStatus)
	server.router.GET(statusNodeKeyPattern, server.handleNodeStatus)
	server.router.GET(statusStoreKeyPrefix, server.handleStoresStatus)
	server.router.GET(statusStoreKeyPattern, server.handleStoreStatus)
	server.router.GET(statusTransactionsKeyPrefix, server.handleTransactionStatus)

	return server
}

// ServeHTTP implements the http.Handler interface.
func (s *statusServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}

// handleGossipStatus handles GET requests for gossip network status.
func (s *statusServer) handleGossipStatus(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set(util.ContentTypeHeader, util.JSONContentType)
	b, err := s.gossip.GetInfosAsJSON()
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	w.Write(b)
}

// handleLocalStatus handles GET requests for local-node status.
func (s *statusServer) handleLocalStatus(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	local := struct {
		Address   util.UnresolvedAddr `json:"address"`
		BuildInfo util.BuildInfo      `json:"buildInfo"`
	}{
		BuildInfo: util.GetBuildInfo(),
	}
	if addr, err := s.gossip.GetNodeIDAddress(s.gossip.GetNodeID()); err == nil {
		local.Address = addr.(util.UnresolvedAddr)
	}
	b, contentType, err := util.MarshalResponse(r, local, []util.EncodingType{util.JSONEncoding})
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set(util.ContentTypeHeader, contentType)
	w.Write(b)
}

// handleLocalLogFiles handles GET requests for list of available logs.
func (s *statusServer) handleLocalLogFiles(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	log.Flush()
	logFiles, err := log.ListLogFiles()
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	b, contentType, err := util.MarshalResponse(r, logFiles, []util.EncodingType{util.JSONEncoding})
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set(util.ContentTypeHeader, contentType)
	w.Write(b)
}

// handleLocalLogFile handles GET requests for a single log. If no filename is
// available, it returns 404. The log contents are returned in structured
// format as JSON.
func (s *statusServer) handleLocalLogFile(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	log.Flush()
	file := ps.ByName("file")
	reader, err := log.GetLogReader(file, false /* !allowAbsolute */)
	if reader == nil || err != nil {
		log.Errorf("log file %s could not be opened: %s", file, err)
		http.NotFound(w, r)
		return
	}
	defer reader.Close()

	entry := log.LogEntry{}
	var entries []log.LogEntry
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

	b, contentType, err := util.MarshalResponse(r, entries, []util.EncodingType{util.JSONEncoding})
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set(util.ContentTypeHeader, contentType)
	w.Write(b)
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

// handleLocalLog returns the log entries parsed from the log files stored on
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
func (s *statusServer) handleLocalLog(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
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
		time.Now().AddDate(0, 0, -1).UnixNano())
	if err != nil {
		http.Error(w,
			fmt.Sprintf("starttime could not be parsed: %s", err),
			http.StatusBadRequest)
		return
	}

	endTimestamp, err := parseInt64WithDefault(
		r.URL.Query().Get("endtime"),
		time.Now().UnixNano())
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
			fmt.Sprintf("max: %s should be set to a value greater than 0", maxEntries),
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

	b, contentType, err := util.MarshalResponse(r, entries, []util.EncodingType{util.JSONEncoding})
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set(util.ContentTypeHeader, contentType)
	w.Write(b)
}

// handleLog checks the node_id parameter and if it matches that of the current
// node, returns the local log entries. Otherwise, it looks up the specified
// node in gossip and if it exists, sends a request directly to the node to get
// its logs. It passes all other query parameters along to this new request.
func (s *statusServer) handleLogs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	nodeIDParam := ps.ByName("node_id")
	if len(nodeIDParam) == 0 {
		s.handleLocalLog(w, r, ps)
		return
	}

	id, err := strconv.ParseInt(nodeIDParam, 10, 64)
	if err != nil {
		http.Error(w,
			fmt.Sprintf("node id could not be parsed: %s", err),
			http.StatusBadRequest)
		return
	}
	nodeID := proto.NodeID(id)

	if s.gossip.GetNodeID() == nodeID {
		s.handleLocalLog(w, r, ps)
		return
	}

	addr, err := s.gossip.GetNodeIDAddress(nodeID)
	if err != nil {
		http.Error(w,
			fmt.Sprintf("node could not be located: %s", nodeIDParam),
			http.StatusBadRequest)
		return
	}

	// Create a call to the correct node. We might want to consider moving this
	// and any other larger calls to an RPC instead of just proxying the http
	// request to the correct node.
	// Generate the redirect url and copy all the parameters to it.
	url := fmt.Sprintf("%s://%s%s?%s", s.ctx.RequestScheme(), addr, statusLogKeyPrefix, r.URL.RawQuery)

	// Call the other node.
	req, err := http.NewRequest("GET", url, nil)
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

// handleLocalStacks handles GET requests for goroutines stack traces.
func (s *statusServer) handleLocalStacks(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
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
		w.Write(buf[:length])
		return
	}
}

// handleNodesStatus handles GET requests for all node statuses.
func (s *statusServer) handleNodesStatus(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	startKey := keys.StatusNodePrefix
	endKey := startKey.PrefixEnd()

	rows, err := s.db.Scan(startKey, endKey, 0)
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	nodeStatuses := []status.NodeStatus{}
	for _, row := range rows {
		nodeStatus := &status.NodeStatus{}
		if err := row.ValueProto(nodeStatus); err != nil {
			log.Error(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		nodeStatuses = append(nodeStatuses, *nodeStatus)
	}
	b, contentType, err := util.MarshalResponse(r, nodeStatuses, []util.EncodingType{util.JSONEncoding})
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set(util.ContentTypeHeader, contentType)
	w.Write(b)
}

// handleNodeStatus handles GET requests for a single node's status. If no id is
// available, it calls handleNodesStatus to return all node's statuses.
func (s *statusServer) handleNodeStatus(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id, err := strconv.ParseInt(ps.ByName("id"), 10, 64)
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	key := keys.NodeStatusKey(int32(id))

	nodeStatus := &status.NodeStatus{}
	if err := s.db.GetProto(key, nodeStatus); err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	b, contentType, err := util.MarshalResponse(r, nodeStatus, []util.EncodingType{util.JSONEncoding})
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set(util.ContentTypeHeader, contentType)
	w.Write(b)
}

// handleStoresStatus handles GET requests for all store statuses.
func (s *statusServer) handleStoresStatus(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	startKey := keys.StatusStorePrefix
	endKey := startKey.PrefixEnd()

	rows, err := s.db.Scan(startKey, endKey, 0)
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	storeStatuses := []storage.StoreStatus{}
	for _, row := range rows {
		storeStatus := &storage.StoreStatus{}
		if err := row.ValueProto(storeStatus); err != nil {
			log.Error(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		storeStatuses = append(storeStatuses, *storeStatus)
	}
	b, contentType, err := util.MarshalResponse(r, storeStatuses, []util.EncodingType{util.JSONEncoding})
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set(util.ContentTypeHeader, contentType)
	w.Write(b)
}

// handleStoreStatus handles GET requests for a single node's status. If no id
// is available, it calls handleStoresStatus to return all store's statuses.
func (s *statusServer) handleStoreStatus(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id, err := strconv.ParseInt(ps.ByName("id"), 10, 32)
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	key := keys.StoreStatusKey(int32(id))

	storeStatus := &storage.StoreStatus{}
	if err := s.db.GetProto(key, storeStatus); err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	b, contentType, err := util.MarshalResponse(r, storeStatus, []util.EncodingType{util.JSONEncoding})
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set(util.ContentTypeHeader, contentType)
	w.Write(b)
}

// handleTransactionStatus handles GET requests for transaction status.
func (s *statusServer) handleTransactionStatus(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set(util.ContentTypeHeader, util.JSONContentType)
	w.Write([]byte(`{"transactions": []}`))
}
